use crate::job_data_structures::{Job, JobQueue, JobOutcome};
use crate::worker::{WorkerPool, Runnable, Worker};
use crate::state_machine::{transition, determine_next_event, JobEvent};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;
use std::collections::HashMap;

const SIMULATE_INITIAL_FAILURE: u64 = 9999;

pub struct JobResult { 
    //a struct to pass through the mpsc with appropriate data representing the finished job
    pub job_id: u64,
    pub worker_id: u64,
    event: JobOutcome,
}

pub struct Scheduler<T> {
    //main scheduler struct
    pub queue: Arc<Mutex<JobQueue>>,
    pub worker_pool: Arc<Mutex<WorkerPool<T>>>,
    pub job_results: Receiver<JobResult>, //wil receive JobResult from mpsc channel
    pub sender: Sender<JobResult>,
    pub in_flight: Arc<Mutex<HashMap<u64, Job>>>, //represents jobs currently managed by scheduler
    pub notify: Arc<Notify>,
    pub waiting_retry: Arc<Mutex<HashMap<u64, Job>>>, //represents jobs which are on timeout after failing
    pub dead_lettered: Arc<Mutex<HashMap<u64, Job>>>, //minimal deadletter queue for testing purposes currently, will replace later with more info-storing data structure
    pub watcher: watch::Receiver<bool>,
    pub succeeded: Arc<Mutex<HashMap<u64, Job>>>,
    pub dispatch_order: Option<Arc<Mutex<Vec<Job>>>>,
}

impl <T: Runnable> Scheduler<T> {
    pub fn new() -> Self {
        let (tx, rx) = channel(100);

        let pool = WorkerPool::new();

        let watcher = pool.subscribe();

        Scheduler { 
            queue: Arc::new(Mutex::new(JobQueue::new())), 
            worker_pool: Arc::new(Mutex::new(pool)), 
            job_results: rx,
            sender: tx, 
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
            waiting_retry: Arc::new(Mutex::new(HashMap::new())),
            dead_lettered: Arc::new(Mutex::new(HashMap::new())),
            watcher,
            succeeded: Arc::new(Mutex::new(HashMap::new())),
            dispatch_order: None, //mainly for testing purposes to confirm that priority of jobs dispatched is non-increasing
        }
    }

    pub fn enqueue_job(&mut self, job: Job) {
        let mut queue = self.queue.lock().unwrap();
        queue.enqueue(job);
        self.notify.notify_one();
    }

    pub fn register_worker(&self, worker: T) {
        let mut worker_pool = self.worker_pool.lock().unwrap();
        worker_pool.register_worker(worker);
        self.notify.notify_one();
    }

    pub async fn run(&mut self, cancel: CancellationToken) {

        eprintln!("run() started");

        let mut shutting_down = false;

        loop {
            tokio::select! {
                result = self.job_results.recv() => {
                    match result {
                        Some(msg) => {
                            match msg.event {
                                JobOutcome::Success {result} => {
                                    let mut in_flight = self.in_flight.lock().unwrap();

                                    match in_flight.get_mut(&msg.job_id) {
                                        Some(job) => {
                                            match transition(job, JobEvent::Success { completed_at: 0u64, result}) {
                                                //placeholder values until i implement real clock
                                                Ok(_) => {
                                                    let mut succeeded = self.succeeded.lock().unwrap();
                                                    succeeded.insert(msg.job_id, job.clone());
                                                    
                                                    in_flight.remove(&msg.job_id); //remove the job from the in_flight hashmap
                                                },
                                                Err(e) => {
                                                    eprintln!("Transition to Success failed for job: {}, error: {:?}", msg.job_id, e);
                                                    in_flight.remove(&msg.job_id);
                                                },
                                            }


                                        },

                                        None => {

                                        },
                                    }
                                },

                                JobOutcome::Failure {error} => {
                                    let mut in_flight = self.in_flight.lock().unwrap();
                                    match in_flight.get_mut(&msg.job_id) {
                                        Some(job) => {


                                            match transition(job, JobEvent::Fail {error}) {
                                                //placeholder values until i implement real clock
                                                Ok(_) => {
                                                    match determine_next_event(job) {
                                                        JobEvent::Retry {retry_after} => {
                                                            match transition(job, JobEvent::Retry { retry_after }) {
                                                                Ok(_) => {
                                                                    {
                                                                        {
                                                                            let mut waiting_retry = self.waiting_retry.lock().unwrap();

                                                                            waiting_retry.insert(msg.job_id, job.clone());
                                                                        }

                                                                        let notify = self.notify.clone();
                                                                        let waiting_retry_lock = self.waiting_retry.clone();
                                                                        let queue_lock = self.queue.clone();
                                                                        let job_clone = job.clone();

                                                                        tokio::spawn(async move { //make job sleep until its retry_after duration finishes, remove from waiting_retry and re-enqueue
                                                                            tokio::time::sleep(retry_after).await; 
                                                                            let mut waiting_retry = waiting_retry_lock.lock().unwrap();
                                                                            waiting_retry.remove(&job_clone.id);
                                                                            
                                                                            let mut queue = queue_lock.lock().unwrap();
                                                                            queue.enqueue(job_clone);

                                                                            notify.notify_one();

                                                                        });
                        
                                                                    }
                                                                    in_flight.remove(&msg.job_id);
                                                                },

                                                                Err(e) => {
                                                                    eprintln!("Transition to Retry failed for job: {}, error: {:?}", msg.job_id, e);
                                                                    in_flight.remove(&msg.job_id);
                                                                }
                                                            }
                                                            
                                    
                                                        },

                                                        JobEvent::DeadLetter {reason} => {
                                                            match transition(job, JobEvent::DeadLetter { reason }) {
                                                                Ok(_) => {
                                                                    let mut dead_lettered = self.dead_lettered.lock().unwrap();
                                                                    dead_lettered.insert(msg.job_id, job.clone());
                                                                    in_flight.remove(&msg.job_id);
                                                                },

                                                                Err(e) => {
                                                                    eprintln!("Transition to Deadletter failed for job: {}, error: {:?}", msg.job_id, e);
                                                                    in_flight.remove(&msg.job_id);
                                                                }
                                                            }
                                                            

                                                        
                                                        },

                                                        _ => {}, //since determine_next_event only ever returns deadletter or retry, im ignoring this
                                                    }

                                                },
                                                Err(e) => {
                                                    eprintln!("Transition to Fail failed for job: {}, error: {:?}", msg.job_id, e);
                                                    in_flight.remove(&msg.job_id);
                                                },
                                            }

                            
                                        }

                                        None => {},
                                    }
                                },

                                JobOutcome::Cancelled => {

                                },

                            }

                            let mut worker_pool_lock = self.worker_pool.lock().unwrap();
                            //in case free_worker returns an error, which should be impossible for now, it will log it and continue the loop
                            if let Err(e) = worker_pool_lock.free_worker(msg.worker_id) {
                                eprintln!("free_worker failed for worker {}: {:?}", msg.worker_id, e);
                            }
                            self.notify.notify_one();
                        },

                        None => {
                            shutting_down = true;
                            eprint!("MPSC channel closed unexpectedly");
                            break;
                        }
                    }
                },


                _ = async { //dispatch branch
                    let idle_worker = {
                        let worker_pool_lock = self.worker_pool.lock().unwrap();
                        worker_pool_lock.find_idle_worker()
                    };//mutex for worker pool dropped here

                    match idle_worker {
                        Some(worker_id) => {
                            let dequeued_job = {
                                let mut queue_lock = self.queue.lock().unwrap();
                                queue_lock.dequeue()
                            }; //lock for jobqueue dropped here

                            match dequeued_job {
                                Ok(mut job) => {

                                    if let Some(tracker) = &self.dispatch_order {
                                        tracker.lock().unwrap().push(job.clone());
                                    }

                                    //at this point both an idle worker and a dequeue-able job exists

                                    {
                                        let mut worker_pool_lock = self.worker_pool.lock().unwrap();
                                        if worker_pool_lock.assign_job(worker_id) != Ok(()) {
                                            return; //currently this is unreachable but returns just in case
                                        }
                                    }

                                    if let Err(e) = transition(&mut job, JobEvent::Run { worker_id, started_at: 0}) {
                                        eprintln!("Transition failed for job: {}, due to {:?}", job.id, e);
                                    }

                                    {
                                        let mut in_flight = self.in_flight.lock().unwrap();
                                        in_flight.insert(job.id.clone(), job.clone());
                                    }

                                    let tx_clone = self.sender.clone();
                                    let worker_id_clone = worker_id.clone();
                                    let cancel_clone = cancel.clone();

                                    let job_clone = job.clone();

                                    let worker_pool_lock = self.worker_pool.clone();
                                

                                    tokio::spawn(async move { //fire job by submitting task to tokio scheduler 
                                        let worker_clone = {
                                            worker_pool_lock.lock().unwrap().get_worker(worker_id_clone)
                                        };

                                        if let Some(worker) = worker_clone {
                                            match worker.run(job_clone.clone(), cancel_clone).await {
                                                Ok(outcome) => {
                                                    let _ = tx_clone.send(JobResult {
                                                        job_id: job_clone.id,
                                                        worker_id: worker_id_clone,
                                                        event: outcome,
                                                    }).await;
                                                },

                                                Err(e) => {
                                                    eprintln!("Error occured during job execution: {:?}", e);
                                                },
                                            }
                                        }
                                        //let _ = tx_clone.send();
                                    });
                                },
                                Err(_) => {
                                    self.notify.notified().await; //if the dequeue fails, then await
                                }
                            }
                        },
                        None => {
                            eprintln!("dispatch: no idle worker, awaiting watcher.changed()");
                            if let Err(_) = self.watcher.changed().await {
                                eprintln!("Watch channel closed, sender dropped");
                            }

                            eprintln!("dispatch: watcher.changed() resolved");
                            //if no idle workers can be found, await
                        },
                    }
                }, if !shutting_down => {},

                _ = async { //cancellation branch
                    cancel.cancelled().await;
                }, if !shutting_down => {shutting_down = true},
            } 

            {
                let in_flight = self.in_flight.lock().unwrap();
                if shutting_down && in_flight.is_empty() {
                    eprint!("Shutdown requested, jobs abandoned");
                    break;
                }
            }
        }
    }

    /*in the future when handling multiple scheduler tasks, be careful of TOCTOU bug 
    regarding the worker pool since it could change after dropping the mutex*/
    
}

async fn simulate_job_execution(job: Job, worker_id: u64, sender: Sender<JobResult>) { //just a simulator for executing jobs
    tokio::time::sleep(std::time::Duration::from_millis(job.payload)).await;

    if job.retry_count == 0 && job.job_type == SIMULATE_INITIAL_FAILURE {
        let _ = sender.send(JobResult { job_id: job.id, worker_id, event: JobOutcome::Failure { error: 1 } }).await;
    } else {
        let _ = sender.send(JobResult {
        job_id: job.id,
        worker_id,
        event: JobOutcome::Success { result: 1 },
        }).await;
    }
    
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

use crate::{job_data_structures::{JobState, RetryPolicy}, worker::WorkerStatus};
    
use super::*;

    #[tokio::test]
    async fn job_dispatched_and_completed() {
        let mut scheduler: Scheduler<Worker> = Scheduler::new();

        let cancel = CancellationToken::new();

        scheduler.enqueue_job(Job { 
            id: 1,
            job_type: 1, 
            payload: 350, 
            priority: 1, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 0, 
            state: JobState::Queued, 
            retry_policy: RetryPolicy::FixedDelay { delay_ms: 100, max_attempts: 3 } 
        });

        scheduler.register_worker(Worker::new(1));

        let in_flight = scheduler.in_flight.clone();
        let worker_pool = scheduler.worker_pool.clone();
        

        tokio::spawn(async move {
            scheduler.run(cancel).await;
        });

        tokio::time::sleep(Duration::from_millis(600)).await;

        assert!(in_flight.lock().unwrap().is_empty());

        assert!({
            let worker_pool = worker_pool.lock().unwrap();
            worker_pool.get_worker_status(1) == Some(WorkerStatus::Idle)
        })

    }

    #[tokio::test]
    async fn graceful_shutdown_drains_in_flight() {
        let mut scheduler = Scheduler::new();

        let cancel = CancellationToken::new();

        scheduler.enqueue_job(Job { 
            id: 1,
            job_type: 1, 
            payload: 350, 
            priority: 1, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 0, 
            state: JobState::Queued, 
            retry_policy: RetryPolicy::FixedDelay { delay_ms: 100, max_attempts: 3 } 
        });

        scheduler.register_worker(Worker::new(1));

        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(async move {
            scheduler.run(cancel_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        
        let dum = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;

        match dum {
            Ok(dum2) => {
                match dum2 {
                    Ok(_) => assert!(true),
                    Err(_) => assert!(false),
                }
            },
            Err(_) => assert!(false),
        }


    }

    #[tokio::test]
    async fn success_after_retry() {
        let mut scheduler: Scheduler<Worker> = Scheduler::new();

        scheduler.enqueue_job(Job {
            id: 1,
            job_type: SIMULATE_INITIAL_FAILURE,
            payload: 350,
            priority: 1,
            available_retry_attempts: 3,
            retry_count: 0,
            created_at: 0,
            state: JobState::Queued,
            retry_policy: RetryPolicy::FixedDelay { delay_ms: 150, max_attempts: 3 },
        });

        scheduler.register_worker(Worker::new(1));

        let in_flight = scheduler.in_flight.clone();
        let worker_pool = scheduler.worker_pool.clone();

        let cancel = CancellationToken::new(); 
        tokio::spawn(async move {
            scheduler.run(cancel).await;
        });
        
        tokio::time::sleep(Duration::from_millis(1100)).await;

        assert!(in_flight.lock().unwrap().is_empty());
        assert_eq!(worker_pool.lock().unwrap().get_worker_status(1), Some(WorkerStatus::Idle));
    }

    #[tokio::test]
    async fn deadlettered() {

        let mut scheduler: Scheduler<Worker> = Scheduler::new();

        scheduler.enqueue_job(Job {
            id: 1,
            job_type: SIMULATE_INITIAL_FAILURE,
            payload: 350,
            priority: 1,
            available_retry_attempts: 1,
            retry_count: 0,
            created_at: 0,
            state: JobState::Queued,
            retry_policy: RetryPolicy::FixedDelay { delay_ms: 150, max_attempts: 1 },
        });

        scheduler.register_worker(Worker::new(1));

        eprintln!("aaa");

        let in_flight = scheduler.in_flight.clone();
        let worker_pool = scheduler.worker_pool.clone();
        let dead_letter_queue = scheduler.dead_lettered.clone();

        let cancel = CancellationToken::new();
        eprintln!("about to spawn run()");
        tokio::spawn(async move {
            scheduler.run(cancel).await;
        });

        eprintln!("spawn() call returned");


        tokio::time::sleep(Duration::from_millis(600)).await;

        assert!(in_flight.lock().unwrap().is_empty());
        assert_eq!(worker_pool.lock().unwrap().get_worker_status(1), Some(WorkerStatus::Idle));
        assert_eq!(dead_letter_queue.lock().unwrap().get(&1), Some(&Job {
            id: 1,
            job_type: SIMULATE_INITIAL_FAILURE,
            payload: 350,
            priority: 1,
            available_retry_attempts: 0,
            retry_count: 1,
            created_at: 0,
            state: JobState::DeadLettered { reason: "retries exhausted".to_string() },
            retry_policy: RetryPolicy::FixedDelay { delay_ms: 150, max_attempts: 1 },
        }));
    }

    #[tokio::test]
    async fn stress_test() {
        let mut scheduler: Scheduler<Worker> = Scheduler::new();

        scheduler.dispatch_order = Some(Arc::new(Mutex::new(Vec::new())));

        for i in 1..=10 {
            scheduler.register_worker(Worker::new(i));
        }

        for i in 0..1000 {
            scheduler.enqueue_job(Job { 
                id: i,
                job_type: 1, 
                payload: rand::random_range(1..=100), 
                priority: rand::random_range(1..=1000), 
                available_retry_attempts: 3, 
                retry_count: 0, 
                created_at: 0, 
                state: JobState::Queued, 
                retry_policy: RetryPolicy::FixedDelay { delay_ms: 100, max_attempts: 3 } 
            });
        }

        let cancel = CancellationToken::new();

        let cancel_clone = cancel.clone();

        let succeeded_clone = scheduler.succeeded.clone();
        let dispatch_order_clone = scheduler.dispatch_order.clone().unwrap();
        let in_flight_clone = scheduler.in_flight.clone();
        let worker_pool_clone= scheduler.worker_pool.clone();

        let handle = tokio::spawn(async move {
            scheduler.run(cancel).await;
        });

        let mut in_flight_empty = false;

        let poll_result = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                {
                    in_flight_empty = in_flight_clone.lock().unwrap().is_empty();
                }
                if succeeded_clone.lock().unwrap().len() >= 1000 && in_flight_empty {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await;

        assert!(poll_result.is_ok(), "stress test did not complete within 60s");

        cancel_clone.cancel();

        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

        
        let guard = dispatch_order_clone.lock().unwrap();

        for pair in guard.windows(2) {
            if pair[0].priority < pair[1].priority {
                panic!("dispatch order was not non-increasing");
            }
        } 

        let pool_guard = worker_pool_clone.lock().unwrap();

        for i in 1..=10 {
            assert_eq!(pool_guard.get_worker_status(i), Some(WorkerStatus::Idle));
        }
        

    }
}
