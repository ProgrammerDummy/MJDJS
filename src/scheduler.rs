use crate::job_data_structures::{Job, JobQueue};
use crate::worker::{WorkerPool, Runnable, Worker};
use crate::state_machine::{transition, determine_next_event, JobEvent};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::{Notify};
use tokio_util::sync::CancellationToken;
use std::collections::HashMap;

pub enum JobOutcome {
    Success {
        result: u64,
    },
    Failure {
        error: u64,
    }
}
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
}

impl <T: Runnable> Scheduler<T> {
    pub fn new() -> Self {
        let (tx, rx) = channel(100);

        Scheduler { 
            queue: Arc::new(Mutex::new(JobQueue::new())), 
            worker_pool: Arc::new(Mutex::new(WorkerPool::new())), 
            job_results: rx,
            sender: tx, 
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
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
                                            let _ = transition(job, JobEvent::Success { completed_at: 0u64, result}); //explicit ignore and placeholder values until i implement real clock
                                            in_flight.remove(&msg.job_id); //remove the job from the in_flight hashmap
                                        },

                                        None => {

                                        },
                                    }
                                },

                                JobOutcome::Failure {error} => {
                                    let mut in_flight = self.in_flight.lock().unwrap();
                                    match in_flight.get_mut(&msg.job_id) {
                                        Some(job) => {
                                            let _ = transition(job, JobEvent::Fail {error}); //another placeholder

                                            match determine_next_event(job) {
                                                JobEvent::Retry {retry_after} => {
                                                    let _ = transition(job, JobEvent::Retry { retry_after });
                                                    {
                                                        let mut queue_lock = self.queue.lock().unwrap();
                                                        queue_lock.enqueue(job.clone()); 
                                                    }
                                                },

                                                JobEvent::DeadLetter {reason} => {
                                                    let _ = transition(job, JobEvent::DeadLetter { reason });
                                                    in_flight.remove(&msg.job_id);
                                                },

                                                _ => {}, //since determine_next_event only ever returns deadletter or retry, im ignoring this
                                            }
                                        }

                                        None => {},
                                    }
                                }

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
                                Ok(job) => {
                                    //at this point both an idle worker and a dequeue-able job exists
                                    let tx_clone = self.sender.clone();
                                    let worker_id_clone = worker_id.clone();
                                    let job_clone = job.clone();

                                    {
                                        let mut worker_pool_lock = self.worker_pool.lock().unwrap();
                                        if worker_pool_lock.assign_job(worker_id, job.id.clone()) != Ok(()) {
                                            return; //currently this is unreachable but returns just in case
                                        }
                                    }

                                    {
                                        let mut in_flight = self.in_flight.lock().unwrap();
                                        in_flight.insert(job.id.clone(), job.clone());
                                    }

                                    tokio::spawn(async move { //fire job by submitting task to tokio scheduler 
                                        simulate_job_execution(job_clone, worker_id_clone, tx_clone).await;
                                    });
                                },
                                Err(_) => {
                                    self.notify.notified().await; //if the dequeue fails, then await
                                }
                            }
                        },
                        None => {
                            self.notify.notified().await; //if no idle workers can be found, await
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
    let exec_time: u64 = 350;
    tokio::time::sleep(std::time::Duration::from_millis(exec_time)).await;
    let _ = sender.send(JobResult {
        job_id: job.id,
        worker_id,
        event: JobOutcome::Success { result: 1 },
    }).await;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

use crate::{job_data_structures::{JobState, RetryPolicy}, scheduler, worker::WorkerStatus};
    
use super::*;

    #[tokio::test]
    async fn job_dispatched_and_completed() {
        let mut scheduler: Scheduler<Worker> = Scheduler::new();

        let cancel = CancellationToken::new();

        scheduler.enqueue_job(Job { 
            id: 1,
            job_type: 1, 
            payload: 1, 
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

        


        // register a worker
        // enqueue a job
        // spawn scheduler.run() as a background task
        // wait long enough for the job to complete (hint: sleep)
        // assert in_flight is empty
        // assert worker is idle again
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_in_flight() {
        let mut scheduler = Scheduler::new();

        let cancel = CancellationToken::new();

        scheduler.enqueue_job(Job { 
            id: 1,
            job_type: 1, 
            payload: 1, 
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
        
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(async move {
            scheduler.run(cancel_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        cancel.cancel();

        handle.await


    }
}
