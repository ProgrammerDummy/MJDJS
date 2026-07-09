use crate::job_data_structures::{Job, JobQueue};
use crate::worker::{WorkerPool};
use crate::state_machine::{transition, determine_next_event, JobEvent};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender, channel};
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
pub struct Scheduler {
    //main scheduler struct
    pub queue: Arc<Mutex<JobQueue>>,
    pub worker_pool: Arc<Mutex<WorkerPool>>,
    pub job_results: Receiver<JobResult>, //wil receive JobResult from mpsc channel
    pub sender: Sender<JobResult>,
    pub in_flight: HashMap<u64, Job>, //represents jobs currently managed by scheduler
}

impl Scheduler {
    pub fn new() -> Self {
        let (tx, rx) = channel(100);

        Scheduler { 
            queue: Arc::new(Mutex::new(JobQueue::new())), 
            worker_pool: Arc::new(Mutex::new(WorkerPool::new())), 
            job_results: rx,
            sender: tx, 
            in_flight: HashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.job_results.recv() => {
                    match msg.event {
                        JobOutcome::Success {result} => {
                            match self.in_flight.get_mut(&msg.job_id) {
                                Some(job) => {
                                    let _ = transition(job, JobEvent::Success { completed_at: 0u64, result}); //explicit ignore and placeholder values until i implement real clock
                                    self.in_flight.remove(&msg.job_id); //remove the job from the in_flight hashmap
                                },

                                None => {},
                            }
                        },

                        JobOutcome::Failure {error} => {
                            match self.in_flight.get_mut(&msg.job_id) {
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
                                            self.in_flight.remove(&msg.job_id);
                                        },

                                        _ => {}, //since determine_next_event only ever returns deadletter or retry, im ignoring this
                                    }
                                }

                                None => {},
                            }

                            {
                                //regardless of outcome, free the worker
                                let mut worker_pool_lock = self.worker_pool.lock().unwrap();
                                worker_pool_lock.free_worker(msg.worker_id);
                            }
                        }
                    }
                },

                _ = async {
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
                                            return;
                                        }
                                    }

                                    self.in_flight.insert(job.id, job);
                                    
                                    tokio::spawn(async move {
                                        simulate_job_execution(job_clone, worker_id_clone, tx_clone).await;
                                    });
                                },
                                Err(_) => {
                                    return
                                }
                            }
                        },
                        None => return,
                    }
                } => {},


            } 
        }
    }

    /*in the future when handling multiple scheduler tasks, be careful of TOCTOU bug 
    regarding the worker pool since it could change after dropping the mutex*/
    
}

async fn simulate_job_execution(job: Job, worker_id: u64, sender: Sender<JobResult>) { //just a simulator for executing jobs
    let exec_time: u64 = 100;
    tokio::time::sleep(std::time::Duration::from_millis(exec_time)).await;
    let _ = sender.send(JobResult {
        job_id: job.id,
        worker_id,
        event: JobOutcome::Success { result: 1 },
    }).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn job_dispatched_and_completed() {
        let mut scheduler = Scheduler::new();
        
        // register a worker
        // enqueue a job
        // spawn scheduler.run() as a background task
        // wait long enough for the job to complete (hint: sleep)
        // assert in_flight is empty
        // assert worker is idle again
    }
}
