/*
a worker needs a unique id, a status, and a job if it is running one currently
*/

use crate::job_data_structures::{Job, JobOutcome};
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const SIMULATE_INITIAL_FAILURE: u64 = 9999;

#[derive(PartialEq, Debug, Clone)]
pub enum WorkerStatus {
    Idle,
    Busy,
    Dead,
}

#[derive(Error, PartialEq, Debug)]
pub enum WorkerPoolError {
    #[error("Invalid assignment attempted")]
    InvalidAssignment, //currently a placeholder, not constructed anywhere
    #[error("Worker not found")]
    WorkerNotFound,
    #[error("Worker is currently unavailable")]
    WorkerUnavailable,
} 

#[derive(Error, Debug, Clone, PartialEq)]
pub enum JobExecutionError {

    //unreachable for now, serves as a placeholder that i will fill out
}

#[derive(Clone)]
pub struct Worker {
    pub worker_id: u64,
}

impl Worker {
    pub fn new(worker_id: u64) -> Self {
        Worker { worker_id }
    }
}
pub trait Runnable: Clone + Send + 'static {
    fn run(&self, job: Job, cancel: CancellationToken) -> impl Future<Output = Result<JobOutcome, JobExecutionError>> + Send;
    fn get_worker_id(&self) -> u64;
}

impl Runnable for Worker {
    async fn run(&self, job: Job, cancel: CancellationToken) -> Result<JobOutcome, JobExecutionError> {

        tokio::select! {
            _ = cancel.cancelled() => {
                return Ok(JobOutcome::Cancelled)
            },

            _ = tokio::time::sleep(std::time::Duration::from_millis(job.payload)) => {
                if job.retry_count == 0 && job.job_type == SIMULATE_INITIAL_FAILURE {
                    return Ok(JobOutcome::Failure { error: 1 });
                } else {
                    return Ok(JobOutcome::Success { result: 1 });
                }
            },
        }
    }

    fn get_worker_id(&self) -> u64 {
        self.worker_id
    }


}

pub struct PoolEntry<T> {
    pub worker: T,
    pub status: WorkerStatus,
}

pub struct WorkerPool<T> {
    pool: HashMap<u64, PoolEntry<T>>,
    watch_teller: watch::Sender<bool>,
    //hashmap with KV pair of worker_id and the worker with generic type
}

impl<T: Runnable> WorkerPool<T> {
    pub fn new() -> Self {

        let (tx, rx) = tokio::sync::watch::channel(false);

        WorkerPool { 
            pool: HashMap::new(),
            watch_teller: tx,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.watch_teller.subscribe()
    }

    pub fn update_availability(&self) { //THIS MUST ALWAYS BE CALLED AFTER WORKER POOL STATUS MUTATION NOT BEFORE
        let has_idle_worker = self.find_idle_worker().is_some();

        self.watch_teller.send_if_modified(|current_value| {
            if has_idle_worker && *current_value == false {
                *current_value = true;
                true
            } else if !has_idle_worker && *current_value == true {
                *current_value = false;
                true
            } else {
                false
            }
        });

    }

    pub fn get_worker(&self, worker_id: u64) -> Option<T> {
        self.pool.get(&worker_id).map(|entry| entry.worker.clone())
    }

    pub fn register_worker(&mut self, worker: T) {
        self.pool.insert(worker.get_worker_id(), PoolEntry { worker, status: WorkerStatus::Idle });
        self.update_availability();
    }

    pub fn find_idle_worker(&self) -> Option<u64> {
        self.pool.iter().find_map(|(&key, val)| { //chapter 13 of rust book
            if val.status == WorkerStatus::Idle {
                Some(key)
            }
            else {
                None
            }
        })
    } 

    pub fn assign_job(&mut self, worker_id: u64) -> Result<(), WorkerPoolError> {
        let pool_entry = self.pool.get_mut(&worker_id);

        match pool_entry {
            Some(pool_entry) => {
                if pool_entry.status == WorkerStatus::Busy || pool_entry.status == WorkerStatus::Dead {
                    Err(WorkerPoolError::WorkerUnavailable)
                }

                else {
                    pool_entry.status = WorkerStatus::Busy;
                    self.update_availability();
                    Ok(())
                }

            },
            None => Err(WorkerPoolError::WorkerNotFound),
        }
    }

    pub fn free_worker(&mut self, worker_id: u64) -> Result<(), WorkerPoolError> {
        let pool_entry = self.pool.get_mut(&worker_id);

        match pool_entry {
            Some(pool_entry) => {
                if pool_entry.status == WorkerStatus::Dead {
                    Err(WorkerPoolError::WorkerUnavailable)
                }

                else {
                    pool_entry.status = WorkerStatus::Idle;
                    self.update_availability();
                    Ok(())
                } 
            },
            None => Err(WorkerPoolError::WorkerNotFound)
        }
    }

    pub fn get_worker_status(&self, worker_id: u64) -> Option<WorkerStatus> {
        match self.pool.get(&worker_id) {
            Some(pool_entry) => {
                return Some(pool_entry.status.clone())
            },
            None => {
                return None
            }
        }
        
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{worker};
    use std::collections::HashMap;

    #[test]
    fn register_worker_check() {
        let mut dum: WorkerPool<Worker> = WorkerPool::new();

        dum.register_worker(Worker { worker_id: 1 });

        assert_eq!(dum.find_idle_worker(), Some(1));
    }

    #[test]
    fn assign_job_test() {
        let mut dum: WorkerPool<Worker> = WorkerPool::new();

        dum.register_worker(Worker { worker_id: 1 });

        assert_eq!(dum.assign_job(1), Ok(()));

        assert_eq!(dum.pool[&1].status, WorkerStatus::Busy);

        assert_eq!(dum.assign_job(1), Err(WorkerPoolError::WorkerUnavailable));

        assert_eq!(dum.assign_job(2), Err(WorkerPoolError::WorkerNotFound));
    }

    #[test]
    fn free_worker_test() {
        let mut dum: WorkerPool<Worker> = WorkerPool::new();

        dum.register_worker(Worker { worker_id: 1 });

        assert_eq!(dum.free_worker(1), Ok(()));

        assert_eq!(dum.pool[&1].status, WorkerStatus::Idle);

        assert_eq!(dum.free_worker(2), Err(WorkerPoolError::WorkerNotFound));
    }

    #[test]
    fn find_idle_worker() {
        let mut dum: WorkerPool<Worker> = WorkerPool::new();

        assert_eq!(dum.find_idle_worker(), None);
    }
}