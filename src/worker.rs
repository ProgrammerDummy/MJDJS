/*
a worker needs a unique id, a status, and a job if it is running one currently
*/

use crate::{worker};
use std::collections::HashMap;

#[derive(PartialEq, Debug)]
pub enum WorkerStatus {
    Idle,
    Busy,
    Dead,
}

#[derive(PartialEq, Debug)]
pub enum WorkerPoolError {
    InvalidAssignment,
    WorkerNotFound,
    WorkerUnavailable,
} 

pub struct Worker {
    pub worker_id: u64,
    pub status: WorkerStatus,
    job_id: Option<u64>,
}

pub struct WorkerPool {
    pool: HashMap<u64, Worker>,
    //hashmap with KV pair of worker_id and the worker
}

impl WorkerPool {
    pub fn new() -> Self {
        WorkerPool { pool: HashMap::new() }
    }

    pub fn register_worker(&mut self, worker_id: u64) {
        self.pool.insert(worker_id, Worker { worker_id, status: WorkerStatus::Idle, job_id: None });
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

    pub fn assign_job(&mut self, worker_id: u64, job_id: u64) -> Result<(), WorkerPoolError> {
        let worker = self.pool.get_mut(&worker_id);

        match worker {
            Some(worker) => {
                if worker.status == WorkerStatus::Busy || worker.status == WorkerStatus::Dead {
                    Err(WorkerPoolError::WorkerUnavailable)
                }

                else {
                    worker.status = WorkerStatus::Busy;
                    worker.job_id = Some(job_id);
                    Ok(())
                }
            },
            None => Err(WorkerPoolError::WorkerNotFound),
        }
    }

    pub fn free_worker(&mut self, worker_id: u64) -> Result<(), WorkerPoolError> {
        let worker = self.pool.get_mut(&worker_id);

        match worker {
            Some(worker) => {
                if worker.status == WorkerStatus::Dead {
                    Err(WorkerPoolError::WorkerUnavailable)
                }

                else {
                    worker.status = WorkerStatus::Idle;
                    worker.job_id = None;
                    Ok(())
                } 
            },
            None => Err(WorkerPoolError::WorkerNotFound)
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
        let mut dum = WorkerPool::new();

        dum.register_worker(1);

        assert_eq!(dum.find_idle_worker(), Some(1));
    }

    #[test]
    fn assign_job_test() {
        let mut dum = WorkerPool::new();

        dum.register_worker(1);

        assert_eq!(dum.assign_job(1, 2), Ok(()));

        assert_eq!(dum.pool[&1].status, WorkerStatus::Busy);

        assert_eq!(dum.assign_job(1, 3), Err(WorkerPoolError::WorkerUnavailable));

        assert_eq!(dum.assign_job(2, 4), Err(WorkerPoolError::WorkerNotFound));
    }

    #[test]
    fn free_worker_test() {
        let mut dum = WorkerPool::new();

        dum.register_worker(1);

        dum.free_worker(1);

        assert_eq!(dum.pool[&1].status, WorkerStatus::Idle);

        assert_eq!(dum.free_worker(2), Err(WorkerPoolError::WorkerNotFound));

    }

    #[test]
    fn find_idle_worker() {
        let mut dum = WorkerPool::new();

        assert_eq!(dum.find_idle_worker(), None);
    }
}