use std::{collections::HashMap, sync::Arc};

use tokio_util::sync::CancellationToken;
use async_trait::async_trait;

use thiserror::Error;

pub struct ExecutorRegistry {
    registry: HashMap<String, Arc<dyn JobExecutor>>,
    //registry is keyed by job_type
}

impl ExecutorRegistry {
    pub fn new() -> Self {
        ExecutorRegistry { registry:HashMap::new() }
    }

    pub fn register(&mut self, executor_type: String, executor: Arc<dyn JobExecutor>) {
        self.registry.insert(executor_type, executor);
    } 

    pub fn get(&self, job_type: &str) -> Option<Arc<dyn JobExecutor>> {
        self.registry.get(job_type).cloned()
    }

    pub fn job_types(&self) -> Vec<String> {
        self.registry.keys().cloned().collect()
    }
}

#[async_trait]
pub trait JobExecutor: Send + Sync {
    async fn execute(&self, payload: u64, cancel: CancellationToken) -> Result<u64, JobError>;
}

#[derive(Debug, Error)]
pub enum JobError {
    #[error("Job execution timed out")]
    Timeout,
    #[error("Job had a malformed or bad payload")]
    BadPayload,
    #[error("Job failed to execute")]
    ExecutionFailure(u64),
    #[error("Job was cancelled")]
    Cancelled,
}


impl JobError {
    pub fn error_code_translation(&self) -> u64 {
        //to map to a error code, depending on the error type
        match self {
            JobError::Timeout => {
                101
            },
            JobError::BadPayload => {
                102
            },
            JobError::ExecutionFailure(num) => {
                (*num).min(99)
            },
            JobError::Cancelled => {
                103
            }
        }
    }

    /*
    
    1-99 are for job specific errors
    100 and beyond are for system errors
     */
}



pub struct SleepJob {}

#[async_trait]
impl JobExecutor for SleepJob {
    async fn execute(&self, payload: u64, cancel: CancellationToken) -> Result<u64, JobError> {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(payload)) => {
                return Ok(payload)
            }

            _ = cancel.cancelled() => {
                return Err(JobError::Cancelled)
            }
        }
    }
}

pub struct AlwaysFailJob {}

#[async_trait]
impl JobExecutor for AlwaysFailJob {
    async fn execute(&self, payload: u64, _cancel: CancellationToken) -> Result<u64, JobError> {
        Err(JobError::ExecutionFailure(payload.min(99)))
    }
}