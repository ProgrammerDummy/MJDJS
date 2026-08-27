use std::{collections::HashMap, sync::Arc};

use tokio_util::sync::CancellationToken;
use tonic::async_trait;

struct ExecutorRegistry {
    registry: HashMap<String, Arc<dyn JobExecutor>>,
    //registry is keyed by job_type
}

#[async_trait]
pub trait JobExecutor: Send + Sync {
    async fn execute(&self, payload: u64, cancel: CancellationToken) -> Result<u64, JobError>;
}

pub enum JobError {
    Timeout,
    BadPayload,
    ExecutionFailure(u64),
    Cancelled,
}


impl JobError {
    pub fn code_translation(&self) -> u64 {
        //to map to a error code, depending on the error type
        match self {
            JobError::Timeout => {
                1
            },
            JobError::BadPayload => {
                2
            },
            JobError::ExecutionFailure(num) => {
                3
            },
            JobError::Cancelled => {
                4
            }
        }
    }
}