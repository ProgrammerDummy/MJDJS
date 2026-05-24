#[derive(Debug, Eq, PartialEq, PartialOrd, Ord)]
pub struct Job {
    pub id: u64,
    pub job_type: u64,
    pub payload: u64,
    pub priority: u64,
    pub retry_count: u64,
    pub created_at: u64,
    pub state: JobState,
}

//removed retry_policy struct field from Job to make the retry policy belong to the type of job for simplicity
//remember to add this later

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum JobState {
    Queued,
    Running {
        worker_id: u64,
        started_at: u64,
    },
    Succeeded {
        completed_at: u64,
        result: u64,
    },
    Failed {
        error: u64,
        attempts: u64,
    },
    Retrying {
        retry_at: u64,
        attempts: u64,
    },
    DeadLettered {
        reason: String,
    }
}

//for now i didn't put the specific datatypes in for the fields, as they can be adjusted later based on the actual implementation and requirements of the job processing system.

pub enum QueueError {
    EmptyQueueDequeue,
    EmptyQueuePeek,
}