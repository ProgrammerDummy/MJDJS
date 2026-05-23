pub struct Job {
    id: usize,
    job_type: usize,
    payload: usize,
    priority: usize,
    retry_count: usize,
    retry_policy: usize,
    created_at: usize,
    state: JobState,
}

pub enum JobState {
    Queued,
    Running {
        worker_id: usize,
        started_at: usize,
    },
    Succeeded {
        completed_at: usize,
    },
    Failed {
        error: usize,
        attempts: usize,
    },
    Retrying {
        retry_at: usize,
        attempts: usize,
    },
    DeadLettered {
        reason: String,
    }
}

