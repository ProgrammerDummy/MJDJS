use std::collections::HashMap;
use std::time::Instant;

use scheduler_core::worker::WorkerId;

#[derive(Debug)]
pub struct WorkerInfo {
    pub id: WorkerId,
    #[allow(dead_code)] // stored at Register; read once workers are listed or logged
    pub hostname: String,
    pub job_types_supported: Vec<String>,
    pub max_concurrent_jobs: u32,
    pub assigned_jobs: HashMap<uuid::Uuid, AssignedJob>, //job_id -> the dispatch this worker holds
    pub last_heartbeat_at: Instant,
    pub capabilities: HashMap<String, String>,
    #[allow(dead_code)] // stored at Register; read once dispatch matches on tags (phase 3)
    pub tags: HashMap<String, String>,
    pub state: WorkerState,
}

/// One dispatch held by a worker, as the scheduler sees it.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct AssignedJob {
    pub dispatch_token: u64,
    /// consecutive heartbeats that arrived without listing this dispatch
    pub missed_heartbeats: u32,
}

#[derive(Debug, PartialEq)]
pub enum WorkerState {
    Active,
    #[allow(dead_code)] // not entered yet: nothing sends StopAcceptingWork
    Draining,
}
