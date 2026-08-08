use std::collections::{HashMap, HashSet};
use std::time::Instant;

use scheduler_core::worker::WorkerId;

#[derive(Debug)]
pub struct WorkerInfo {
    id: WorkerId,
    hostname: String,
    job_types_supported: Vec<String>,
    max_concurrent_jobs: u32,
    assigned_jobs: HashSet<uuid::Uuid>,
    last_heartbeat_at: Instant,
    tags: HashMap<String, String>,
    state: WorkerState,
}

#[derive(Debug)]
pub enum WorkerState {
    Active,
    Dead,
    Draining,
}