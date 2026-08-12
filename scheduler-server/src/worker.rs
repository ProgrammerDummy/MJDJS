use std::collections::{HashMap, HashSet};
use std::time::Instant;

use scheduler_core::worker::WorkerId;

#[derive(Debug)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub hostname: String,
    pub job_types_supported: Vec<String>,
    pub max_concurrent_jobs: u32,
    pub assigned_jobs: HashSet<uuid::Uuid>,
    pub last_heartbeat_at: Instant,
    pub capabilities: HashMap<String, String>,
    pub tags: HashMap<String, String>,
    pub state: WorkerState,
}

#[derive(Debug)]
pub enum WorkerState {
    Active,
    Dead,
    Draining,
}