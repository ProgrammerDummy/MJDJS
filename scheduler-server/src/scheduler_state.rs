use scheduler_core::job_data_structures::{JobOutcome, RetryPolicy};
use uuid;

use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU64;
use std::collections::{BinaryHeap, BTreeMap};
use dashmap::DashMap;
use std::collections::HashMap;

use std::time::Instant;

pub struct QueuedJob {
    pub id: uuid::Uuid,
    pub priority: u64,
    pub created_at: u64,
    pub requirements: HashMap<String, String>,

    //to get a binaryheap ordering based off of priority, and a fallback sort of created_at, these need to be included
    //also id is always included to keep track of which Job it is exactly
}

pub struct RunningJob {
    pub id: uuid::Uuid,
    pub worker_id: u64,
    pub started_at: u64,
    pub retry_count: u64,
    pub infra_interruptions: u64,
    pub retry_policy: RetryPolicy,
}

pub struct CompletedJob {
    pub id: uuid::Uuid,
    pub completed_at: u64,
    pub outcome: JobOutcome,
    //a completed job is something that has ended, so it will include both success and failure
}

pub struct SchedulerState {
    job_queue: Arc<Mutex<BinaryHeap<QueuedJob>>>, //binary heap ordered by priority and then created_at for retrieving first value for one at a time access
    running_jobs: Arc<DashMap<uuid::Uuid, RunningJob>>, //dashmap for sharding since this will have many concurrent callers, avoids contention
    completed_jobs: Arc<DashMap<uuid::Uuid, CompletedJob>>, //same as running_jobs
    //workers: Arc<DashMap<WorkerId, WorkerInfo>>, //same as above
    retry_queue: Arc<Mutex<BTreeMap<Instant, uuid::Uuid>>>, //BTreeMap for allowing cancellation and keyed removal of individual elements
    total_submitted: Arc<AtomicU64>, //atomic counter
    total_completed: Arc<AtomicU64>,
    total_failed: Arc<AtomicU64>,
    total_dead_lettered: Arc<AtomicU64>,

    //chose to wrap each field with Arc and mutex/dashmap so that only necessary components can clone the arc handle rather than having access to entire struct
}