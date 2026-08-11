use scheduler_core::job_data_structures::{JobState, RetryPolicy};
use scheduler_core::worker::{WorkerId};
use crate::worker::WorkerInfo;

use uuid;

use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU64;
use std::collections::{BTreeMap, BTreeSet};
use dashmap::DashMap;
use std::collections::HashMap;

use std::time::Instant;
use std::cmp::Ordering;

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum CompletedJobOutcome {
    Succeeded {
        result: u64,
    },
    DeadLettered {
        reason: String,
    },
    Abandoned {
        reason: String,
    }
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub struct QueuedJob {
    pub id: uuid::Uuid,
    pub job_type: String,
    pub payload: u64,
    pub priority: u64,
    pub created_at: u64,
    pub retry_count: u64,
    pub infra_interruptions: u64,
    pub retry_policy: RetryPolicy,
    pub requirements: HashMap<String, String>,
    pub metadata: HashMap<String, String>,

}

impl Ord for QueuedJob {
    fn cmp(&self, other: &Self) -> Ordering {
        // compare by priority first (highest goes first)
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => {
                //if priorities are equal, compare the created_at time to decide which one is ordered first
                match self.created_at.cmp(&other.created_at).reverse()  {
                    Ordering::Equal => self.id.cmp(&other.id),
                    other => other,
                }
                //use .reverse() so that earlier jobs come first if priorities are same
            }
            other => other, //if different priorites, give the ordering as is
        }
    }
}

impl PartialOrd for QueuedJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
        //calls the custom cmp implementation for ord trait
    }
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub struct RunningJob {
    pub id: uuid::Uuid,
    pub job_type: String,
    pub payload: u64,
    pub priority: u64,
    pub created_at: u64,
    pub retry_count: u64,
    pub running_phase: RunningPhase,
    pub infra_interruptions: u64,
    pub requirements: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
    pub retry_policy: RetryPolicy,
} //every job outside of queued and completed (fail or success or cancelled) jobs live here
//this includes jobs that are retrying

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RunningPhase {
    Executing { worker_id: WorkerId, started_at: u64 },
    Retrying,
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub struct CompletedJob {
    pub id: uuid::Uuid,
    pub job_type: String,
    pub payload: u64,
    pub priority: u64,
    pub created_at: u64,
    pub retry_policy: RetryPolicy,
    pub requirements: HashMap<String, String>,
    pub retry_count: u64,
    pub infra_interruptions: u64,
    pub completed_at: u64,
    pub outcome: CompletedJobOutcome,
    pub metadata: HashMap<String, String>,
    //a completed job is something that has ended, so it will include both success and failure
}

#[derive(Debug)]
pub struct SchedulerState {
    pub job_queue: Arc<Mutex<BTreeSet<QueuedJob>>>, //BTreeSet ordered by priority and then created_at for retrieving first value for one at a time access
    pub running_jobs: Arc<DashMap<uuid::Uuid, RunningJob>>, //dashmap for sharding since this will have many concurrent callers, avoids contention
    pub completed_jobs: Arc<DashMap<uuid::Uuid, CompletedJob>>, //same as running_jobs
    pub workers: Arc<DashMap<WorkerId, WorkerInfo>>, //same as above
    pub retry_queue: Arc<Mutex<BTreeMap<Instant, uuid::Uuid>>>, //BTreeMap for allowing cancellation and keyed removal of individual elements
    pub total_submitted: Arc<AtomicU64>, //atomic counter
    pub total_completed: Arc<AtomicU64>,
    pub total_failed: Arc<AtomicU64>,
    pub total_dead_lettered: Arc<AtomicU64>,

    //chose to wrap each field with Arc and mutex/dashmap so that only necessary components can clone the arc handle rather than having access to entire struct
}

impl SchedulerState {

    pub fn new() -> Self {
        SchedulerState {
            job_queue: Arc::new(Mutex::new(BTreeSet::new())), 
            running_jobs: Arc::new(DashMap::new()), 
            completed_jobs: Arc::new(DashMap::new()), 
            workers: Arc::new(DashMap::new()), 
            retry_queue: Arc::new(Mutex::new(BTreeMap::new())), 
            total_submitted: Arc::new(0.into()), 
            total_completed: Arc::new(0.into()),
            total_failed: Arc::new(0.into()),
            total_dead_lettered: Arc::new(0.into()),
        }

    }
}