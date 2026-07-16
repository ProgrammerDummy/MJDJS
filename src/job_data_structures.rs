use std::time::UNIX_EPOCH;
use std::{cmp::Ordering, time::SystemTime};

use std::collections::BinaryHeap;

use ordered_float::OrderedFloat;

use thiserror::Error;

use crate::job_data_structures::RetryPolicy::{ExponentialBackoff, FixedDelay, NoRetry};

#[derive(Debug, PartialEq, Clone, Eq)]
pub struct Job {
    pub id: u64,
    pub job_type: u64,
    pub payload: u64,
    pub priority: u64,
    pub available_retry_attempts: u64,
    pub retry_count: u64,
    pub created_at: u64,
    pub state: JobState,
    pub retry_policy: RetryPolicy,
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> Ordering {
        // compare by priority first (highest goes first)
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => {
                //if priorities are equal, compare the created_at time to decide which one is ordered first
                self.created_at.cmp(&other.created_at).reverse() 
                //use .reverse() so that earlier jobs come first if priorities are same
            }
            other => other, //if different priorites, give the ordering as is
        }
    }


}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
        //calls the custom cmp implementation for ord trait
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum JobOutcome {
    Success {
        result: u64,
    },
    Failure {
        error: u64,
    },
    Cancelled,
}


//removed retry_policy struct field from Job to make the retry policy belong to the type of job for simplicity
//remember to add this later

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryPolicy { //a retry policy which has three options, to not retry, a fixed delay between retries, or an exponential backoff
    NoRetry,
    FixedDelay {
        delay_ms: u64,
        max_attempts: u32,
    },
    ExponentialBackoff {
        base_ms: u64,
        multiplier: OrderedFloat<f64>,
        max_attempts: u32,
        max_delay_ms: u64,
    },
}

/*
job fail s → transition() records the Fail → determine_next_event(job) 
  → calls job.retry_policy.next_delay(job.retry_count) 
  → Some(delay) => JobEvent::Retry { retry_at: now + delay }
  → None        => JobEvent::DeadLetter { reason: "retries exhausted" }

*/

impl RetryPolicy {
    pub fn next_delay(&self, retry_count: u64) -> Option<std::time::Duration> { //returns a duration computed
        match self {
            NoRetry => {
                return None
            },

            FixedDelay { delay_ms, max_attempts} => {
                if retry_count >= *max_attempts as u64 {
                    return None
                }

                return Some(std::time::Duration::from_millis(((*delay_ms as f64)*rand::random_range(0.75..1.25)) as u64)) //added jitter to fixed delay
            },

            ExponentialBackoff {
                base_ms, 
                multiplier, 
                max_attempts, 
                max_delay_ms } => {
                    if retry_count >= *max_attempts as u64 {
                        return None
                    }

                    let computed_delay = (((*base_ms as f64) * multiplier.powf(retry_count as f64) * rand::random_range(0.75..1.25)) as u64);
                    //jitter added between a range of 0.75 and 1.25

                    if computed_delay >= *max_delay_ms { //clamp to max_delay_ms
                        return Some(std::time::Duration::from_millis(*max_delay_ms));
                    }

                    return Some(std::time::Duration::from_millis(computed_delay))
                    
                }

        }

    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
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
    },
    Retrying {
        retry_after: std::time::Duration,
    },
    DeadLettered {
        reason: String,
    }
}

//for now i didn't put the specific datatypes in for the fields, as they can be adjusted later based on the actual implementation and requirements of the job processing system.

#[derive(Error, Debug, PartialEq, Eq)]
pub enum QueueError {
    #[error("Attempted to dequeue on an empty queue")]
    EmptyQueueDequeue,
    #[error("Attempted to peek on an empty queue")]
    EmptyQueuePeek,
}

pub struct JobQueue {
    heap: BinaryHeap<Job>,

}

impl JobQueue {
    pub fn new() -> Self {
        JobQueue {
            heap: BinaryHeap::new(),
        }
    }

    pub fn enqueue(&mut self, job: Job) {
        self.heap.push(job);
    }

    pub fn dequeue(&mut self) -> Result<Job, QueueError> {
        self.heap.pop().ok_or(QueueError::EmptyQueueDequeue)
    }

    pub fn peek(&self) -> Result<&Job, QueueError> {
        self.heap.peek().ok_or(QueueError::EmptyQueuePeek)
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .expect("system clock was set before UNIX_EPOCH, so a timestamp cannot be generated")
}