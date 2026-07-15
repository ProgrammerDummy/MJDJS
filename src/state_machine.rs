use crate::job_data_structures::{JobState, Job, RetryPolicy};
use thiserror::Error;

#[derive(PartialEq, Debug)]
pub enum JobEvent {
    Run {
        worker_id: u64,
        started_at: u64,
    },
    Success {
        completed_at: u64,
        result: u64,
    },
    Retry {
        retry_after: std::time::Duration,
    },
    Fail {
        error: u64,
    },
    DeadLetter {
        reason: String,
    },
}

#[derive(Error, PartialEq, Debug)]
pub enum TransitionError {
    #[error("Invalid transition attempted, previous state: {previous_state:?}, attempted state: {attempted_transition:?}")]
    InvalidTransition {
        previous_state: JobState,
        attempted_transition: JobEvent,
    }, //an example of an invalid transition between states would be success and run
    #[error("Retry limit reached maximum")]
    RetryLimitReached, 
    //when the retry count reaches its maximum so must be deadlettered, for now the error is unreachable so just a placeholder rn
}

pub fn determine_next_event(job: &Job) -> JobEvent { //this is for determining if a failed job should be deadlettered or retried
    match job.retry_policy.next_delay(job.retry_count) {
        Some(delay) => {
            //let now = std::time::Instant::now();
            return JobEvent::Retry { retry_after: delay }
        },

        None => {
            return JobEvent::DeadLetter { reason: "retries exhausted".to_string() }
        }
    }
}

//transition should be a pure function 
//think about race conditions here in the future make sure this is an atomic operation
//along with reading into the job
pub fn transition(job: &mut Job, event: JobEvent) -> Result<(), TransitionError> {
    let current_state = std::mem::replace(&mut job.state, JobState::Queued);
    match (current_state, event) {
        (JobState::Queued, JobEvent::Run { worker_id, started_at }) => {
            job.state = JobState::Running { worker_id, started_at };
            Ok(())
        },

        (JobState::Running {worker_id: _, started_at: _ }, JobEvent::Success { completed_at, result }) => {
            job.state = JobState::Succeeded { completed_at, result };
            Ok(())
        },

        (JobState::Running { worker_id: _, started_at: _ }, JobEvent::Fail { error }) => {
            job.state = JobState::Failed { error };
            job.available_retry_attempts -= 1;
            job.retry_count += 1;
            Ok(()) 
        },

        (JobState::Failed { error: _ }, JobEvent::Retry { retry_after }) => {
            job.state = JobState::Retrying { retry_after };
            Ok(())
        },

        (JobState::Retrying { retry_after: _}, JobEvent::Run { worker_id, started_at }) => {
            job.state = JobState::Running { worker_id, started_at }; //must check for the number of attempts as well later on
            Ok(())
        },

        (JobState::Retrying { retry_after: _}, JobEvent::DeadLetter { reason }) => {
            job.state = JobState::DeadLettered { reason };
            Ok(())
        },

        (JobState::Failed { error: _ }, JobEvent::DeadLetter { reason }) => {
            job.state = JobState::DeadLettered { reason };
            Ok(())
        },

        (state, event) => {
            job.state = state.clone();  
            Err(TransitionError::InvalidTransition { previous_state: state, attempted_transition: event })
        },

    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job_data_structures::{Job, JobQueue, JobState, QueueError};
 
    fn make_job(state: JobState) -> Job {
        Job {
            id: 1,
            job_type: 1,
            payload: 1,
            priority: 1,
            available_retry_attempts: 3,
            retry_count: 0,
            created_at: 0,
            state,
            retry_policy: RetryPolicy::NoRetry,
        }
    }

    //this for testing valid transitions
    #[test]

    fn running_plus_success_to_succeeded() {
        let mut job = make_job(JobState::Running { worker_id: 1, started_at: 1 });
        let event = JobEvent::Success { completed_at: 1, result: 1 };
        let result = transition(&mut job, event);
        assert_eq!(result, Ok(()));
        assert_eq!(job.state, JobState::Succeeded { completed_at: 1, result: 1 });
    }
    #[test]
    fn running_plus_fail_to_failed() {
        let mut job = make_job(JobState::Running { worker_id: 1, started_at: 1 });
        let event = JobEvent::Fail { error: 1 };
        let result = transition(&mut job, event);
        assert_eq!(result, Ok(()));
        assert_eq!(job.state, JobState::Failed { error: 1 });
        assert_eq!(job.retry_count, 1);
        assert_eq!(job.available_retry_attempts, 2);
    }
    #[test]
    fn failed_plus_retry_to_retrying() {
        let mut job = make_job(JobState::Failed { error: 1 });
        let event = JobEvent::Retry { retry_after: std::time::Duration::from_millis(200) };
        let result = transition(&mut job, event);
        assert_eq!(result, Ok(()));
        assert_eq!(job.state, JobState::Retrying { retry_after: std::time::Duration::from_millis(200) });
    }
    #[test]
    fn retrying_plus_run_to_running() {
        let mut job = make_job(JobState::Retrying { retry_after: std::time::Duration::from_millis(200) });
        let event = JobEvent::Run { worker_id: 1, started_at: 1 };
        let result = transition(&mut job, event);
        assert_eq!(result, Ok(()));
        assert_eq!(job.state, JobState::Running { worker_id: 1, started_at: 1 });
    }
    #[test]
    fn retrying_plus_deadletter_to_deadlettered() {
        let mut job = make_job(JobState::Retrying { retry_after: std::time::Duration::from_millis(200) });
        let event = JobEvent::DeadLetter { reason: "unknown for now".to_string() };
        let result = transition(&mut job, event);
        assert_eq!(result, Ok(()));
        assert_eq!(job.state, JobState::DeadLettered { reason: "unknown for now".to_string() });
    }
    #[test]
    fn queued_plus_run_transitions_to_running() {
        let mut job = make_job(JobState::Queued);
        let event = JobEvent::Run { worker_id: 42, started_at: 100 };
        let result = transition(&mut job, event);
        assert_eq!(result, Ok(()));
        assert_eq!(job.state, JobState::Running { worker_id: 42, started_at: 100 });
    }

    #[test]
    //invalid transition tests
    fn queued_plus_success_to_queued() {
        let mut job = make_job(JobState::Queued);
        let event = JobEvent::Success { completed_at: 1, result: 1 };
        let result = transition(&mut job, event);
        assert_eq!(result, Err(TransitionError::InvalidTransition { previous_state: JobState::Queued, attempted_transition: JobEvent::Success { completed_at: 1, result: 1 }}));
        assert_eq!(job.state, JobState::Queued); 
    }
    #[test]
    fn succeeded_plus_run_to_succeeded() {
        let mut job = make_job(JobState::Succeeded { completed_at: 1, result: 2 });
        let event = JobEvent::Run { worker_id: 1, started_at: 3 };
        let result = transition(&mut job, event);
        assert_eq!(result, Err(TransitionError::InvalidTransition { previous_state: JobState::Succeeded { completed_at: 1, result: 2 }, attempted_transition: JobEvent::Run { worker_id: 1, started_at: 3 }}));
        assert_eq!(job.state, JobState::Succeeded { completed_at: 1, result: 2 }); 
    }
    #[test]
    fn running_plus_retry_to_running() {
        let mut job = make_job(JobState::Running { worker_id: 1, started_at: 300 });
        let event = JobEvent::Retry { retry_after: std::time::Duration::from_millis(200) };
        let result = transition(&mut job, event);
        assert_eq!(result, Err(TransitionError::InvalidTransition { previous_state: JobState::Running { worker_id: 1, started_at: 300 }, attempted_transition: JobEvent::Retry { retry_after: std::time::Duration::from_millis(200)}}));
        assert_eq!(job.state, JobState::Running { worker_id: 1, started_at: 300 }); 
    }


    //tests for determine_next_event to see if decision making for if a job should retry or not is correct
    #[test]
    fn expected_return_retry() {
        let mut job = Job {
            id: 1,
            job_type: 1,
            payload: 1,
            priority: 1,
            available_retry_attempts: 3,
            retry_count: 0,
            created_at: 0,
            state: JobState::Failed { error: 1 },
            retry_policy: RetryPolicy::FixedDelay { delay_ms: 300, max_attempts: 3 },
        };
        let result = determine_next_event(&mut job);

        match result {
            JobEvent::Retry { retry_after } => {
                assert!((225..375).contains(&retry_after.as_millis()));
            },

            _ => panic!("expected Retry, got {:?}", result),
        }
    }
    #[test]
    fn expected_return_deadletter() {
        let mut job = Job {
            id: 1,
            job_type: 1,
            payload: 1,
            priority: 1,
            available_retry_attempts: 0,
            retry_count: 0,
            created_at: 0,
            state: JobState::Failed { error: 1 },
            retry_policy: RetryPolicy::NoRetry,
        };
        let result = determine_next_event(&mut job);

        assert_eq!(result, JobEvent::DeadLetter { reason: "retries exhausted".to_string() });

        let mut job = Job {
            id: 1,
            job_type: 1,
            payload: 1,
            priority: 1,
            available_retry_attempts: 0,
            retry_count: 4,
            created_at: 0,
            state: JobState::Failed { error: 1 },
            retry_policy: RetryPolicy::ExponentialBackoff { base_ms: 200, multiplier: ordered_float::OrderedFloat(1.5), max_attempts: 3, max_delay_ms: 1000 },
        };

        let result = determine_next_event(&mut job);

        assert_eq!(result, JobEvent::DeadLetter { reason: "retries exhausted".to_string() });
    }

    #[test]
    //JobQueue tests for ordering and errors
    fn jobqueue_priority_test() {
        let mut queue = JobQueue::new();
        queue.enqueue(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 1, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 12, 
            state: JobState::Queued,
            retry_policy: RetryPolicy::NoRetry,
        });

        queue.enqueue(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 2, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 12, 
            state: JobState::Queued,
            retry_policy: RetryPolicy::NoRetry, 
        });

        assert_eq!(queue.dequeue(), Ok(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 2, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 12, 
            state: JobState::Queued,
            retry_policy: RetryPolicy::NoRetry, 
        }));

        assert_eq!(queue.dequeue(), Ok(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 1, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 12, 
            state: JobState::Queued,
            retry_policy: RetryPolicy::NoRetry,
        }));        
        
        
    }
    #[test]
    fn jobqueue_created_at_test() {
        let mut queue = JobQueue::new();
        queue.enqueue(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 1, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 12, 
            state: JobState::Queued,
            retry_policy: RetryPolicy::NoRetry, 
        });

        queue.enqueue(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 1, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 10, 
            state: JobState::Queued,
            retry_policy: RetryPolicy::NoRetry, 
        });

        assert_eq!(queue.dequeue(), Ok(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 1, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 10, 
            state: JobState::Queued,
            retry_policy: RetryPolicy::NoRetry, 
        }));

        assert_eq!(queue.dequeue(), Ok(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 1, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 12, 
            state: JobState::Queued,
            retry_policy: RetryPolicy::NoRetry, 
        }));        
    }
    #[test]
    fn empty_queue_peek_error() {
        let queue = JobQueue::new();
        assert_eq!(queue.peek(), Err(QueueError::EmptyQueuePeek));
    }
    #[test]
    fn empty_queue_deqeue_error() {
        let mut queue = JobQueue::new();
        assert_eq!(queue.dequeue(), Err(QueueError::EmptyQueueDequeue));
    }

    



}