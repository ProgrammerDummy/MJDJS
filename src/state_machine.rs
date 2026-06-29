use crate::job_data_structures::{JobState, Job};

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
        retry_at: u64,
    },
    Fail {
        error: u64,
    },
    DeadLetter {
        reason: String,
    },
}

#[derive(PartialEq, Debug)]
pub enum TransitionError {
    InvalidTransition {
        previous_state: JobState,
        attempted_transition: JobEvent,
    }, //an example of an invalid transition between states would be success and run
    RetryLimitReached, //when the retry count reaches its maximum so must be deadlettered
}

pub fn determine_next_event(job: &Job) -> JobEvent { //this is for determining if a failed job should be deadlettered or retried
    if job.available_retry_attempts != 0 {
        return JobEvent::Retry { retry_at: 100 }; //just a place holder for retry_at for now
    }
    else {
        return JobEvent::DeadLetter { reason: String::from("placeholder reason") };
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

        (JobState::Failed { error: _ }, JobEvent::Retry { retry_at }) => {
            job.state = JobState::Retrying { retry_at };
            Ok(())
        },

        (JobState::Retrying { retry_at: _}, JobEvent::Run { worker_id, started_at }) => {
            job.state = JobState::Running { worker_id, started_at }; //must check for the number of attempts as well later on
            Ok(())
        },

        (JobState::Retrying { retry_at: _}, JobEvent::DeadLetter { reason }) => {
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
        let event = JobEvent::Retry { retry_at: 2 };
        let result = transition(&mut job, event);
        assert_eq!(result, Ok(()));
        assert_eq!(job.state, JobState::Retrying { retry_at: 2 });
    }
    #[test]
    fn retrying_plus_run_to_running() {
        let mut job = make_job(JobState::Retrying { retry_at: 1 });
        let event = JobEvent::Run { worker_id: 1, started_at: 1 };
        let result = transition(&mut job, event);
        assert_eq!(result, Ok(()));
        assert_eq!(job.state, JobState::Running { worker_id: 1, started_at: 1 });
    }
    #[test]
    fn retrying_plus_deadletter_to_deadlettered() {
        let mut job = make_job(JobState::Retrying { retry_at: 1 });
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
        let event = JobEvent::Retry { retry_at: 10 };
        let result = transition(&mut job, event);
        assert_eq!(result, Err(TransitionError::InvalidTransition { previous_state: JobState::Running { worker_id: 1, started_at: 300 }, attempted_transition: JobEvent::Retry { retry_at: 10 }}));
        assert_eq!(job.state, JobState::Running { worker_id: 1, started_at: 300 }); 
    }


    //tests for determine_next_event to see if decision making for if a job should retry or not is correct
    #[test]
    fn expected_return_retry() {
        let job = Job {
            id: 1,
            job_type: 1,
            payload: 1,
            priority: 1,
            available_retry_attempts: 3,
            retry_count: 0,
            created_at: 0,
            state: JobState::Failed { error: 1 },
        };
        let result = determine_next_event(&job);

        assert_eq!(result, JobEvent::Retry { retry_at: 100 });
    }
    #[test]
    fn expected_return_deadletter() {
        let job = Job {
            id: 1,
            job_type: 1,
            payload: 1,
            priority: 1,
            available_retry_attempts: 0,
            retry_count: 0,
            created_at: 0,
            state: JobState::Failed { error: 1 },
        };
        let result = determine_next_event(&job);

        assert_eq!(result, JobEvent::DeadLetter { reason: "placeholder reason".to_string() });
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
            state: JobState::Queued 
        });

        queue.enqueue(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 2, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 12, 
            state: JobState::Queued 
        });

        assert_eq!(queue.dequeue(), Ok(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 2, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 12, 
            state: JobState::Queued 
        }));

        assert_eq!(queue.dequeue(), Ok(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 1, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 12, 
            state: JobState::Queued 
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
            state: JobState::Queued 
        });

        queue.enqueue(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 1, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 10, 
            state: JobState::Queued 
        });

        assert_eq!(queue.dequeue(), Ok(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 1, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 10, 
            state: JobState::Queued 
        }));

        assert_eq!(queue.dequeue(), Ok(Job { 
            id: 1, 
            job_type: 1, 
            payload: 2, 
            priority: 1, 
            available_retry_attempts: 3, 
            retry_count: 0, 
            created_at: 12, 
            state: JobState::Queued 
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