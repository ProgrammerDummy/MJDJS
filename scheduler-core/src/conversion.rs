use crate::job_data_structures::{Job, JobState, RetryPolicy};
use crate::proto::{self};
use ordered_float::OrderedFloat;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("invalid job id: {0}")]
    InvalidId(#[from] uuid::Error),
    #[error("job status missing state field")]
    MissingState,
    #[error("retry policy missing policy field")]
    MissingPolicy,
    #[error("conversion failed due to negative duration")]
    NegativeDuration,
    #[error("no duration present to convert")]
    NoDuration,
    #[error("delay duration exceed maximum cap")]
    MaxDurationExceeded,
}

impl TryFrom<Job> for proto::Job {
    type Error = ConversionError;

    fn try_from(job: Job) -> Result<Self, Self::Error> {
        Ok(proto::Job { 
            id: job.id.as_bytes().to_vec(), 
            job_type: job.job_type, 
            payload: job.payload, 
            priority: job.priority, 
            retry_count: job.retry_count, 
            created_at: job.created_at, 
            state: match job.state {
                JobState::Queued => {
                    Some(proto::JobStatus { state: Some(proto::job_status::State::Queued(proto::job_status::Queued {}))})
                },

                JobState::Running { worker_id, started_at } => {
                    Some(proto::JobStatus { state: Some(proto::job_status::State::Running(proto::job_status::Running { worker_id, started_at}))})
                },

                JobState::Succeeded { completed_at, result } => {
                    Some(proto::JobStatus { state: Some(proto::job_status::State::Succeeded(proto::job_status::Succeeded { completed_at, result}))})
                },

                JobState::Failed { error } => {
                    Some(proto::JobStatus { state: Some(proto::job_status::State::Failed(proto::job_status::Failed { error}))})
                },

                JobState::Retrying { retry_after } => {
                    Some(proto::JobStatus { state: Some(proto::job_status::State::Retrying(proto::job_status::Retrying { retry_after: Some(retry_after.try_into().map_err(|_| ConversionError::MaxDurationExceeded)?)}))})
                },//it is unexpected to get an error during this conversion, given that validate_retry_policy exists to ensrue that it doesnt exceed

                JobState::DeadLettered { reason } => {
                    Some(proto::JobStatus { state: Some(proto::job_status::State::Deadlettered(proto::job_status::DeadLettered { reason}))})
                },

                JobState::Abandoned { reason, abandoned_at } => {
                    Some(proto::JobStatus { state: Some(proto::job_status::State::Abandoned(proto::job_status::Abandoned { reason, abandoned_at}))})
                },
            }, 

            retry_policy: match job.retry_policy {
                RetryPolicy::FixedDelay { delay_ms, max_attempts } => {
                    Some(proto::RetryPolicy { policy: Some(proto::retry_policy::Policy::FixedDelay(proto::retry_policy::FixedDelay { delay_ms, max_attempts}))})
                },
                RetryPolicy::ExponentialBackoff { base_ms, multiplier, max_attempts, max_delay_ms } => {
                    Some(proto::RetryPolicy { policy: Some(proto::retry_policy::Policy::ExponentialBackoff(proto::retry_policy::ExponentialBackoff { base_ms, multiplier: multiplier.into_inner() as f32, max_attempts, max_delay_ms}))})
                },
                RetryPolicy::NoRetry => {
                    Some(proto::RetryPolicy{ policy: Some(proto::retry_policy::Policy::NoRetry(proto::NoRetry {}))})
                }
            }, 
            requirements: job.requirements, 
            metadata: job.metadata 
        })
    }

}


impl TryFrom<proto::Job> for Job {
    type Error = ConversionError;

    fn try_from(p: proto::Job) -> Result<Self, Self::Error> {
        let id = uuid::Uuid::from_slice(&p.id)?;

        let state = match p.state {
            Some(job_status) => {
                match job_status.state {
                    Some(proto::job_status::State::Queued(_)) => Ok::<JobState, ConversionError>(JobState::Queued),

                    Some(proto::job_status::State::Running(proto::job_status::Running { worker_id, started_at })) => {
                        Ok(JobState::Running { worker_id, started_at })
                    },

                    Some(proto::job_status::State::Succeeded(proto::job_status::Succeeded { completed_at, result })) => {
                        Ok(JobState::Succeeded { completed_at, result })
                    },

                    Some(proto::job_status::State::Failed(proto::job_status::Failed { error })) => {
                        Ok(JobState::Failed { error })
                    },

                    Some(proto::job_status::State::Retrying(proto::job_status::Retrying { retry_after })) => {
                        match retry_after {
                            Some(retry_after) => {
                                match std::time::Duration::try_from(retry_after) {
                                    Ok(std_duration) => {
                                        Ok(JobState::Retrying { retry_after: std_duration })
                                    },

                                    Err(e) => {
                                        return Err(ConversionError::NegativeDuration)
                                    },
                                }
                            },
                            
                            None => {
                                return Err(ConversionError::NoDuration);
                            }
                        }

                    },

                    Some(proto::job_status::State::Deadlettered(proto::job_status::DeadLettered { reason })) => {
                        Ok(JobState::DeadLettered { reason })
                    },

                    Some(proto::job_status::State::Abandoned(proto::job_status::Abandoned { reason, abandoned_at })) => {
                        Ok(JobState::Abandoned { reason, abandoned_at })
                    },
                    
                    None => return Err(ConversionError::MissingState),
                }
            },
            None => return Err(ConversionError::MissingState),
        }?;

        let retry_policy = match p.retry_policy {
            Some(policy_option) => {
                match policy_option.policy {
                    Some(proto::retry_policy::Policy::FixedDelay(proto::retry_policy::FixedDelay { delay_ms, max_attempts})) => {
                        Ok::<RetryPolicy, ConversionError>(RetryPolicy::FixedDelay { delay_ms, max_attempts })
                    },

                    Some(proto::retry_policy::Policy::ExponentialBackoff(proto::retry_policy::ExponentialBackoff { base_ms, multiplier, max_attempts, max_delay_ms})) => {
                        Ok(RetryPolicy::ExponentialBackoff { base_ms, multiplier: OrderedFloat(multiplier as f64), max_attempts, max_delay_ms })
                    },

                    Some(proto::retry_policy::Policy::NoRetry(_)) => {
                        Ok(RetryPolicy::NoRetry)
                    },

                    None => {
                        return Err(ConversionError::MissingPolicy);
                    }
                }
            },

            None => {
                return Err(ConversionError::MissingPolicy);
            }
        }?;

        Ok(Job {
            id,
            job_type: p.job_type,
            payload: p.payload,
            priority: p.priority,
            retry_count: p.retry_count,
            created_at: p.created_at,
            state,
            retry_policy,
            requirements: p.requirements,
            metadata: p.metadata,
        })

    
    }
}


fn validate_retry_policy(policy: &RetryPolicy) -> Result<(), ConversionError> {
    const MAX_DELAY_MS: u64 = 10 * 60 * 1000; //set as 10 minutes max delay per job

    match policy {
        RetryPolicy::FixedDelay { delay_ms, max_attempts } => {
            if *delay_ms < MAX_DELAY_MS {
                return Ok(())
            }

            return Err(ConversionError::MaxDurationExceeded)
        },

        RetryPolicy::ExponentialBackoff { base_ms, multiplier, max_attempts, max_delay_ms } => {
            if *base_ms < MAX_DELAY_MS && *max_delay_ms < MAX_DELAY_MS {
                return Ok(())
            } 

            return Err(ConversionError::MaxDurationExceeded)


        },

        RetryPolicy::NoRetry => {
            Ok(())
        }
    }
    // check delay_ms/base_ms/max_delay_ms against MAX_DELAY_MS
    // wherever the policy variant carries them
} //move this to the submitjob handler