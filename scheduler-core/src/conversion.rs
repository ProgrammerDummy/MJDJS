use crate::job_data_structures::{Job, JobState, RetryPolicy};
use crate::proto;
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
    
}

impl From<Job> for proto::Job {
    fn from(job: Job) -> Self {
        todo!()
    }
}

impl TryFrom<proto::Job> for Job {
    type Error = ConversionError;

    fn try_from(p: proto::Job) -> Result<Self, Self::Error> {
        let id = uuid::Uuid::from_slice(&p.id)?;

        let state = match p.state {
            Some(job_status) => {
                match job_status.state {
                    Some(proto::job_status::State::Queued(_)) => Ok(JobState::Queued),

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

        todo!()
    }
}



/*
pub mod job_status {
    #[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
    pub struct Queued {}
    #[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
    pub struct Running {
        #[prost(uint64, tag = "1")]
        pub worker_id: u64,
        #[prost(uint64, tag = "2")]
        pub started_at: u64,
    }
    #[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
    pub struct Succeeded {
        #[prost(uint64, tag = "1")]
        pub completed_at: u64,
        #[prost(uint64, tag = "2")]
        pub result: u64,
    }
    #[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
    pub struct Failed {
        #[prost(uint64, tag = "1")]
        pub error: u64,
    }
    #[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
    pub struct Retrying {
        #[prost(message, optional, tag = "1")]
        pub retry_after: ::core::option::Option<::prost_types::Duration>,
    }
    #[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
    pub struct DeadLettered {
        #[prost(string, tag = "1")]
        pub reason: ::prost::alloc::string::String,
    }
    #[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
    pub struct Abandoned {
        #[prost(string, tag = "1")]
        pub reason: ::prost::alloc::string::String,
        #[prost(uint64, tag = "2")]
        pub abandoned_at: u64,
    }
    #[derive(Clone, PartialEq, Eq, Hash, ::prost::Oneof)]
    pub enum State {
        #[prost(message, tag = "1")]
        Queued(Queued),
        #[prost(message, tag = "2")]
        Running(Running),
        #[prost(message, tag = "3")]
        Succeeded(Succeeded),
        #[prost(message, tag = "4")]
        Failed(Failed),
        #[prost(message, tag = "5")]
        Retrying(Retrying),
        #[prost(message, tag = "6")]
        Deadlettered(DeadLettered),
        #[prost(message, tag = "7")]
        Abandoned(Abandoned),
    }



pub struct Job {
    pub id: uuid::Uuid,
    pub job_type: String,
    pub payload: u64,
    pub priority: u64,
    pub retry_count: u64,
    pub created_at: u64,
    pub state: JobState,
    pub retry_policy: RetryPolicy,
    pub requirements: std::collections::HashMap<String, String>,
    pub metadata: std::collections::HashMap<String, String>,
}
*/