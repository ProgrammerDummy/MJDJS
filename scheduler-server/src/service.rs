use scheduler_core::proto::SubmitJobResponse;
use scheduler_core::{conversion::ConversionError};
use scheduler_core::job_data_structures::{Job, JobState, RetryPolicy};
use scheduler_core::proto::{self, scheduler_service_server::SchedulerService};
use tonic::{Request, Response, Status};
use std::collections::HashMap;
use std::convert::TryFrom;
use futures::stream::BoxStream;
use uuid;
use std::sync::{Arc, Mutex};

pub struct MySchedulerService {
    jobs: Arc<Mutex<std::collections::HashMap<uuid::Uuid, Job>>>,
}

impl MySchedulerService {
    pub fn new() -> Self {
        MySchedulerService { jobs: Arc::new(Mutex::new(HashMap::new())) }
    }
}

#[tonic::async_trait]
impl SchedulerService for MySchedulerService {
    async fn submit_job(&self, request: Request<proto::Job>) -> Result<Response<proto::SubmitJobResponse>, Status> {
        //this method is the only method to submit jobs, there must be a lot of checks to ensure that the job is valid

        let proto_job = request.into_inner();

        let job = Job::try_from(proto_job).map_err(|_| tonic::Status::invalid_argument("an invalid field exists within the proto job"))?;

        //check the retrypolicy here now that the job is converted properly

        validate_retry_policy(&job.retry_policy).map_err(|_| tonic::Status::invalid_argument("retry policy was invalid"))?;

        let job = Job::new_submitted(job);
        
        {
            let mut jobs = self.jobs.lock().unwrap();
            jobs.insert(job.id, job.clone());

        }

        Ok(tonic::Response::new(proto::SubmitJobResponse {
            id: job.id.into_bytes().to_vec()
        }))
        

       
    }

    async fn get_job_status(&self, request: Request<proto::JobIdRequest>) -> Result<Response<proto::JobStatus>, Status> {
        Err(Status::unimplemented("get_job_status not yet implemented"))
    }

    async fn cancel_job(&self, request: Request<proto::JobIdRequest>) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("cancel_job not yet implemented"))
    }

    async fn requeue_from_dlq(&self, request: Request<proto::JobIdRequest>) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("requeue_from_dlq not yet implemented"))
    }

    async fn create_template(&self, request: Request<proto::Template>) -> Result<Response<proto::TemplateResponse>, Status> {
        Err(Status::unimplemented("create_template not yet implemented"))
    }

    type ListJobsStream = BoxStream<'static, Result<proto::Job, Status>>;

    type ListDeadLetteredStream = BoxStream<'static, Result<proto::Job, Status>>;

    async fn list_jobs(&self, request: Request<proto::ListRequest>) -> Result<Response<Self::ListJobsStream>, Status> {
        Err(Status::unimplemented("list_jobs not yet implemented"))
    }

    async fn list_dead_lettered(&self, request: Request<proto::ListRequest>) -> Result<Response<Self::ListDeadLetteredStream>, Status> {
        Err(Status::unimplemented("list_dead_lettered not yet implemented"))
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