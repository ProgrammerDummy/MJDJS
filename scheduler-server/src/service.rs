use scheduler_core::proto::{JobStatus, SubmitJobResponse};
use scheduler_core::{conversion::{ConversionError, job_state_to_proto, proto_to_job_status}};
use scheduler_core::job_data_structures::{Job, JobState, RetryPolicy};
use scheduler_core::proto::{self, scheduler_service_server::SchedulerService};
use tonic::{Request, Response, Status};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::atomic::Ordering;
use futures::stream::BoxStream;
use uuid;
use std::sync::{Arc};

use parking_lot::Mutex;

use scheduler_core::proto::scheduler_service_server::SchedulerServiceServer;
use scheduler_core::proto::scheduler_service_client;

use crate::scheduler_state::{QueuedJob, SchedulerState, RunningPhase, CompletedJobOutcome};

pub struct MySchedulerService {
    pub scheduler_state: SchedulerState,
}

impl MySchedulerService {
    pub fn new() -> Self {
        MySchedulerService { scheduler_state: SchedulerState::new() }
    }
}

impl MySchedulerService {
    fn scan_lists_for_job_status(&self, requested_id: uuid::Uuid) -> Result<JobState, Status> {

        //look for running_jobs first

        
        if let Some(running_job) = self.scheduler_state.running_jobs.get(&requested_id) {
            if let RunningPhase::Executing { worker_id, started_at} = running_job.running_phase {
                return Ok(JobState::Running { worker_id, started_at })
            } 
        
            //if RunningPhase::Retrying
            //warning for the future: if a RunningJob is within running_job but somehow not within retry_queue for some reason, the returned value of this function will be tonic::Status::not_found because it'll fall all the way through
            
            {
                let retry_queue_guard = self.scheduler_state.retry_queue.lock();
                if let Some(timeout_until) = retry_queue_guard
                    .iter()
                    .find(| (_, id) | *id == requested_id)
                    .map(| (timeout_until, _) | timeout_until) { //O(n) time, could speed up with BiBiTreeMap?
                    
                    return Ok(JobState::Retrying { retry_after: timeout_until.saturating_duration_since(std::time::Instant::now()) })
                }
            }
        }



        //look for completed_jobs next

        if let Some(completed_job) = self.scheduler_state.completed_jobs.get(&requested_id) {
            if let CompletedJobOutcome::Abandoned { reason } = &completed_job.outcome {
                return Ok(JobState::Abandoned { reason: reason.to_string(), abandoned_at: completed_job.completed_at })
            }

            if let CompletedJobOutcome::DeadLettered { reason } = &completed_job.outcome {
                return Ok(JobState::DeadLettered { reason: reason.to_string() })
            }

            if let CompletedJobOutcome::Succeeded { result } = &completed_job.outcome {
                return Ok(JobState::Succeeded { completed_at: completed_job.completed_at, result: *result })
            }
        }

        //finally, look at job_queue 

        {
            {
                let job_queue_guard = self.scheduler_state.job_queue.lock();
                if job_queue_guard.iter().any(|job| job.id == requested_id) {
                    return Ok(JobState::Queued);
                }

                return Err(tonic::Status::not_found("job was not found in MySchedulerService"));
            } 
        }
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

            let queued_job = QueuedJob {
                id: job.id,
                job_type: job.job_type,
                payload: job.payload,
                priority: job.priority,
                retry_count: job.retry_count,
                infra_interruptions: job.infra_interruptions,
                created_at: job.created_at,
                retry_policy: job.retry_policy,
                requirements: job.requirements,
                metadata: job.metadata,
            };

            {
                let mut jobs = self.scheduler_state.job_queue.lock();

                jobs.insert(queued_job);
            }

            self.scheduler_state.total_submitted.fetch_add(1, Ordering::SeqCst); //increment total_submitted in SchedulerState

        }

        Ok(tonic::Response::new(proto::SubmitJobResponse {
            id: job.id.into_bytes().to_vec()
        }))
        

       
    }


    async fn get_job_status(&self, request: Request<proto::JobIdRequest>) -> Result<Response<proto::JobStatus>, Status> {
        let job_id_request = request.into_inner();

        let job_id_request = uuid::Uuid::from_slice(job_id_request.id.as_slice());

        match job_id_request {
            Ok(job_uuid) => {

                //this is assuming that no duplicates will exist between job_queue, running_jobs, and completed_jobs at a time
                //should be ensured during migrations between data structures
 
                let mut job_state = self.scan_lists_for_job_status(job_uuid);

                if let Err(e) = job_state {
                    return Err(e);
                } //return early here if job couldn't be found
                
                match job_state_to_proto(job_state.unwrap()) { //safe to unwrap here
                    Ok(proto_job_status) => {
                        return Ok(tonic::Response::new(proto_job_status))
                    },

                    Err(e) => {
                        return Err(tonic::Status::internal("invalid job entered the system")) //this shouldn't occur due to the entrypoint having bounds, if this happens something has gone very wrong
                    }
                }
            },
            Err(e) => {
                return Err(tonic::Status::invalid_argument(format!("invalid job id: {e}"))) 
            },
        }


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
}

//`Mutex<RawMutex, BTreeSet<QueuedJob>>` and `std::sync::Mutex<BTreeSet<QueuedJob>>`
async fn bind_spawn_connect_for_tests() -> (scheduler_service_client::SchedulerServiceClient<tonic::transport::Channel>, Arc<Mutex<std::collections::BTreeSet<QueuedJob>>>) { //return back the job_queue clone instead
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tonic::transport::server::TcpIncoming::from(listener);

    let service = MySchedulerService::new();

    let clone_check = service.scheduler_state.job_queue.clone();

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(SchedulerServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    (scheduler_service_client::SchedulerServiceClient::new(channel), clone_check)
    
}



#[tokio::test]
async fn job_submission_success() {

    let (mut client, clone_check) = bind_spawn_connect_for_tests().await;
    
    let job = Job {
        id: uuid::Uuid::now_v7(),
        job_type: "test".to_string(),
        payload: 0,
        priority: 1,
        retry_count: 0,
        infra_interruptions: 0,
        created_at: 0,
        state: JobState::Queued,
        retry_policy: RetryPolicy::NoRetry,
        requirements: HashMap::new(),
        metadata: HashMap::new(),
    };

    let job = tonic::Request::new(proto::Job::try_from(job).unwrap());

    match client.submit_job(job).await {
        Ok(dum) => {
            let dum = dum.into_inner();  
            match uuid::Uuid::from_slice(&dum.id) {
                Ok(id) => {
                    {
                        let jobs_stored = clone_check.lock();
                        if !jobs_stored.iter().any(|job| job.id == id) {
                            panic!();
                        }
                    }
                },
                Err(e) => {
                    eprintln!("{e}");
                    panic!();
                }
            }
            
        },

        Err(e) => {
            eprintln!("{e}");
            panic!();
        }
    }

    


    /*
    testing plan: 
    look for client type and creation method within the generated file from prost and tonic

    setup the grpc server and bind to port for listening, run it in background using spawn
    wait until its done, use a oneshot channel to make sure that its ready to accept requests before firing my test request
    create a proper job and send a request after serializing it with prost into proto::job using try_from methods in conversion.rs
    submit this job through a grpc request
    receive this through the server, then inspect the inner jobs hashmap and assert it 
     */
}

#[tokio::test]
async fn job_submission_failure_invalid_retry_policy() {
    let (mut client, _clone_check) = bind_spawn_connect_for_tests().await;

    let job = Job {
        id: uuid::Uuid::now_v7(),
        job_type: "test".to_string(),
        payload: 0,
        priority: 1,
        retry_count: 0,
        infra_interruptions: 0,
        created_at: 0,
        state: JobState::Queued,
        retry_policy: RetryPolicy::FixedDelay { delay_ms: 600001, max_attempts: 2 }, //set a delay_ms greater than 10 minutes to violate the submit job retry time bound
        requirements: HashMap::new(),
        metadata: HashMap::new(),
    };

    let job = tonic::Request::new(proto::Job::try_from(job).unwrap());

    match client.submit_job(job).await {
        Ok(dum) => {
            panic!("this test was expected to fail with InvalidArgument");
        },
        Err(status) => {
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
        },
    }

}

#[tokio::test]
async fn get_job_status_success() {
    let (mut client, _clone_check) = bind_spawn_connect_for_tests().await;
    
    let job = Job {
        id: uuid::Uuid::now_v7(),
        job_type: "test".to_string(),
        payload: 0,
        priority: 1,
        retry_count: 0,
        infra_interruptions: 0,
        created_at: 0,
        state: JobState::Queued,
        retry_policy: RetryPolicy::NoRetry,
        requirements: HashMap::new(),
        metadata: HashMap::new(),
    };
    
    let job = tonic::Request::new(proto::Job::try_from(job).unwrap());

    match client.submit_job(job).await {
        Ok(dum) => {
            let dum = dum.into_inner();
            let response = client.get_job_status(tonic::Request::new(proto::JobIdRequest { id: dum.id})).await;
            match response {
                Ok(job_status_response) => {
                    let job_status = job_status_response.into_inner();
                    let job_status = proto_to_job_status(job_status.state).unwrap();
                    assert_eq!(job_status, JobState::Queued);
                    
                },

                Err(_) => {
                    eprintln!("uuid retrieval failed unexpectedly");
                    panic!();
                },
            }
        },

        Err(e) => {
            eprintln!("{e}");
            panic!();
        }
    }

}

#[tokio::test]
async fn get_job_status_not_found() {
    let (mut client, _clone_check) = bind_spawn_connect_for_tests().await;

    let nonexistent_id = uuid::Uuid::now_v7();
    let nonexistent_id = nonexistent_id.as_bytes();

    let job_status_request = tonic::Request::new(proto::JobIdRequest { id: nonexistent_id.to_vec() });

    let response = client.get_job_status(job_status_request).await;

    match response {
        Ok(_) => {
            eprintln!("get_job_status should have failed due to nonexistent job lookup");
            panic!();
        },

        Err(e) => {
            assert_eq!(e.code(), tonic::Code::NotFound);
        }
    }

    
}

#[tokio::test]
async fn get_job_status_invalid_id() {
    let (mut client, _clone_check) = bind_spawn_connect_for_tests().await;

    let invalid_uuid = vec![1, 2, 3];

    let job_status_request = tonic::Request::new(proto::JobIdRequest { id: invalid_uuid });

    let response = client.get_job_status(job_status_request).await;

    match response {
        Ok(_) => {
            eprintln!("get_job_status should have failed due to invalid uuid submitted");
            panic!();
        },

        Err(e) => {
            assert_eq!(e.code(), tonic::Code::InvalidArgument);
        }
    }


}