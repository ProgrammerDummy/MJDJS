use scheduler_core::proto::{JobStatus, SubmitJobResponse};
use scheduler_core::{conversion::{ConversionError, job_state_to_proto, proto_to_job_status}};
use scheduler_core::job_data_structures::{Job, JobState, RetryPolicy};
use scheduler_core::proto::{self, scheduler_service_server::SchedulerService};
use tonic::{Request, Response, Status};
use std::collections::HashMap;
use std::convert::TryFrom;
use futures::stream::BoxStream;
use uuid;
use std::sync::{Arc, Mutex};

use scheduler_core::proto::scheduler_service_server::SchedulerServiceServer;
use scheduler_core::proto::scheduler_service_client;

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
        let job_id_request = request.into_inner();

        let job_id_request = uuid::Uuid::from_slice(job_id_request.id.as_slice());

        match job_id_request {
            Ok(job_uuid) => {
                let jobs_lock = self.jobs.lock().unwrap();
                let job = jobs_lock.get(&job_uuid);
                
                match job {
                    Some(job) => {
                        match job_state_to_proto(job.state.clone()) {
                            Ok(proto_job_status) => {
                                return Ok(tonic::Response::new(proto_job_status))
                            },

                            Err(e) => {
                                return Err(tonic::Status::internal("invalid job entered the system")) //this shouldn't occur due to the entrypoint having bounds, if this happens something has gone very wrong
                            }
                        }

                    },

                    None => {
                        return Err::<Response<proto::JobStatus>, Status>(tonic::Status::not_found("job was not found in MySchedulerService"));
                    },
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


async fn bind_spawn_connect_for_tests() -> (scheduler_service_client::SchedulerServiceClient<tonic::transport::Channel>, Arc<Mutex<std::collections::HashMap<uuid::Uuid, Job>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tonic::transport::server::TcpIncoming::from(listener);

    let service = MySchedulerService::new();

    let clone_check = service.jobs.clone();

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
                        let jobs_stored = clone_check.lock().unwrap();
                        match jobs_stored.get(&id) {
                            Some(_) => {},
                            None => {
                                panic!();
                            }
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
    let (mut client, clone_check) = bind_spawn_connect_for_tests().await;

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
    let (mut client, clone_check) = bind_spawn_connect_for_tests().await;

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