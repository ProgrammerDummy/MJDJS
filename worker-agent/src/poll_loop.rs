//call RequestWork rpc
//if there is no work available, then retry after a timeout, fixed (100 ms)? vs exponential backoff?
//remember to add jitter as well
//if there is work, then call executor and after call report_result RPC
//keep in-flight jobs in memory as well so request_work is not called to overload

//tracking mechanism for in-flight jobs since jobs run concurrentlyt and call executor concurrently too
//semaphore or atomic int?

use std::{collections::HashMap, sync::{Arc, atomic::AtomicBool}};

use parking_lot::Mutex;
use tokio::sync::Semaphore;

use scheduler_core::proto::{self, JobOutcome};
use tokio_util::sync::CancellationToken;
use tonic::Request;
use std::net::SocketAddr;

use crate::executor::{ExecutorRegistry};
use crate::heartbeat::sleep_timer;

pub async fn poll_loop(
    addr: SocketAddr,
    worker_uuid: Arc<Mutex<uuid::Uuid>>, 
    max_concurrent_jobs: u32, 
    token_map: Arc<Mutex<HashMap<uuid::Uuid, CancellationToken>>>,
    main_token: CancellationToken,
    stop_accepting_work: Arc<AtomicBool>,
    exec_registry: Arc<ExecutorRegistry>, 
    ) {

    tokio::select! {
        _ = main_token.cancelled() => {

        },

        _ = async {

            let mut connection_failures = 0; //serves as the outer loops retry for connection to server and determines timeout times

            loop {
                
                if connection_failures != 0 {
                    sleep_timer(connection_failures).await;
                }


                let displayable_worker_id = {
                    let guard = worker_uuid.lock();
                    guard.clone()
                };

                let channel = match tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                    .unwrap()
                    .connect()
                    .await 
                {

                    Ok(channel) => {
                        channel
                        
                    }
                    Err(e) => {
                        tracing::warn!(
                            worker_id = %displayable_worker_id,
                            "poll_loop could not connect to scheduler server, retrying"  
                        );
                        connection_failures += 1;
                        continue;
                    }
                };

                connection_failures = 0;

                let mut worker_client = proto::worker_service_client::WorkerServiceClient::new(channel);

                //create a semaphore from max_concurrent_jobs
                let current_jobs = Arc::new(Semaphore::new(max_concurrent_jobs as usize));
                
                
                //always have the cancellationtoken ready
                //race it with sleeps or entire loop?

                
                let mut empty_queue_polls = 0; //serves as the retry counter for the inner loop's timeouts

                loop {
                    

                    if empty_queue_polls != 0 {
                        sleep_timer(empty_queue_polls).await;
                    }
                    
                    //check if we have space by incrementing job pool semaphore

                    let Ok(job_slot) = current_jobs.clone().acquire_owned().await else {
                        tracing::warn!("acquiring current_jobs semaphore failed");
                        empty_queue_polls += 1;
                        continue;
                    };

                    let request = Request::new(proto::AssignedWorkerId { worker_id: displayable_worker_id.as_bytes().to_vec()});

                    let Ok(job) = worker_client.request_work(request).await else {
                        empty_queue_polls += 1;
                        continue;
                    };

                    let job = match job.into_inner().result {
                        Some(req_work_response) => {

                            if let proto::request_work_response::Result::Job(job) = req_work_response {
                                Some(job)
                            }

                            else {
                                None
                            }
                        },
                        
                        None => {
                            None
                        },
                    };


                    let Some(job) = job else {
                        empty_queue_polls += 1;
                        continue;
                    };

                    let executor = exec_registry.get(&job.job_type);

                    let Some(executor) = executor else {
                        //report that there was no executor for specific job type

                        //worker_client.report_result().await;


                        empty_queue_polls += 1;
                        continue;
                    };

                    let child_token = main_token.child_token();

                    let job_uuid = match uuid::Uuid::from_slice(&job.id) {
                        Ok(uuid) => {
                            uuid
                        }

                        Err(_) => {
                            tracing::warn!(
                                worker_id = %displayable_worker_id,
                                "invalid job uuid detected at worker"
                            );

                            break;
                            //i should just log this and continue
                        }
                    };


                    {
                        let mut token_map_guard = token_map.lock();
                        token_map_guard.insert(job_uuid, child_token.clone());
                    }  

                    empty_queue_polls = 0;

                    let mut worker_client_clone = worker_client.clone();

                    let worker_uuid_clone = worker_uuid.clone();

                    let token_map_clone = token_map.clone();

                    tokio::spawn(async move {
                        let exec_result = executor.execute(job.payload, child_token).await;
                        
                        //report the result through report_result RPC using worker_client_clone, and remove from token_map
                        //also translate the error type into an error code after getting exec_result

                        let job_outcome = match exec_result {
                            Ok(success_num) => {
                                Some(JobOutcome {
                                    outcome: Some(proto::job_outcome::Outcome::Success(
                                        proto::job_outcome::Success { result: success_num}
                                    ))
                                })      
                            }

                            Err(job_error) => {

                                if job_error.error_code_translation() == 103 {
                                    Some(JobOutcome { 
                                        outcome: Some(proto::job_outcome::Outcome::Cancelled(
                                            proto::job_outcome::Cancelled {}
                                        )) 
                                    })
                                }

                                else {
                                    Some(JobOutcome { 
                                        outcome: Some(proto::job_outcome::Outcome::Failure(
                                            proto::job_outcome::Failure { error: job_error.error_code_translation() }
                                        )) 
                                    })
                                }
                            }
                        };

                        let worker_id = {
                            let worker_uuid_guard = worker_uuid_clone.lock();
                            *worker_uuid_guard
                        };

                        let request = proto::JobResult {
                            job_id: job.id,
                            worker_id: worker_id.as_bytes().to_vec(),
                            job_outcome
                        };

                        //check returned value from server

                        worker_client_clone.report_result(Request::new(request)).await;

                        
                        {
                            //remove from token mapping
                            let mut token_map_guard = token_map_clone.lock();
                            token_map_guard.remove(&job_uuid);
                        }


                        drop(job_slot); //drop the semaphore within the spawned task after job finishes
                    });

                    
                    /*
                    
                    idea: 
                    - to prevent the blocking nature of .join, i can use a mpsc channel and send the sender side to another spawned task then have it feed in the result?
                    - and to 
                    */ 

                }
            
            }

        } => {}
    }


}


/*

2 loops similar idea to heartbeat.rs

outer loop handles connections and configuration

inner loop handles job request and execution and reporting of results


for a cancellationtoken to job mapping, use a parent cancellationtoken for main process and 
child_token for jobs

parent token is called for shutdowns 
child token called by scheduler-client cancel_job RPC, processed by server and sent through HeartbeatControl


todo:
- add a new oneof variant to HeartbeatControl for signaling for job cancellation

*/

