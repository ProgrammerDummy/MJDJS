//call RequestWork rpc
//if there is no work available, then retry after a timeout, fixed (100 ms)? vs exponential backoff?
//remember to add jitter as well
//if there is work, then call executor and after call report_result RPC
//keep in-flight jobs in memory as well so request_work is not called to overload

//tracking mechanism for in-flight jobs since jobs run concurrentlyt and call executor concurrently too
//semaphore or atomic int?

use std::{collections::HashMap, sync::{Arc, atomic::AtomicBool}};

use parking_lot::Mutex;
use tokio::sync::{Semaphore, OwnedSemaphorePermit};

use scheduler_core::proto::{self, JobOutcome};
use tokio_util::sync::CancellationToken;
use tonic::{Request, transport::Channel};
use std::net::SocketAddr;

use std::sync::atomic::Ordering;
use proto::worker_service_client::WorkerServiceClient;


use crate::executor::{ExecutorRegistry, JobError, JobExecutor};
use crate::heartbeat::sleep_timer;


const DRAIN_POLL_INTERVAL : std::time::Duration = std::time::Duration::from_millis(500);


//basic helper functions for jobs, outcomes, and results to send through for report_result RPC
//to help wih code conciseness and clarity

fn extract_job(response: proto::RequestWorkResponse) -> Option<proto::Job> {
    match response.result? {
        proto::request_work_response::Result::Job(job) => Some(job),
        proto::request_work_response::Result::None(_) => None,
    }
}

fn outcome_from(exec_result: Result<u64, JobError>) -> proto::JobOutcome {
    let outcome = match exec_result {
        Ok(result) => proto::job_outcome::Outcome::Success(
            proto::job_outcome::Success { result }
        ),
        Err(JobError::Cancelled) => proto::job_outcome::Outcome::Cancelled(
            proto::job_outcome::Cancelled {}
        ),
        Err(e) => proto::job_outcome::Outcome::Failure(
            proto::job_outcome::Failure { error: e.error_code_translation() }
        ),
    };
    proto::JobOutcome { outcome: Some(outcome) }
}

fn build_result(job_id: Vec<u8>, worker_id: uuid::Uuid, outcome: proto::JobOutcome) -> proto::JobResult {
    proto::JobResult {
        job_id,
        worker_id: worker_id.as_bytes().to_vec(),
        job_outcome: Some(outcome),
    }
}



//I/O helper functions for retrying for report_result and reconnection with exp backoff for channel connection to gRPC server

async fn connect_with_backoff(addr: SocketAddr) -> WorkerServiceClient<Channel> {
    //loops and attempts to connect with exponential backoff in retries
    //will not return unless connection is successful

    let mut retry_attempts = 0u32;
    loop {
        if(retry_attempts != 0) {
            sleep_timer(retry_attempts).await;
        }

        let Ok(channel) = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await else {
                retry_attempts += 1;
                continue;
        };

        return proto::worker_service_client::WorkerServiceClient::new(channel)


    }
}

/// Retries transport failures; gives up on semantic rejections
/// (NOT_FOUND, FAILED_PRECONDITION, INVALID_ARGUMENT).
async fn report_with_retry(
    client: &mut WorkerServiceClient<Channel>,
    result: proto::JobResult,
) {

    let mut retry_attempts = 0u32;

    const MAX_REPORT_ATTEMPTS : u32 = 3;
    

    loop {
        if retry_attempts != 0 {
            sleep_timer(retry_attempts).await;
        }  

        match client.report_result(Request::new(result.clone())).await {
            Ok(_) => return,
            Err(e) => {
                match e.code() {
                    tonic::Code::Unknown | tonic::Code::DeadlineExceeded | tonic::Code::Unavailable => {
                        if(retry_attempts < MAX_REPORT_ATTEMPTS) {
                            retry_attempts += 1;
                            continue;
                        }

                        return;
                    }

                    _ => {
                        tracing::warn!("report request was rejected");
                        return;
                    }
                }
            }
        }
    }
}




async fn run_job(
    job: proto::Job,
    job_uuid: uuid::Uuid,
    executor: Arc<dyn JobExecutor>,
    cancel: CancellationToken,
    mut client: WorkerServiceClient<Channel>,
    worker_uuid: Arc<Mutex<uuid::Uuid>>,
    token_map: Arc<Mutex<HashMap<uuid::Uuid, CancellationToken>>>,
    _permit: OwnedSemaphorePermit,   // held for the task's lifetime, dropped on return
) {
    let job_id = job.id.clone();
    let exec_result = executor.execute(job.payload, cancel).await;

    let worker_id = *worker_uuid.lock();
    report_with_retry(&mut client, build_result(job_id, worker_id, outcome_from(exec_result))).await;

    token_map.lock().remove(&job_uuid);
} //spawned separately and detached




async fn work_loop(
    mut client: WorkerServiceClient<Channel>,
    worker_uuid: Arc<Mutex<uuid::Uuid>>,
    semaphore: Arc<Semaphore>,
    token_map: Arc<Mutex<HashMap<uuid::Uuid, CancellationToken>>>,
    main_token: &CancellationToken,
    stop_accepting_work: Arc<AtomicBool>,
    registry: Arc<ExecutorRegistry>,
) {
    let mut idle_polls = 0;

    loop {
        if idle_polls != 0 { //sleep with exponential backoff
            sleep_timer(idle_polls).await; 
        }

        if stop_accepting_work.load(Ordering::Relaxed) { //load in latest status of stop_accepting_work
            //if its true, then the jobs must drain so dont request any more work but stay alive for jobs to finish
            tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
            continue;
        }

        let Ok(permit) = semaphore.clone().acquire_owned().await else {
            return;   // semaphore closed, which is permanent on non-retryable
        };

        let worker_id = *worker_uuid.lock();
        let request = Request::new(proto::AssignedWorkerId {
            worker_id: worker_id.as_bytes().to_vec(),
        });

        let response = match client.request_work(request).await {
            Ok(r) => r.into_inner(),
            Err(status) => {
                //FAILED_PRECONDITION here means stale registration 
                //heartbeat.rs should fix this by itself so backoff for now regardless of what kind of error 
                idle_polls += 1;
                continue;
            }
        };

        let Some(job) = extract_job(response) else {
            idle_polls += 1;
            continue;
        };

        let job_uuid = match uuid::Uuid::from_slice(&job.id) {
            Ok(u) => u,
            Err(_) => { 
                tracing::warn!(worker_id = %worker_id, "invalid job uuid was detected at worker");
                idle_polls += 1; 
                continue; 
            }   
        };

        let Some(executor) = registry.get(&job.job_type) else {
            let worker_id = *worker_uuid.lock();
            let outcome = outcome_from(Err(JobError::NoExecutor));
            report_with_retry(&mut client, build_result(job.id, worker_id, outcome)).await;
            idle_polls += 1;
            continue;
        };

        idle_polls = 0;

        let cancel = main_token.child_token();
        token_map.lock().insert(job_uuid, cancel.clone());

        tokio::spawn(run_job(
            job, job_uuid, executor, cancel,
            client.clone(), worker_uuid.clone(), token_map.clone(), permit,
        ));
    }
}

pub async fn poll_loop(
    addr: SocketAddr,
    worker_uuid: Arc<Mutex<uuid::Uuid>>,
    max_concurrent_jobs: u32,
    token_map: Arc<Mutex<HashMap<uuid::Uuid, CancellationToken>>>,
    main_token: CancellationToken,
    stop_accepting_work: Arc<AtomicBool>,
    registry: Arc<ExecutorRegistry>,
) {
    let semaphore = Arc::new(Semaphore::new(max_concurrent_jobs as usize));

    tokio::select! {
        _ = main_token.cancelled() => {}
        _ = async {
            loop { //outer connection loop

                let client = connect_with_backoff(addr).await;

                work_loop( //inner work loop
                    client, worker_uuid.clone(), semaphore.clone(),
                    token_map.clone(), &main_token,
                    stop_accepting_work.clone(), registry.clone(),
                ).await;
                //if the work_loop returns, then the connection died so reconnect
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

