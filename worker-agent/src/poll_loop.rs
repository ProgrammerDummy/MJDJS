//call RequestWork rpc
//if there is no work available, then retry after a timeout, fixed (100 ms)? vs exponential backoff?
//remember to add jitter as well
//if there is work, then call executor and after call report_result RPC
//keep in-flight jobs in memory as well so request_work is not called to overload

//tracking mechanism for in-flight jobs since jobs run concurrentlyt and call executor concurrently too
//semaphore or atomic int?

use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicBool},
};

use parking_lot::Mutex;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use scheduler_core::proto::{self};
use std::net::SocketAddr;
use tokio_util::sync::CancellationToken;
use tonic::{Request, transport::Channel};

use proto::worker_service_client::WorkerServiceClient;
use std::sync::atomic::Ordering;

use crate::executor::{ExecutorRegistry, JobError, JobExecutor};
use crate::heartbeat::sleep_timer;

/// Every dispatch this worker holds, keyed by (job_id, dispatch_token) so two dispatches of the
/// same job can coexist without overwriting each other. The value is the dispatch's cancellation
/// token and its "revoked" flag: whoever cancels because the lease was lost (CancelJob REVOKED,
/// re-registration) sets the flag *before* cancelling, so run_job knows not to report.
pub type TokenMap = Arc<Mutex<HashMap<(uuid::Uuid, u64), (CancellationToken, Arc<AtomicBool>)>>>;

//note for future: maybe I can do a dashmap here instead of a hashmap?

const DRAIN_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

const MAX_TRANSPORT_FAILURES: u32 = 5;

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
        Ok(result) => proto::job_outcome::Outcome::Success(proto::job_outcome::Success { result }),
        Err(JobError::Cancelled) => {
            proto::job_outcome::Outcome::Cancelled(proto::job_outcome::Cancelled {})
        }
        Err(e) => proto::job_outcome::Outcome::Failure(proto::job_outcome::Failure {
            error: e.error_code_translation(),
        }),
    };
    proto::JobOutcome {
        outcome: Some(outcome),
    }
}

fn build_result(
    job_id: Vec<u8>,
    worker_id: uuid::Uuid,
    outcome: proto::JobOutcome,
    dispatch_token: u64,
) -> proto::JobResult {
    proto::JobResult {
        job_id,
        worker_id: worker_id.as_bytes().to_vec(),
        job_outcome: Some(outcome),
        dispatch_token, //echo of the token the job was dispatched with
    }
}

//I/O helper functions for retrying for report_result and reconnection with exp backoff for channel connection to gRPC server

pub async fn connect_with_backoff(addr: SocketAddr) -> WorkerServiceClient<Channel> {
    //loops and attempts to connect with exponential backoff in retries
    //will not return unless connection is successful

    let mut retry_attempts = 0u32;
    loop {
        if retry_attempts != 0 {
            sleep_timer(retry_attempts).await;
        }

        let Ok(channel) = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
        else {
            retry_attempts += 1;
            continue;
        };

        return proto::worker_service_client::WorkerServiceClient::new(channel);
    }
}

/// Retries transport failures; gives up on semantic rejections
/// (NOT_FOUND, INVALID_ARGUMENT).
async fn report_with_retry(client: &mut WorkerServiceClient<Channel>, result: proto::JobResult) {
    let mut retry_attempts = 0u32;

    const MAX_REPORT_ATTEMPTS: u32 = 5;

    loop {
        if retry_attempts != 0 {
            sleep_timer(retry_attempts).await;
        }

        match client.report_result(Request::new(result.clone())).await {
            Ok(_) => return,
            Err(e) => {
                match e.code() {
                    tonic::Code::Unknown
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::Unavailable => {
                        //transport failures
                        if retry_attempts < MAX_REPORT_ATTEMPTS {
                            retry_attempts += 1;
                            continue;
                        }

                        tracing::warn!(
                            "transport failures continued past limit for report_result, abandoning job"
                        );

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
    worker_uuid: uuid::Uuid,
    token_map: TokenMap,
    _permit: OwnedSemaphorePermit, // held for the task's lifetime, dropped on return
) {
    let job_id = job.id.clone();
    let dispatch_token = job.dispatch_token;
    let exec_result = executor.execute(job.payload, cancel).await;

    //if this dispatch's revoked flag is set, skip the report
    //regardless of flag's state, the removal from token_map below must still happen

    let report_skip = {
        let token_map_guard = token_map.lock();
        match token_map_guard.get(&(job_uuid, dispatch_token)) {
            Some((_, skip_report_flag)) => {
            skip_report_flag.load(Ordering::SeqCst)
            }

            None => {
                tracing::warn!("job unable to be found during run_job at worker: {worker_uuid}");
                false
            }
        }
    };

    if !report_skip {
        report_with_retry(
            &mut client,
            build_result(job_id, worker_uuid, outcome_from(exec_result), dispatch_token),
        )
        .await;
    }

    //removed only after the report finishes, so a heartbeat can never omit a dispatch whose report is in flight
    token_map.lock().remove(&(job_uuid, dispatch_token));
} //spawned separately and detached

async fn work_loop(
    mut client: WorkerServiceClient<Channel>,
    worker_uuid: Arc<Mutex<uuid::Uuid>>,
    semaphore: Arc<Semaphore>,
    token_map: TokenMap,
    main_token: &CancellationToken,
    stop_accepting_work: Arc<AtomicBool>,
    shutting_down: Arc<AtomicBool>,
    registry: Arc<ExecutorRegistry>,
    max_concurrent_jobs: u32,
    needs_reregister: Arc<Notify>,
    undergoing_reregistration: Arc<Mutex<AtomicBool>>,
) {
    let mut idle_polls = 0;

    let mut transport_failures = 0u32;

    loop {
        if idle_polls != 0 {
            //sleep with exponential backoff
            sleep_timer(idle_polls).await;
        }

        if stop_accepting_work.load(Ordering::Relaxed) || shutting_down.load(Ordering::Relaxed) {
            //load in latest status of stop_accepting_work
            //if its true, then the jobs must drain so dont request any more work but stay alive for jobs to finish
            tokio::time::sleep(DRAIN_POLL_INTERVAL).await;

            if semaphore.available_permits() == max_concurrent_jobs as usize {
                //if semaphore is completely vacant, then jobs are done draining
                return;
            }

            continue;
        }

        let Ok(permit) = semaphore.clone().acquire_owned().await else {
            return; // semaphore closed, which is permanent on non-retryable
        };

        let worker_id = *worker_uuid.lock();
        let request = Request::new(proto::AssignedWorkerId {
            worker_id: worker_id.as_bytes().to_vec(),
        });

        let response = match client.request_work(request).await {
            Ok(r) => {
                transport_failures = 0;
                r.into_inner()
            }

            Err(status) => {
                //NotFound here means stale registration
                //heartbeat.rs should fix this by itself so backoff for now regardless of what kind of error
                if status.code() == tonic::Code::Unknown
                    || status.code() == tonic::Code::Unavailable
                    || status.code() == tonic::Code::DeadlineExceeded
                {
                    transport_failures += 1;
                } else if status.code() == tonic::Code::NotFound {
                    //possible fix: branch condition on tonic::Code::NotFound? --> applied

                    {
                        let guard = undergoing_reregistration.lock();
                        if guard.load(Ordering::Relaxed) {
                            idle_polls += 1;
                            continue;
                        }
                    }

                    {
                        let possibly_changed_worker_id = *worker_uuid.lock();
                        if possibly_changed_worker_id == worker_id {
                            needs_reregister.notify_one();
                        }
                    }
                    //for a dead worker, such as one that loses network connection past the TTL
                    //its jobs will be collected by scheduler and will be marked as dead
                    //but if it reconnects afterwards itll be a useless worker and NotFound will be sent in response to request_work
                    //so reregister it with new uuid instead of resurrectring it
                }

                if transport_failures > MAX_TRANSPORT_FAILURES {
                    return; //if transport failures keep occuring return early to try reconnecting at poll_loop
                }

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
            let outcome = outcome_from(Err(JobError::NoExecutor));
            report_with_retry(&mut client, build_result(job.id, worker_id, outcome, job.dispatch_token)).await;
            idle_polls += 1;
            continue;
        };

        idle_polls = 0;

        let cancel = main_token.child_token();
        token_map.lock().insert(
            (job_uuid, job.dispatch_token),
            (cancel.clone(), Arc::new(AtomicBool::new(false))),
        );

        tokio::spawn(run_job(
            job,
            job_uuid,
            executor,
            cancel,
            client.clone(),
            worker_id,
            token_map.clone(),
            permit,
        ));
    }
}

pub async fn poll_loop(
    addr: SocketAddr,
    worker_uuid: Arc<Mutex<uuid::Uuid>>,
    max_concurrent_jobs: u32,
    token_map: TokenMap,
    main_token: CancellationToken,
    stop_accepting_work: Arc<AtomicBool>,
    shutting_down: Arc<AtomicBool>,
    registry: Arc<ExecutorRegistry>,
    needs_reregister: Arc<Notify>,
    undergoing_reregistration: Arc<Mutex<AtomicBool>>,
) {
    let semaphore = Arc::new(Semaphore::new(max_concurrent_jobs as usize));

    tokio::select! {
        _ = main_token.cancelled() => {}
        _ = async {
            loop { //outer connection loop

                let client = connect_with_backoff(addr).await;

                work_loop( //inner work loop
                    client, worker_uuid.clone(), semaphore.clone(),
                    token_map.clone(),
                    &main_token,
                    stop_accepting_work.clone(),
                    shutting_down.clone(),
                    registry.clone(),
                    max_concurrent_jobs,
                    needs_reregister.clone(),
                    undergoing_reregistration.clone(),
                ).await;
                //if the work_loop returns, then the connection died so reconnect

                //unless the shutting_down flag is active
                //then it means i should stop it

                if shutting_down.load(Ordering::Relaxed) {
                    break;
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
