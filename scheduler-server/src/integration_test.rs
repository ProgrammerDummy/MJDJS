#![cfg(test)]

use std::collections::HashMap;
use std::time::Duration;

use scheduler_core::job_data_structures::{Job, JobState, RetryPolicy};
use scheduler_core::proto::{
    self, scheduler_service_client::SchedulerServiceClient,
    scheduler_service_server::SchedulerServiceServer,
    worker_service_client::WorkerServiceClient,
    worker_service_server::WorkerServiceServer,
};
use tonic::transport::Channel;

use crate::background::{run_death_detector, run_retry_queue_monitor};
use crate::scheduler_state::SchedulerState;
use crate::service::MySchedulerService;
use crate::worker_service::MyWorkerService;

/// Spins up a full server (both services + both background tasks) on a random
/// port and returns clients for each, plus a handle to the shared state.
async fn spawn_full_server() -> (
    SchedulerServiceClient<Channel>,
    WorkerServiceClient<Channel>,
    SchedulerState,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tonic::transport::server::TcpIncoming::from(listener);

    let state = SchedulerState::new();

    let scheduler_service = MySchedulerService::from_state(state.clone());
    let worker_service = MyWorkerService::new(state.clone());

    tokio::spawn(run_retry_queue_monitor(state.clone()));
    tokio::spawn(run_death_detector(state.clone()));

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(SchedulerServiceServer::new(scheduler_service))
            .add_service(WorkerServiceServer::new(worker_service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    (
        SchedulerServiceClient::new(channel.clone()),
        WorkerServiceClient::new(channel),
        state,
    )
}

fn test_job(job_type: &str, payload: u64, retry_policy: RetryPolicy) -> proto::Job {
    let job = Job {
        id: uuid::Uuid::now_v7(),
        job_type: job_type.to_string(),
        payload,
        priority: 1,
        retry_count: 0,
        infra_interruptions: 0,
        created_at: 0,
        state: JobState::Queued,
        retry_policy,
        requirements: HashMap::new(),
        metadata: HashMap::new(),
    };
    proto::Job::try_from(job).unwrap()
}

fn test_worker(job_types: &[&str], max_concurrent_jobs: u32) -> proto::Worker {
    proto::Worker {
        hostname: "test-host".to_string(),
        job_types_supported: job_types.iter().map(|s| s.to_string()).collect(),
        capabilities: HashMap::new(),
        tags: HashMap::new(),
        max_concurrent_jobs,
    }
}

async fn status_of(
    client: &mut SchedulerServiceClient<Channel>,
    job_id: &[u8],
) -> JobState {
    let resp = client
        .get_job_status(tonic::Request::new(proto::JobIdRequest {
            id: job_id.to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    scheduler_core::conversion::proto_to_job_status(resp.state).unwrap()
}


//tests begin here

#[tokio::test]
async fn full_lifecycle_success() {
    let (mut sched, mut worker, state) = spawn_full_server().await;

    // register
    let worker_id = worker
        .register(tonic::Request::new(test_worker(&["sleep"], 2)))
        .await
        .unwrap()
        .into_inner()
        .worker_id;

    // submit
    let job_id = sched
        .submit_job(tonic::Request::new(test_job(
            "sleep",
            50,
            RetryPolicy::NoRetry,
        )))
        .await
        .unwrap()
        .into_inner()
        .id;

    assert_eq!(status_of(&mut sched, &job_id).await, JobState::Queued);

    // dispatch
    let resp = worker
        .request_work(tonic::Request::new(proto::AssignedWorkerId {
            worker_id: worker_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner();

    let dispatched = match resp.result {
        Some(proto::request_work_response::Result::Job(j)) => j,
        other => panic!("expected a job, got {other:?}"),
    };
    assert_eq!(dispatched.id, job_id);

    match status_of(&mut sched, &job_id).await {
        JobState::Running { .. } => {}
        other => panic!("expected Running, got {other:?}"),
    }

    // report success
    worker
        .report_result(tonic::Request::new(proto::JobResult {
            job_id: job_id.clone(),
            worker_id: worker_id.clone(),
            job_outcome: Some(proto::JobOutcome {
                outcome: Some(proto::job_outcome::Outcome::Success(
                    proto::job_outcome::Success { result: 42 },
                )),
            }),
        }))
        .await
        .unwrap();

    match status_of(&mut sched, &job_id).await {
        JobState::Succeeded { result, .. } => assert_eq!(result, 42),
        other => panic!("expected Succeeded, got {other:?}"),
    }

    // state cleanup
    let uuid = uuid::Uuid::from_slice(&job_id).unwrap();
    let worker_uuid = uuid::Uuid::from_slice(&worker_id).unwrap();
    assert!(!state.running_jobs.contains_key(&uuid));
    assert!(state.completed_jobs.contains_key(&uuid));
    assert!(state.workers.get(&worker_uuid).unwrap().assigned_jobs.is_empty());
    assert_eq!(
        state.total_completed.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn failure_retries_then_requeues() {
    let (mut sched, mut worker, state) = spawn_full_server().await;

    let worker_id = worker
        .register(tonic::Request::new(test_worker(&["sleep"], 1)))
        .await
        .unwrap()
        .into_inner()
        .worker_id;

    let job_id = sched
        .submit_job(tonic::Request::new(test_job(
            "sleep",
            10,
            RetryPolicy::FixedDelay {
                delay_ms: 100,
                max_attempts: 3,
            },
        )))
        .await
        .unwrap()
        .into_inner()
        .id;

    worker
        .request_work(tonic::Request::new(proto::AssignedWorkerId {
            worker_id: worker_id.clone(),
        }))
        .await
        .unwrap();

    worker
        .report_result(tonic::Request::new(proto::JobResult {
            job_id: job_id.clone(),
            worker_id: worker_id.clone(),
            job_outcome: Some(proto::JobOutcome {
                outcome: Some(proto::job_outcome::Outcome::Failure(
                    proto::job_outcome::Failure { error: 7 },
                )),
            }),
        }))
        .await
        .unwrap();

    match status_of(&mut sched, &job_id).await {
        JobState::Retrying { .. } => {}
        other => panic!("expected Retrying, got {other:?}"),
    }

    // the retry monitor should put it back on the queue once the delay elapses
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(status_of(&mut sched, &job_id).await, JobState::Queued);

    let uuid = uuid::Uuid::from_slice(&job_id).unwrap();
    assert!(!state.running_jobs.contains_key(&uuid));
    let queued = state.job_queue.lock();
    let requeued = queued.iter().find(|j| j.id == uuid).expect("job requeued");
    assert_eq!(requeued.retry_count, 1);
}