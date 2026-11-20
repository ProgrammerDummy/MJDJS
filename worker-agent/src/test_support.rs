//! Test harness for worker-agent.
//!
//! `FakeScheduler` implements the real `WorkerService` over a real socket, so the worker code under
//! test (run_job, work_loop, send_heartbeats) talks to it exactly as it would talk to
//! scheduler-server. It records everything the worker sends, and it can be told to forget a worker,
//! after which `request_work` and `heartbeat` answer NOT_FOUND, the same way the real server does
//! once a worker's heartbeat lease has expired.
//!
//! Every lock taken here is released before any `.await`.
//!
//! Where tests go: a test of a private function lives in that function's own module (a
//! `#[cfg(test)] mod tests` at the bottom of poll_loop.rs, heartbeat.rs or lifecycle.rs), because
//! child modules can see their parent's private items. Bring the harness in with
//! `use crate::test_support::*;`.
//!
//! Long-running loops (send_heartbeats, work_loop) never return on their own: spawn them, keep the
//! JoinHandle, and call `.abort()` on it at the end of the test.

//not every test uses every helper
#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use parking_lot::Mutex;
use scheduler_core::proto::{
    self,
    worker_service_client::WorkerServiceClient,
    worker_service_server::{WorkerService, WorkerServiceServer},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::executor::{ExecutorRegistry, JobExecutor, SleepJob};
use crate::poll_loop::TokenMap;

/// Everything the fake scheduler knows and has seen.
#[derive(Default)]
struct FakeState {
    /// raw worker ids the fake currently recognises
    known_workers: HashSet<Vec<u8>>,
    /// how many times Register was called
    registrations: usize,
    /// every JobResult received, in arrival order
    reports: Vec<proto::JobResult>,
    /// every HeartbeatPing received, in arrival order
    pings: Vec<proto::HeartbeatPing>,
    /// handed out by RequestWork in order; NoWorkAvailable once empty
    jobs_to_hand_out: VecDeque<proto::Job>,
    /// how many times RequestWork was called
    request_work_calls: usize,
    /// when true, Register answers INVALID_ARGUMENT, which register_with_retry does not retry
    reject_registrations: bool,
    /// the most recently opened heartbeat stream, used by push_directive
    heartbeat_tx: Option<mpsc::Sender<Result<proto::HeartbeatControl, Status>>>,
}

/// A handle to a running fake scheduler. Cheap to clone; all clones share the same state.
#[derive(Clone, Default)]
pub struct FakeScheduler {
    state: Arc<Mutex<FakeState>>,
}

impl FakeScheduler {
    /// Starts a fake scheduler on an ephemeral localhost port and returns its address and a handle.
    pub async fn spawn() -> (SocketAddr, FakeScheduler) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tonic::transport::server::TcpIncoming::from(listener);

        let fake = FakeScheduler::default();
        let service = fake.clone();

        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(WorkerServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        (addr, fake)
    }

    /// Registers a worker directly (no RPC) and returns its id, for tests that want a worker the
    /// fake already recognises without going through Register.
    pub fn add_worker(&self) -> uuid::Uuid {
        let id = uuid::Uuid::now_v7();
        self.state
            .lock()
            .known_workers
            .insert(id.as_bytes().to_vec());
        id
    }

    /// Forgets a worker: from now on its request_work and heartbeat calls get NOT_FOUND, as with
    /// the real server after the death detector removes it.
    pub fn forget_worker(&self, id: uuid::Uuid) {
        self.state
            .lock()
            .known_workers
            .remove(id.as_bytes().as_slice());
    }

    /// Queues a job for the next RequestWork call.
    pub fn queue_job(&self, job: proto::Job) {
        self.state.lock().jobs_to_hand_out.push_back(job);
    }

    /// Every JobResult received so far, in arrival order.
    pub fn reports(&self) -> Vec<proto::JobResult> {
        self.state.lock().reports.clone()
    }

    /// Every HeartbeatPing received so far, in arrival order.
    pub fn pings(&self) -> Vec<proto::HeartbeatPing> {
        self.state.lock().pings.clone()
    }

    /// How many times Register has been called.
    pub fn registrations(&self) -> usize {
        self.state.lock().registrations
    }

    /// From now on Register answers INVALID_ARGUMENT, so register_with_retry returns None.
    pub fn reject_registrations(&self) {
        self.state.lock().reject_registrations = true;
    }

    /// How many times RequestWork has been called.
    pub fn request_work_calls(&self) -> usize {
        self.state.lock().request_work_calls
    }

    /// Sends a directive down the most recently opened heartbeat stream, as the real server does
    /// for CancelJob. Panics if no heartbeat stream is open.
    pub async fn push_directive(&self, directive: proto::heartbeat_control::Directive) {
        let tx = self
            .state
            .lock()
            .heartbeat_tx
            .clone()
            .expect("push_directive called before any heartbeat stream was opened");

        tx.send(Ok(proto::HeartbeatControl {
            directive: Some(directive),
        }))
        .await
        .expect("heartbeat stream closed");
    }
}

fn not_found() -> Status {
    Status::not_found("worker is not registered with the fake scheduler")
}

#[tonic::async_trait]
impl WorkerService for FakeScheduler {
    async fn register(
        &self,
        _request: Request<proto::Worker>,
    ) -> Result<Response<proto::AssignedWorkerId>, Status> {
        let id = uuid::Uuid::now_v7().as_bytes().to_vec();

        let mut state = self.state.lock();
        state.registrations += 1;

        if state.reject_registrations {
            return Err(Status::invalid_argument(
                "fake scheduler is rejecting registrations",
            ));
        }

        state.known_workers.insert(id.clone());

        Ok(Response::new(proto::AssignedWorkerId { worker_id: id }))
    }

    async fn report_result(
        &self,
        request: Request<proto::JobResult>,
    ) -> Result<Response<()>, Status> {
        self.state.lock().reports.push(request.into_inner());
        Ok(Response::new(()))
    }

    async fn request_work(
        &self,
        request: Request<proto::AssignedWorkerId>,
    ) -> Result<Response<proto::RequestWorkResponse>, Status> {
        let worker_id = request.into_inner().worker_id;

        let mut state = self.state.lock();
        state.request_work_calls += 1;

        if !state.known_workers.contains(&worker_id) {
            return Err(not_found());
        }

        let result = match state.jobs_to_hand_out.pop_front() {
            Some(job) => proto::request_work_response::Result::Job(job),
            None => proto::request_work_response::Result::None(proto::NoWorkAvailable {}),
        };

        Ok(Response::new(proto::RequestWorkResponse {
            result: Some(result),
        }))
    }

    type HeartbeatStream =
        Pin<Box<dyn Stream<Item = Result<proto::HeartbeatControl, Status>> + Send + 'static>>;

    /// Records every ping. An unknown worker gets NOT_FOUND and the stream closes; a known worker
    /// gets an Ack per ping. Directives pushed with push_directive go down the same stream.
    async fn heartbeat(
        &self,
        request: Request<tonic::Streaming<proto::HeartbeatPing>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let mut pings = request.into_inner();
        let (tx, rx) = mpsc::channel(16);

        self.state.lock().heartbeat_tx = Some(tx.clone());

        let state = self.state.clone();

        tokio::spawn(async move {
            while let Ok(Some(ping)) = pings.message().await {
                let known = {
                    let mut state = state.lock();
                    let known = state.known_workers.contains(&ping.worker_id);
                    state.pings.push(ping);
                    known
                };

                if !known {
                    let _ = tx.send(Err(not_found())).await;
                    break;
                }

                let ack = proto::HeartbeatControl {
                    directive: Some(proto::heartbeat_control::Directive::Ack(proto::Ack {})),
                };

                if tx.send(Ok(ack)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}

/// A client connected to `addr`.
pub async fn client(addr: SocketAddr) -> WorkerServiceClient<Channel> {
    crate::poll_loop::connect_with_backoff(addr).await
}

/// A "sleep" job that runs for `payload_ms` milliseconds, with the given dispatch token.
/// Returns the proto job and its id.
pub fn test_job(payload_ms: u64, dispatch_token: u64) -> (proto::Job, uuid::Uuid) {
    let id = uuid::Uuid::now_v7();

    let job = proto::Job {
        id: id.as_bytes().to_vec(),
        job_type: "sleep".to_string(),
        payload: payload_ms,
        dispatch_token,
        ..Default::default()
    };

    (job, id)
}

/// The executor for "sleep" jobs: finishes after `payload` ms, or returns Cancelled if its token
/// is cancelled first.
pub fn sleep_executor() -> Arc<dyn JobExecutor> {
    Arc::new(SleepJob {})
}

/// Polls `condition` every 10ms until it is true. Panics with `what` if it is still false after
/// `timeout`. Use this instead of fixed sleeps: it is as fast as the code allows and fails loudly.
pub async fn eventually(timeout: Duration, what: &str, condition: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;

    while !condition() {
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out after {timeout:?} waiting for: {what}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The Worker message a test sends to Register.
pub fn test_worker() -> proto::Worker {
    proto::Worker {
        hostname: "test-host".to_string(),
        job_types_supported: vec!["sleep".to_string()],
        max_concurrent_jobs: 1,
        ..Default::default()
    }
}

/// A registry with only the "sleep" executor.
pub fn test_registry() -> Arc<ExecutorRegistry> {
    let mut registry = ExecutorRegistry::new();
    registry.register("sleep".into(), sleep_executor());
    Arc::new(registry)
}

/// An empty TokenMap.
pub fn empty_token_map() -> TokenMap {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Inserts a dispatch into `token_map` the way work_loop does (fresh token, flag false) and
/// returns clones of both, so the test can hand the token to run_job and inspect the flag later.
pub fn insert_dispatch(
    token_map: &TokenMap,
    job_uuid: uuid::Uuid,
    dispatch_token: u64,
    parent: &CancellationToken,
) -> (CancellationToken, Arc<AtomicBool>) {
    let cancel = parent.child_token();
    let revoked = Arc::new(AtomicBool::new(false));

    token_map.lock().insert(
        (job_uuid, dispatch_token),
        (cancel.clone(), revoked.clone()),
    );

    (cancel, revoked)
}

/// A semaphore permit for calling run_job directly.
pub async fn permit() -> OwnedSemaphorePermit {
    Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap()
}

/// Smoke tests for the harness itself.
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fake_records_register_dispatch_and_report() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let mut client = client(addr).await;

        let worker_id = client
            .register(Request::new(proto::Worker::default()))
            .await
            .expect("register failed")
            .into_inner()
            .worker_id;
        assert_eq!(fake.registrations(), 1);

        let (job, _) = test_job(10, 7);
        fake.queue_job(job.clone());

        let response = client
            .request_work(Request::new(proto::AssignedWorkerId {
                worker_id: worker_id.clone(),
            }))
            .await
            .expect("request_work failed")
            .into_inner();
        assert_eq!(
            response.result,
            Some(proto::request_work_response::Result::Job(job.clone()))
        );
        assert_eq!(fake.request_work_calls(), 1);

        let report = proto::JobResult {
            job_id: job.id.clone(),
            worker_id,
            job_outcome: None,
            dispatch_token: 7,
        };
        client
            .report_result(Request::new(report.clone()))
            .await
            .expect("report_result failed");
        assert_eq!(fake.reports(), vec![report]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fake_answers_not_found_for_a_forgotten_worker() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let mut client = client(addr).await;

        let worker = fake.add_worker();
        fake.forget_worker(worker);

        let err = client
            .request_work(Request::new(proto::AssignedWorkerId {
                worker_id: worker.as_bytes().to_vec(),
            }))
            .await
            .expect_err("forgotten worker was given work");
        assert_eq!(err.code(), tonic::Code::NotFound);

        let (tx, rx) = mpsc::channel(4);
        let mut controls = client
            .heartbeat(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .expect("heartbeat RPC failed")
            .into_inner();

        tx.send(proto::HeartbeatPing {
            worker_id: worker.as_bytes().to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

        let err = controls
            .message()
            .await
            .expect_err("forgotten worker's ping was acknowledged");
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fake_acks_pings_and_pushes_directives() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let mut client = client(addr).await;
        let worker = fake.add_worker();

        let (tx, rx) = mpsc::channel(4);
        let mut controls = client
            .heartbeat(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .expect("heartbeat RPC failed")
            .into_inner();

        tx.send(proto::HeartbeatPing {
            worker_id: worker.as_bytes().to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

        let first = controls.message().await.unwrap().unwrap().directive;
        assert!(matches!(
            first,
            Some(proto::heartbeat_control::Directive::Ack(_))
        ));

        eventually(Duration::from_secs(2), "the ping to be recorded", || {
            fake.pings().len() == 1
        })
        .await;

        let cancel = proto::CancelJob {
            job_id: vec![1; 16],
            dispatch_token: 3,
            reason: proto::CancelReason::Revoked as i32,
        };
        fake.push_directive(proto::heartbeat_control::Directive::CancelJob(
            cancel.clone(),
        ))
        .await;

        let pushed = controls.message().await.unwrap().unwrap().directive;
        assert_eq!(
            pushed,
            Some(proto::heartbeat_control::Directive::CancelJob(cancel))
        );
    }
}

/*
tests to write:
- needs_reregister test, ensuring duplicate signals are ignored
- stop_accepting_work and drain test
-
-
*/
