use std::{collections::HashSet, sync::atomic::Ordering::SeqCst};

use futures::stream::BoxStream;
use scheduler_core::{
    job_data_structures::{Job, JobState, now_millis},
    proto::{self, HeartbeatControl, worker_service_server::WorkerService},
    state_machine::{JobEvent, determine_next_event, transition},
    worker::WorkerId,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::{
    scheduler_state::{
        CompletedJob, CompletedJobOutcome, RunningJob, RunningPhase, SchedulerState,
    },
    worker::{WorkerInfo, WorkerState},
};

pub struct MyWorkerService {
    //Shared with MySchedulerService. Every field inside is `Arc`-wrapped,
    //so both services mutate the same maps and queues.
    pub scheduler_state: SchedulerState,
}

impl MyWorkerService {
    pub fn new(scheduler_state: SchedulerState) -> Self {
        MyWorkerService { scheduler_state }
    }
}

///Transforms raw bytes from proto to uuids.
fn parse_uuid(raw: &[u8], message: &'static str) -> Result<Uuid, Status> {
    Uuid::from_slice(raw).map_err(|_| Status::invalid_argument(message))
}

/// Verifies the executing phase of the Job, ensuring that the worker id is correct and that it is not in
/// the retrying_queue. Any job in the retrying_queue only has ownership via the retrying_queue, so if a worker is still
/// able to access it, it means an error occured in report_result and it is stale.
/// Returns `started_at` so the failure path can rebuild the staging `Job` without re-matching.
fn verify_executing_phase(phase: &RunningPhase, sender: WorkerId) -> Result<u64, Status> {
    match *phase {
        RunningPhase::Executing {
            worker_id,
            started_at,
        } => {
            if worker_id != sender {
                return Err(Status::invalid_argument(
                    "message is stale or inaccurate, worker id does not match sender",
                ));
            }
            Ok(started_at)
        }

        RunningPhase::Retrying => Err(Status::invalid_argument(
            "JobResult is stale/inaccurate, worker should not have access to Job currently in retry_queue",
        )),
    }
}

/// Every terminal outcome produces the same record, differing only in `outcome`.
fn build_completed_job(job: RunningJob, outcome: CompletedJobOutcome) -> CompletedJob {
    CompletedJob {
        id: job.id,
        job_type: job.job_type,
        payload: job.payload,
        priority: job.priority,
        created_at: job.created_at,
        retry_policy: job.retry_policy,
        requirements: job.requirements,
        retry_count: job.retry_count,
        infra_interruptions: job.infra_interruptions,
        completed_at: now_millis(),
        outcome,
        metadata: job.metadata,
    }
}

/// Creates a tonic Response for the case where work is current unavailable in response to request_work from workers
fn no_work_available() -> Response<proto::RequestWorkResponse> {
    Response::new(proto::RequestWorkResponse {
        result: Some(proto::request_work_response::Result::None(
            proto::NoWorkAvailable {},
        )),
    })
}


//state transitions 

impl MyWorkerService {
    ///Clears the slot the finished job is occupying in the worker's assigned_jobs allowing it to request for more jobs if previously full
    fn release_from_worker(&self, worker_uuid: WorkerId, job_uuid: Uuid) -> Result<(), Status> {
        let Some(mut worker) = self.scheduler_state.workers.get_mut(&worker_uuid) else {
            return Err(Status::not_found(format!(
                "worker with id: {worker_uuid} was not found in worker pool"
            )));
        };

        if !worker.assigned_jobs.contains(&job_uuid) {
            tracing::warn!(
                job_id = %job_uuid,
                worker_id = %worker_uuid,
                "job present in running_jobs but missing from worker's assigned_jobs"
            );
            return Err(Status::not_found(
                "job was not found within WorkerInfo.assigned_jobs",
            ));
        }

        worker.assigned_jobs.remove(&job_uuid);
        Ok(())
    }

    /// Used for Success/Cancelled paths, validates sender uuid, removes job from running_jobs, releases job slot from worker, places in completed_jobs.
    fn complete_job(
        &self,
        job_uuid: Uuid,
        worker_uuid: WorkerId,
        outcome: CompletedJobOutcome,
    ) -> Result<(), Status> {
        let Some(running_job) = self.scheduler_state.running_jobs.get(&job_uuid) else {
            return Err(Status::not_found(
                "job uuid did not exist within running_jobs",
            ));
        };

        verify_executing_phase(&running_job.running_phase, worker_uuid)?;

        //drop read lock to access write lock on same shard of dashmap
        drop(running_job);

        let Some((_, running_job)) = self.scheduler_state.running_jobs.remove(&job_uuid) else {
            return Err(Status::internal(
                "None was returned for removal of job in Completed branch during report_result, race condition possibly",
            ));
        };

        self.release_from_worker(worker_uuid, job_uuid)?;

        let job_id = running_job.id;
        self.scheduler_state
            .completed_jobs
            .insert(job_id, build_completed_job(running_job, outcome));

        Ok(())
    }


    /// For job failures reported by the worker, more specifically the job itself failing.
    /// Infrastructure failure is handled separately, via the run_death_detector, so anything here is for job-level failures.
    /// The designation for retrying vs deadlettering is handled by 'transition' and 'determine_next_event' which belongs in scheduler-core/src/transition.rs. 
    fn handle_failure(
        &self,
        job_uuid: Uuid,
        worker_uuid: WorkerId,
        error: u64,
    ) -> Result<(), Status> {
        let Some(mut running_job) = self.scheduler_state.running_jobs.get_mut(&job_uuid) else {
            return Err(Status::not_found(
                "job uuid did not exist within running_jobs",
            ));
        };

        let started_at = verify_executing_phase(&running_job.running_phase, worker_uuid)?;

        self.release_from_worker(worker_uuid, job_uuid)?;

        let mut staging_job = Job {
            id: running_job.id,
            job_type: running_job.job_type.clone(),
            payload: running_job.payload,
            priority: running_job.priority,
            retry_count: running_job.retry_count,
            infra_interruptions: running_job.infra_interruptions,
            created_at: running_job.created_at,
            state: JobState::Running {
                worker_id: worker_uuid,
                started_at,
            },
            retry_policy: running_job.retry_policy.clone(),
            requirements: running_job.requirements.clone(),
            metadata: running_job.metadata.clone(),
        };

        //increments retry_count by applying Fail to Job and passing to state machine
        transition(&mut staging_job, JobEvent::Fail { error })
            .map_err(|e| Status::internal(format!("invalid transition: {e:?}")))?;

        //determines whether to retry or deadletter
        let next = determine_next_event(&staging_job);

        transition(&mut staging_job, next)
            .map_err(|e| Status::internal(format!("invalid transition: {e:?}")))?;

        //deadlettered jobs count as failed, so increment regardless
        self.scheduler_state.total_failed.fetch_add(1, SeqCst);

        match staging_job.state {
            JobState::Retrying { retry_after } => {
                running_job.retry_count = staging_job.retry_count;
                running_job.running_phase = RunningPhase::Retrying;

                {
                    let mut retry_queue_guard = self.scheduler_state.retry_queue.lock();
                    //absolute deadline, not a duration. the monitor sleeps until this Instant.
                    retry_queue_guard.insert((std::time::Instant::now() + retry_after, job_uuid));
                }

                self.scheduler_state.retry_notify.notify_one();
                Ok(())
            }

            JobState::DeadLettered { reason } => {
                //must release before dead_letter() calls remove() on the same key to avoid deadlock
                drop(running_job);
                self.dead_letter(job_uuid, reason)
            }

            _ => Err(Status::internal(
                "a job transition other than retrying or deadlettered should not be possible",
            )),
        }
    }

    /// Move a job out of `running_jobs` into `completed_jobs` as dead-lettered.
    /// Any guard on `job_uuid` must be dropped/released first.
    fn dead_letter(&self, job_uuid: Uuid, reason: String) -> Result<(), Status> {
        let Some((_, running_job)) = self.scheduler_state.running_jobs.remove(&job_uuid) else {
            tracing::warn!(
                job_id = %job_uuid,
                "job disappeared from running_jobs before it could be dead-lettered"
            );
            return Ok(());
        };

        let job_id = running_job.id;
        self.scheduler_state.completed_jobs.insert(
            job_id,
            build_completed_job(running_job, CompletedJobOutcome::DeadLettered { reason }),
        );

        self.scheduler_state.total_dead_lettered.fetch_add(1, SeqCst);
        Ok(())
    }
}


//WorkerService RPCs start here

#[tonic::async_trait]
impl WorkerService for MyWorkerService {
    /// Registers a worker with capabilities defined by the user on the worker machine submitting them, verification 
    /// currently isn't possible since capabilities change per machine, a new uuid is minted by the server per registration so re-registering workers will have new uuids
    async fn register(
        &self,
        request: Request<proto::Worker>,
    ) -> Result<Response<proto::AssignedWorkerId>, Status> {
        let registered_worker = request.into_inner();

        if registered_worker.max_concurrent_jobs == 0 {
            return Err(Status::invalid_argument(
                "worker's max_concurrent_jobs field was set to 0, unusable worker registration rejected",
            ));
        }

        let new_worker_id: WorkerId = Uuid::now_v7();
        let now = std::time::Instant::now();

        //seeding heartbeat deadline immediately after uuid creation
        //if unresponsive, will be detected by run_death_detector
        {
            let mut guard = self.scheduler_state.worker_heartbeat_timer.lock();
            guard.insert((now + self.scheduler_state.heartbeat_deadline, new_worker_id));
        }

        self.scheduler_state.workers.insert(
            new_worker_id,
            WorkerInfo {
                id: new_worker_id,
                hostname: registered_worker.hostname,
                job_types_supported: registered_worker.job_types_supported,
                max_concurrent_jobs: registered_worker.max_concurrent_jobs,
                assigned_jobs: HashSet::new(),
                last_heartbeat_at: now,
                capabilities: registered_worker.capabilities,
                tags: registered_worker.tags,
                state: WorkerState::Active,
            },
        );

        //notify run_death_detector to possibly recalibrate sleeping time
        self.scheduler_state.new_worker_deadline_notify.notify_one();

        //return worker uuid back to registered machine
        Ok(Response::new(proto::AssignedWorkerId {
            worker_id: new_worker_id.as_bytes().to_vec(),
        }))
    }

    async fn report_result(
        &self,
        request: Request<proto::JobResult>,
    ) -> Result<Response<()>, Status> {
        let result = request.into_inner();

        //check uuids of job and worker
        let job_uuid = parse_uuid(
            &result.job_id,
            "invalid job uuid was entered when attempting to report_result",
        )?;
        let worker_uuid = parse_uuid(
            &result.worker_id,
            "invalid worker uuid was entered when attempting to report_result",
        )?;

        //proto3 wraps every message field in Option and every oneof in a second Option
        //peel off here before accessing logic section
        let outcome = result
            .job_outcome
            .ok_or_else(|| Status::invalid_argument("no JobOutcome was sent"))?
            .outcome
            .ok_or_else(|| Status::invalid_argument("no variant present within JobOutcome"))?;

        match outcome {
            proto::job_outcome::Outcome::Success(proto::job_outcome::Success { result }) => {
                self.complete_job(
                    job_uuid,
                    worker_uuid,
                    CompletedJobOutcome::Succeeded { result },
                )?;
                self.scheduler_state.total_completed.fetch_add(1, SeqCst);
            }

            proto::job_outcome::Outcome::Failure(proto::job_outcome::Failure { error }) => {
                self.handle_failure(job_uuid, worker_uuid, error)?;
            }

            proto::job_outcome::Outcome::Cancelled(proto::job_outcome::Cancelled {}) => {
                self.complete_job(
                    job_uuid,
                    worker_uuid,
                    CompletedJobOutcome::Abandoned {
                        reason: "job was canceled".to_string(),
                    },
                )?;
            }
        }

        Ok(Response::new(()))
    }


    /// Called from workers to request work (pull-based dispatch). For each call at most one job is returned.
    /// note: locking sequence is workers write guard, job_queue mutex.
    /// DO NOT CHANGE SEQUENCE UNLESS DEADLOCKS ARE WANTED!!
    async fn request_work(
        &self,
        request: Request<proto::AssignedWorkerId>,
    ) -> Result<Response<proto::RequestWorkResponse>, Status> {
        let worker_uuid = parse_uuid(
            &request.into_inner().worker_id,
            "invalid worker uuid used to request job",
        )?;

        //one guard spans the state check, the capacity check, the extraction and the assignment. 
        //splitting it would let two concurrent RequestWork calls from the same worker both pass a capacity check with one free slot between them.
        let Some(mut worker) = self.scheduler_state.workers.get_mut(&worker_uuid) else {
            return Err(Status::not_found(format!(
                "worker with id: {worker_uuid} was not found in worker pool"
            )));
        };

        match worker.state {
            WorkerState::Active => {}

            //draining allows for workers to finish current jobs while not taking any new jobs
            WorkerState::Draining => return Ok(no_work_available()),

            WorkerState::Dead => {
                return Err(Status::failed_precondition(
                    "work requesting worker is dead",
                ));
            }
        }

        if worker.max_concurrent_jobs <= worker.assigned_jobs.len() as u32 {
            return Err(Status::resource_exhausted(format!(
                "no space in worker id: {worker_uuid} job pool for more jobs"
            )));
        }

        let worker_capabilities = worker.capabilities.clone();
        let worker_job_types = worker.job_types_supported.clone();


        //a job is a match when a job's job_type is a subset of worker's job_types and the job's requirements 
        //is a subset of the worker's capabilities as well. 
        
        //extract_if scans the BTreeSet in ascending order, which is based off highest priority, 
        //then oldest, and finally id, so the first match is the highest-priority job that is runnable with the requesting machine.

        let candidate = {
            let mut job_queue_guard = self.scheduler_state.job_queue.lock();
            job_queue_guard
                .extract_if(.., |job| {
                    worker_job_types.contains(&job.job_type)
                        && job.requirements.iter().all(|(key, value)| {
                            worker_capabilities.get(key) == Some(value)
                        })
                })
                .next()
        };

        let Some(job) = candidate else {
            return Ok(no_work_available());
        };

        let now = now_millis();

        self.scheduler_state.running_jobs.insert(
            job.id,
            RunningJob {
                id: job.id,
                job_type: job.job_type.clone(),
                payload: job.payload,
                priority: job.priority,
                created_at: job.created_at,
                retry_count: job.retry_count,
                infra_interruptions: job.infra_interruptions,
                running_phase: RunningPhase::Executing {
                    worker_id: worker_uuid,
                    started_at: now,
                },
                requirements: job.requirements.clone(),
                metadata: job.metadata.clone(),
                retry_policy: job.retry_policy.clone(),
            },
        );

        worker.assigned_jobs.insert(job.id);

        let dispatched = Job {
            id: job.id,
            job_type: job.job_type,
            payload: job.payload,
            priority: job.priority,
            retry_count: job.retry_count,
            infra_interruptions: job.infra_interruptions,
            created_at: job.created_at,
            state: JobState::Running {
                worker_id: worker_uuid,
                started_at: now,
            },
            retry_policy: job.retry_policy,
            requirements: job.requirements,
            metadata: job.metadata,
        };

        let proto_job =
            proto::Job::try_from(dispatched).map_err(|_| Status::internal("job was invalid"))?;

        Ok(Response::new(proto::RequestWorkResponse {
            result: Some(proto::request_work_response::Result::Job(proto_job)),
        }))
    }

    type HeartbeatStream = BoxStream<'static, Result<proto::HeartbeatControl, Status>>;

    /// Bidirectional liveness stream. The worker pushes `HeartbeatPing`s and the scheduler refreshes its TTL
    /// and pushes back an `Ack`. This is also the scheduler's only channel for pushing directives
    /// to a worker, which is what `cancel_job` will eventually be transmitted by.
    /// 
    /// TODO: cancel_job implementation and new oneof variant for cancel_job in proto file
    async fn heartbeat(
        &self,
        request: Request<tonic::Streaming<proto::HeartbeatPing>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let mut pings = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(16);

        
        let workers = self.scheduler_state.workers.clone();
        let heartbeat_timer = self.scheduler_state.worker_heartbeat_timer.clone();
        let deadline_notify = self.scheduler_state.new_worker_deadline_notify.clone();
        let heartbeat_deadline = self.scheduler_state.heartbeat_deadline;

        tokio::spawn(async move {
            loop {
                let ping = match pings.message().await {
                    Ok(Some(ping)) => ping,
                    Ok(None) => break, // worker closed its send half cleanly
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                };

                let worker_uuid = match Uuid::from_slice(&ping.worker_id) {
                    Ok(uuid) => uuid,
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("invalid worker uuid: {e}"))))
                            .await;
                        break;
                    }
                };

                //using monotonic clocks, not wall clocks due to drift/jumps/unreliability in time across separate machines
                let now = std::time::Instant::now();

                //key is (deadline, worker_id), so to renew TTL, the old pair must be removed and replaced, 
                //due to no in place updates.
                {
                    let mut guard = heartbeat_timer.lock();

                    let old_deadline = guard
                        .iter()
                        .find(|(_, id)| *id == worker_uuid)
                        .map(|(instant, _)| *instant);

                    if let Some(old_deadline) = old_deadline {
                        guard.remove(&(old_deadline, worker_uuid));
                    }

                    guard.insert((now + heartbeat_deadline, worker_uuid));
                }

                //only after the guard is dropped: notifying while holding it makes the woken
                //task immediately block on the lock we still own.
                deadline_notify.notify_one();

                if let Some(mut worker) = workers.get_mut(&worker_uuid) {
                    worker.last_heartbeat_at = now;
                } else {
                    let _ = tx
                        .send(Err(Status::internal(
                            "invalid worker uuid present in workers",
                        )))
                        .await;
                    break;
                }

                let ack = HeartbeatControl {
                    directive: Some(proto::heartbeat_control::Directive::Ack(proto::Ack {})),
                };

                //Err here means the worker's receive half is gone, nothing left to serve.
                if tx.send(Ok(ack)).await.is_err() {
                    break;
                }
            }
        });

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream)))
    }
}


//tests start here

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler_core::job_data_structures::RetryPolicy;
    use std::collections::HashMap;
    use tonic::transport::Channel;

    type SchedulerClient = proto::scheduler_service_client::SchedulerServiceClient<Channel>;
    type WorkerClient = proto::worker_service_client::WorkerServiceClient<Channel>;

    /// Everything a report_result test needs after setup has run.
    struct Fixture {
        worker_client: WorkerClient,
        /// Clone of the server's `SchedulerState` — the same Arcs, so assertions see live state.
        state: SchedulerState,
        /// Raw proto bytes, since that is what the RPCs take.
        worker_id: Vec<u8>,
        job: proto::Job,
    }

    /// Bind both services to an ephemeral port and serve them on a background task.
    ///
    /// Note: the background tasks (retry monitor, death detector) are deliberately *not*
    /// spawned here. These tests assert on the state left behind by an RPC, and a running
    /// retry monitor would drain the retry queue out from under them.
    async fn spawn_server() -> (SchedulerClient, WorkerClient, SchedulerState) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tonic::transport::server::TcpIncoming::from(listener);

        let scheduler_service = crate::service::MySchedulerService::new();
        let state = scheduler_service.scheduler_state.clone();
        let worker_service = MyWorkerService::new(state.clone());

        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    proto::scheduler_service_server::SchedulerServiceServer::new(
                        scheduler_service,
                    ),
                )
                .add_service(proto::worker_service_server::WorkerServiceServer::new(
                    worker_service,
                ))
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
            SchedulerClient::new(channel.clone()),
            WorkerClient::new(channel),
            state,
        )
    }

    /// Walk the full happy path up to the point of reporting: submit one job, register a
    /// worker that can run it, and dispatch it via RequestWork.
    async fn submit_register_dispatch(retry_policy: RetryPolicy) -> Fixture {
        let (mut scheduler_client, mut worker_client, state) = spawn_server().await;

        let job = Job {
            id: Uuid::now_v7(),
            job_type: "test".to_string(),
            payload: 0,
            priority: 1,
            retry_count: 0,
            infra_interruptions: 0,
            created_at: 0,
            state: JobState::Queued,
            retry_policy,
            requirements: HashMap::new(),
            metadata: HashMap::new(),
        };

        let submitted = scheduler_client
            .submit_job(Request::new(proto::Job::try_from(job).unwrap()))
            .await
            .expect("submit_job failed")
            .into_inner();

        let submitted_id = Uuid::from_slice(&submitted.id).expect("submit_job returned a bad uuid");

        {
            let queued = state.job_queue.lock();
            assert!(
                queued.iter().any(|job| job.id == submitted_id),
                "submitted job never landed in job_queue"
            );
        }

        let worker_id = worker_client
            .register(Request::new(proto::Worker {
                tags: HashMap::new(),
                capabilities: HashMap::new(),
                hostname: "test".to_string(),
                job_types_supported: vec!["test".to_string()],
                max_concurrent_jobs: 3,
            }))
            .await
            .expect("registration of test_worker failed")
            .into_inner()
            .worker_id;

        let response = worker_client
            .request_work(Request::new(proto::AssignedWorkerId {
                worker_id: worker_id.clone(),
            }))
            .await
            .expect("requesting work failed")
            .into_inner();

        let job = match response.result {
            Some(proto::request_work_response::Result::Job(job)) => job,
            Some(proto::request_work_response::Result::None(_)) => {
                panic!("no work was available somehow")
            }
            None => panic!("None was sent over in response to request work"),
        };

        Fixture {
            worker_client,
            state,
            worker_id,
            job,
        }
    }

    fn job_result(
        f: &Fixture,
        outcome: proto::job_outcome::Outcome,
    ) -> Request<proto::JobResult> {
        Request::new(proto::JobResult {
            job_id: f.job.id.clone(),
            worker_id: f.worker_id.clone(),
            job_outcome: Some(proto::JobOutcome {
                outcome: Some(outcome),
            }),
        })
    }

    /// Covers submit -> register -> dispatch -> report success, i.e. the whole green path
    /// including capability matching and slot release.
    #[tokio::test]
    async fn report_result_success() {
        let mut f = submit_register_dispatch(RetryPolicy::NoRetry).await;

        let report = job_result(
            &f,
            proto::job_outcome::Outcome::Success(proto::job_outcome::Success { result: 1 }),
        );
        f.worker_client
            .report_result(report)
            .await
            .expect("report result failed");

        assert!(f.state.job_queue.lock().is_empty());

        let job_uuid = Uuid::from_slice(&f.job.id).expect("uuid failed to form");

        {
            let completed = f
                .state
                .completed_jobs
                .get(&job_uuid)
                .expect("id did not exist in completed_jobs");

            assert_eq!(completed.job_type, "test".to_string());
            assert_eq!(
                completed.outcome,
                CompletedJobOutcome::Succeeded { result: 1 }
            );
        }

        assert_eq!(f.state.total_completed.load(SeqCst), 1);
        assert!(!f.state.running_jobs.contains_key(&job_uuid));

        if let Some(worker) = f.state.workers.get(&Uuid::from_slice(&f.worker_id).unwrap()) {
            assert!(worker.assigned_jobs.is_empty());
        }
    }

    #[tokio::test]
    async fn report_result_failure_to_retry() {
        let mut f = submit_register_dispatch(RetryPolicy::FixedDelay {
            delay_ms: 100,
            max_attempts: 3,
        })
        .await;

        let report = job_result(
            &f,
            proto::job_outcome::Outcome::Failure(proto::job_outcome::Failure { error: 1 }),
        );
        f.worker_client
            .report_result(report)
            .await
            .expect("report result failed");

        let job_uuid = Uuid::from_slice(&f.job.id).expect("uuid failed to form");

        // NOTE (preserved from the original): this is an `if let` with no `else`. If the job
        // is NOT in running_jobs, every assertion below is skipped and the test passes without
        // checking anything. See my notes — one-line fix.
        if let Some(job_ref) = f.state.running_jobs.get(&job_uuid) {
            assert_eq!(job_ref.retry_count, 1);
            assert_eq!(job_ref.running_phase, RunningPhase::Retrying);
            assert_eq!(job_ref.job_type, "test".to_string());
            assert_eq!(f.state.total_failed.load(SeqCst), 1);

            if let Some(worker) = f.state.workers.get(&Uuid::from_slice(&f.worker_id).unwrap()) {
                assert!(worker.assigned_jobs.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn report_result_failure_to_dlq() {
        // NoRetry means determine_next_event dead-letters on the first failure.
        let mut f = submit_register_dispatch(RetryPolicy::NoRetry).await;

        let report = job_result(
            &f,
            proto::job_outcome::Outcome::Failure(proto::job_outcome::Failure { error: 1 }),
        );
        f.worker_client
            .report_result(report)
            .await
            .expect("report result failed");

        let job_uuid = Uuid::from_slice(&f.job.id).expect("uuid failed to form");

        // NOTE (preserved from the original): same vacuous-pass hazard as above.
        if let Some(job_ref) = f.state.completed_jobs.get(&job_uuid) {
            assert_eq!(job_ref.retry_count, 0);
            assert_eq!(job_ref.job_type, "test".to_string());
            assert_eq!(f.state.total_dead_lettered.load(SeqCst), 1);
            assert_eq!(
                job_ref.outcome,
                CompletedJobOutcome::DeadLettered {
                    reason: "retries exhausted".to_string()
                }
            );

            if let Some(worker) = f.state.workers.get(&Uuid::from_slice(&f.worker_id).unwrap()) {
                assert!(worker.assigned_jobs.is_empty());
            }
        }
    }
}