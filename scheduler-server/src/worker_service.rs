use core::time;
use std::{collections::{HashMap, HashSet}, sync::atomic::Ordering::SeqCst};

use scheduler_core::{job_data_structures::{Job, JobState, now_millis}, proto::{HeartbeatControl, worker_service_server::WorkerService}, state_machine::{JobEvent, determine_next_event, transition}, worker::WorkerId};

use tonic::{Request, Response, Status};
use scheduler_core::proto;
use futures::stream::BoxStream;
use uuid::Uuid;

use crate::{scheduler_state::{CompletedJob, CompletedJobOutcome, RunningJob, RunningPhase, SchedulerState}, worker::{WorkerInfo, WorkerState}};

const NEXT_HEARTBEAT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(15);

pub struct MyWorkerService {
    scheduler_state: SchedulerState, //copy SchedulerState over from MySchedulerService during initialization
    //safe to copy since all fields are Arc wrapped
}

impl MyWorkerService {
    
}



#[tonic::async_trait]
impl WorkerService for MyWorkerService {
    async fn register(&self, request: Request<proto::Worker>) -> Result<Response<proto::AssignedWorkerId>, Status> {
        
        //receive Request<proto::Worker> which holds capabilities and tags (extra metadata)
        /*
        take inner() and create new WorkerInfo struct 

        generate the unique id of the worker

        new entry within self.workers


        currnetly, there are no checks to establish that the capabilities of a worker are valid, is it even possible without 
        a bank of "valid" capabilities for a worker?

        actually, this is impossible since this always changes per machine and for each machine, the capabilities can change
         */

    
        
        let new_worker_id: WorkerId = uuid::Uuid::now_v7(); //for now a placeholder until i figure out how to generate unique 64 bit ids

        let registered_worker = request.into_inner();

        if registered_worker.max_concurrent_jobs == 0 {
            return Err(Status::invalid_argument("worker's max_concurrent_jobs field was set to 0, unusable worker registration rejected"))
        }
    
        let now = std::time::Instant::now();

        
        if let Ok(mut guard) = self.scheduler_state.worker_heartbeat_timer.lock() {
            guard.insert((now+NEXT_HEARTBEAT_DEADLINE, new_worker_id));
        } else {
            return Err(Status::internal("worker_heartbeat_timer mutex lock poisoned"));
        }

        let new_worker = WorkerInfo {
            id: new_worker_id,
            hostname: registered_worker.hostname,
            job_types_supported: registered_worker.job_types_supported,
            max_concurrent_jobs: registered_worker.max_concurrent_jobs,
            assigned_jobs: HashSet::new(),
            last_heartbeat_at: now,
            capabilities: registered_worker.capabilities,
            tags: registered_worker.tags,
            state: WorkerState::Active,
        };
         
        self.scheduler_state.workers.insert(new_worker_id, new_worker);


        return Ok(Response::new(proto::AssignedWorkerId { worker_id: new_worker_id.as_bytes().to_vec() }));

    }

    async fn report_result(&self, request: Request<proto::JobResult>) -> Result<Response<()>, Status> {

        //receive a proto::JobResult, we get job_id, worker_id, and job_outcome

        let result = request.into_inner();

        let job_uuid = match uuid::Uuid::from_slice(&result.job_id) {
            Ok(uuid) => uuid,
            Err(e) => {
                return Err(Status::invalid_argument("invalid job uuid was entered when attempting to report_result"));
            }
        };

        let worker_uuid = match uuid::Uuid::from_slice(&result.worker_id) {
            Ok(uuid) => uuid,
            Err(e) => {
                return Err(Status::invalid_argument("invalid worker uuid was entered when attempting to report_result"));
            }
        };

        match result.job_outcome {
            Some(job_outcome) => {
                match job_outcome.outcome {
                    Some(outcome) => { //unlayering the proto layers
                        match outcome {
                            //regardless of outcome, transition the RunningJob into a CompletedJob
                            //pop out the RunningJob and create a new CompletedJob
                            //also pop out of worker's job pool

                            proto::job_outcome::Outcome::Success(proto::job_outcome::Success { result }) => {
                                
                                if let Some(running_job) = self.scheduler_state.running_jobs.get(&job_uuid) {
                                    //check to see if sender is actually correct worker
                                    //and check to see if job is in executing phase or retrying phase
                                    match running_job.running_phase {
                                        RunningPhase::Executing { worker_id, started_at } => {
                                            if worker_id != worker_uuid {
                                                return Err(Status::internal("message is stale or inaccurate, worker id does not match sender"));
                                            }
                                        },

                                        RunningPhase::Retrying => {
                                            return Err(Status::internal("JobResult is stale/inaccurate, worker should not have access to Job currently in retry_queue"));
                                        }
                                    }
                                } else {
                                    //doesnt erxist within running_job
                                    return Err(Status::internal("job uuid did not exist within running_jobs"));
                                }

                                //now checks have passed for staleness of messages and existence within running_jobs, safe to remove
                                
                                if let Some((running_job_uuid, running_job)) = self.scheduler_state.running_jobs.remove(&job_uuid) {

                                    //pop out of worker job pool

                                    if let Some(mut worker) = self.scheduler_state.workers.get_mut(&worker_uuid) {

                                        if !worker.assigned_jobs.contains(&running_job_uuid) {
                                            tracing::warn!(
                                                job_id = %running_job_uuid,
                                                worker_id = %worker_uuid,
                                                "job present in running_jobs but missing from worker's assigned_jobs"
                                            );
                                            return Err(Status::not_found("job was not found within WorkerInfo.assigned_jobs"))
                                        }
                                        worker.assigned_jobs.remove(&running_job_uuid);
                                           

                                    } else {
                                        return Err(Status::not_found(format!("worker with id: {} was not found in worker pool", worker_uuid)));
                                    }


                                    let completed_job = CompletedJob {
                                        id: running_job_uuid,
                                        job_type: running_job.job_type,
                                        payload: running_job.payload,
                                        priority: running_job.priority,
                                        created_at: running_job.created_at,
                                        retry_policy: running_job.retry_policy,
                                        requirements: running_job.requirements,
                                        retry_count: running_job.retry_count,
                                        infra_interruptions: running_job.infra_interruptions,
                                        completed_at: now_millis(),
                                        outcome: CompletedJobOutcome::Succeeded { result },
                                        metadata: running_job.metadata,
                                    };

                                    self.scheduler_state.completed_jobs.insert(running_job_uuid, completed_job);

                                    self.scheduler_state.total_completed.fetch_add(1, SeqCst);

                                    return Ok(Response::new(()));

                                }
                                
                            },

                            proto::job_outcome::Outcome::Failure(proto::job_outcome::Failure { error}) => {

                                //how do i tell if it failed becasue of infrastructure or because of job itself?
                                //if the infrastructure breaks, i can automatically detect it because of the loss in heartbeats
                                //therefore all jobs within that worker will be flagged as an infrastructure interruption once their heartbeat leases expire

                                //so i just need to account for the actual job failure here since infrastruture failures wont be reported at all

                                if !self.scheduler_state.running_jobs.contains_key(&job_uuid) {
                                    return Err(Status::internal("job uuid did not exist within running_jobs"));
                                }

                                
                                if let Some(mut running_job) = self.scheduler_state.running_jobs.get_mut(&job_uuid) {

                                    //check to see if sender is actually correct worker
                                    //and check to see if job is in executing phase or retrying phase
                                    match running_job.running_phase {
                                        RunningPhase::Executing { worker_id, started_at } => {
                                            if worker_id != worker_uuid {
                                                return Err(Status::internal("message is stale or inaccurate, worker id does not match sender"));
                                            }
                                        },

                                        RunningPhase::Retrying => {
                                            return Err(Status::internal("JobResult is stale/inaccurate, worker should not have access to Job currently in retry_queue"));
                                        }
                                    }

                                    //pop out of worker job pool

                                    if let Some(mut worker) = self.scheduler_state.workers.get_mut(&worker_uuid) {

                                        if !worker.assigned_jobs.contains(&job_uuid) {
                                            tracing::warn!(
                                                job_id = %&running_job.id,
                                                worker_id = %worker_uuid,
                                                "job present in running_jobs but missing from worker's assigned_jobs"
                                            );
                                            return Err(Status::not_found("job was not found within WorkerInfo.assigned_jobs"))
                                        }
                                        worker.assigned_jobs.remove(&job_uuid);
                                           

                                    } else {
                                        return Err(Status::not_found(format!("worker with id: {} was not found in worker pool", worker_uuid)));
                                    }

                                    //for the failure case, create a temporary job instance then use transition() to apply an increment
                                    //then use determine_next_event on the new retry_count, re-enter into the transition method again and match from there?

                                    let mut temp_job = Job {
                                        id: running_job.id,
                                        job_type: running_job.job_type.clone(),
                                        payload: running_job.payload,
                                        priority: running_job.priority,
                                        retry_count: running_job.retry_count,
                                        infra_interruptions: running_job.infra_interruptions,
                                        created_at: running_job.created_at,
                                        state: JobState::Failed { error },
                                        retry_policy: running_job.retry_policy.clone(),
                                        requirements: running_job.requirements.clone(),
                                        metadata: running_job.metadata.clone(),
                                    };

                                    let _ = transition(&mut temp_job, JobEvent::Fail { error }); //increment retry_count by 1

                                    let next = determine_next_event(&temp_job);

                                    let _ = transition(&mut temp_job, next);

                                    match temp_job.state {
                                        JobState::Retrying { retry_after } => {
                                            if let Some(timeout) = running_job.retry_policy.next_delay(running_job.retry_count) {
                                                //increment retry_count
                                                running_job.retry_count += 1;
                                                running_job.running_phase = RunningPhase::Retrying;
                                                
                                                if let Ok(mut retry_queue_guard) = self.scheduler_state.retry_queue.lock() {
                                                    retry_queue_guard.insert((std::time::Instant::now()+timeout, job_uuid));
                                                    //insert job into retry_queue with calculated timeout with jitter

                                                    //increment total_failed

                                                    self.scheduler_state.total_failed.fetch_add(1, SeqCst);
                                                } else {
                                                    return Err(Status::internal("retry_queue mutex lock was poisoned"));
                                                }


                                            }
                                        },

                                        JobState::DeadLettered { reason } => {

                                            if let Some((_, job_to_be_deadlettered)) = self.scheduler_state.running_jobs.remove(&job_uuid) {
                                                let deadlettered_job = CompletedJob {
                                                    id: job_to_be_deadlettered.id,
                                                    job_type: job_to_be_deadlettered.job_type,
                                                    payload: job_to_be_deadlettered.payload,
                                                    priority: job_to_be_deadlettered.priority,
                                                    created_at: job_to_be_deadlettered.created_at,
                                                    retry_policy: job_to_be_deadlettered.retry_policy,
                                                    requirements: job_to_be_deadlettered.requirements,
                                                    retry_count: job_to_be_deadlettered.retry_count,
                                                    infra_interruptions: job_to_be_deadlettered.infra_interruptions,
                                                    completed_at: now_millis(),
                                                    outcome: CompletedJobOutcome::DeadLettered { reason: "available retries exhausted".to_string() },
                                                    metadata: job_to_be_deadlettered.metadata,
                                                };

                                                //store in completed_jobs with deadlettered status

                                                self.scheduler_state.completed_jobs.insert(job_to_be_deadlettered.id, deadlettered_job);
                                                
                                                //increment atomic
                                                self.scheduler_state.total_dead_lettered.fetch_add(1, SeqCst);
                                            }
                                        },

                                        _ => {
                                            return Err(Status::internal("a job transition other than retrying or deadlettered should not be possible"));

                                        }
                                    }

                                    return Ok(Response::new(()));

                                }


                                //before incrementations, compare against RetryPolicy using next_delay
                                //if next_delay returns None, then deadletter it, if not then retry it

                                
                                //timeout calculation using next_delay

                                //insert into retry_queue

                                //change the running_phase to retrying
                            },

                            proto::job_outcome::Outcome::Cancelled(proto::job_outcome::Cancelled {}) => {
                                
                                //same flow as Outcome::Success variant


                                if !self.scheduler_state.running_jobs.contains_key(&job_uuid) {
                                    return Err(Status::internal("job uuid did not exist within running_jobs"));
                                }

                                if let Some(running_job) = self.scheduler_state.running_jobs.get(&job_uuid) {
                                    //check to see if sender is actually correct worker
                                    //and check to see if job is in executing phase or retrying phase
                                    match running_job.running_phase {
                                        RunningPhase::Executing { worker_id, started_at } => {
                                            if worker_id != worker_uuid {
                                                return Err(Status::internal("message is stale or inaccurate, worker id does not match sender"));
                                            }
                                        },

                                        RunningPhase::Retrying => {
                                            return Err(Status::internal("JobResult is stale/inaccurate, worker should not have access to Job currently in retry_queue"));
                                        }
                                    }
                                } else {
                                    //doesnt erxist within running_job
                                    return Err(Status::internal("job uuid did not exist within running_jobs"));
                                }

                                
                                if let Some((running_job_uuid, running_job)) = self.scheduler_state.running_jobs.remove(&job_uuid) {

                                    //pop out of worker job pool

                                    if let Some(mut worker) = self.scheduler_state.workers.get_mut(&worker_uuid) {

                                        if !worker.assigned_jobs.contains(&running_job_uuid) {
                                            tracing::warn!(
                                                job_id = %running_job_uuid,
                                                worker_id = %worker_uuid,
                                                "job present in running_jobs but missing from worker's assigned_jobs"
                                            );
                                            return Err(Status::not_found("job was not found within WorkerInfo.assigned_jobs"))
                                        }
                                        worker.assigned_jobs.remove(&running_job_uuid);
                                           

                                    } else {
                                        return Err(Status::not_found(format!("worker with id: {} was not found in worker pool", worker_uuid)));
                                    }


                                    let completed_job = CompletedJob {
                                        id: running_job_uuid,
                                        job_type: running_job.job_type,
                                        payload: running_job.payload,
                                        priority: running_job.priority,
                                        created_at: running_job.created_at,
                                        retry_policy: running_job.retry_policy,
                                        requirements: running_job.requirements,
                                        retry_count: running_job.retry_count,
                                        infra_interruptions: running_job.infra_interruptions,
                                        completed_at: now_millis(),
                                        outcome: CompletedJobOutcome::Abandoned { reason: "job was canceled".to_string() },
                                        metadata: running_job.metadata,
                                    };

                                    self.scheduler_state.completed_jobs.insert(running_job_uuid, completed_job);

                                    self.scheduler_state.total_failed.fetch_add(1, SeqCst);

                                    return Ok(Response::new(()));

                                }


                            }
                        }
                    },

                    None => {
                        return Err(Status::invalid_argument("no variant present within JobOutcome"));
                    }
                }
            },

            None => {
                return Err(Status::invalid_argument("no JobOutcome was sent"));
            }
        }



        //free job_id from worker with worker_id working job pool

        /*
        
        if job_outcome is Success { result }, transition RunningJob to CompletedJob, insert with uuid, CompletedJob kv pair
        increment total_completed in SchedulerState

        if job_outcome is Failure { error }, increment job's retry_count/infra_interruptions, how do i tell which caused it??

        also check against max_cap as well before i do the incrementation, to possibly drop and increment total_dead_lettered
        
         */

        Err(Status::unimplemented("report_result not yet implemented"))
    }

    async fn request_work(&self, request: Request<proto::AssignedWorkerId>) -> Result<Response<proto::RequestWorkResponse>, Status> {
        //check that worker exists and worker has enough space in job pool

        
        let job_requesting_worker_id = request.into_inner().worker_id;

        let job_requesting_worker_id = match Uuid::from_slice(&job_requesting_worker_id) {
            Ok(uuid) => {
                uuid
            },
            Err(e) => {
                return Err(Status::invalid_argument("invalid worker uuid used to request job"))
            }
        };

        let mut worker_capabilities: HashMap<String, String> = HashMap::new();
        let mut worker_job_types: Vec<String>;

        if let Some(mut worker) = self.scheduler_state.workers.get_mut(&job_requesting_worker_id) {
            worker_capabilities = worker.capabilities.clone();
            worker_job_types = worker.job_types_supported.clone();

            if worker.max_concurrent_jobs <= worker.assigned_jobs.len() as u32 { //check to see if the job pool is full
                return Err(Status::resource_exhausted(format!("no space in worker id: {} job pool for more jobs", job_requesting_worker_id)))
            } else {

                if let Ok(mut job_queue_guard) = self.scheduler_state.job_queue.lock() {

                let mut extractor = job_queue_guard.extract_if(.., |job| {
                    worker_job_types.contains(&job.job_type) &&
                    job.requirements.iter().all(|(requirements_key, requirements_value)| worker_capabilities.get(requirements_key) == Some(requirements_value))
                }); //iterates over requirements to check that the requirements hashmap is a subset of capabilities hashmap
                //if it matches, then the QueuedJob is unlinked from the btreeset


                if let Some(job) = extractor.next() { //executes the extractor, if there is a job whose requirements match the worker's capabilities

                    //create a RunningJob and pass the values from the QueuedJob into it

                    let now = now_millis();

                    let new_running_job = RunningJob {
                        id: job.id,
                        job_type: job.job_type.clone(),
                        payload: job.payload,
                        priority: job.priority,
                        created_at: job.created_at,
                        retry_count: job.retry_count,
                        running_phase: RunningPhase::Executing { 
                            worker_id: job_requesting_worker_id, 
                            started_at: now, 
                        },
                        infra_interruptions: job.infra_interruptions,
                        requirements: job.requirements.clone(),
                        metadata: job.metadata.clone(),
                        retry_policy: job.retry_policy.clone(),
                    };

                    self.scheduler_state.running_jobs.insert(job.id, new_running_job);
                    
                    worker.assigned_jobs.insert(job.id);

                    //insert new job into worker's job hashset and incremement max_concurrent_job counter by 1
                    //check should have been done before the mutex for job_queue was locked

                    let proto_job = Job {
                        id: job.id, 
                        job_type: job.job_type,
                        payload: job.payload,
                        priority: job.priority,
                        retry_count: job.retry_count,
                        infra_interruptions: job.infra_interruptions,
                        created_at: job.created_at,
                        state: JobState::Running { worker_id: job_requesting_worker_id, started_at: now },
                        retry_policy: job.retry_policy,
                        requirements: job.requirements,
                        metadata: job.metadata,
                    };

                    if let Ok(proto_job) = proto::Job::try_from(proto_job) {
                        return Ok(Response::new(proto::RequestWorkResponse {
                            result: Some(proto::request_work_response::Result::Job(proto_job))
                        }))
                    } else {
                        return Err(Status::internal("job was invalid"));
                    }



                    //insert new RunningJob into running_job dashmap

                    /*
                    think about when noworkavailable would be sent back --> when there are no jobs in queue at all or when there are no matching jobs?

                
                    
                    now modify SchedulerState.workers to add in new job to WorkerInfo.max_concurrent_jobs
                    and WorkerInfo.assigned_jobs

                    create Job, then use try_from to turn into proto::Job in converison.rs
                    return the proto::Job
                        */
                    
                } else {
                    return Ok(Response::new(proto::RequestWorkResponse {
                        result: Some(proto::request_work_response::Result::None(proto::NoWorkAvailable {}))
                    }))
                }

                

            } else {
                return Err(Status::internal("scheduler state's job_queue lock was poisoned"));
            }
            }
        } else { //worker doesn't exist
            return Err(Status::not_found(format!("worker with id: {} was not found in worker pool", job_requesting_worker_id)))
        }

    

        //note: lock acquisition must be in this order in the future to avoid deadlocks
        //acquire write lock for workers dashmap, then job_queue mutex

        //also dont try to reacquire the same lock again, dont acquire read lock adn then try to do write lock 

    }
    
    type HeartbeatStream = BoxStream<'static, Result<proto::HeartbeatControl, Status>>;

    async fn heartbeat(&self, request: Request<tonic::Streaming<proto::HeartbeatPing>>) -> Result<Response<Self::HeartbeatStream>, Status> {
        
        let mut incoming_heartbeat_ping_stream = request.into_inner();

        let (tx, rx) = tokio::sync::mpsc::channel(16);

        let workers = self.scheduler_state.workers.clone(); //arc clone
        let heartbeat_timer_lock = self.scheduler_state.worker_heartbeat_timer.clone();

        //one spawned task that continously loops in the background for heartbeats
        //poll for the heartbeats, i can tell what is happening on the channel based on the returned values
        
        //on main function thread, pass the receiver stream to the caller of the RPC 

        tokio::spawn(async move {
            loop {
                match incoming_heartbeat_ping_stream.message().await {
                    Ok(Some(ping)) => {
                        //received a ping successfully, so update worker_heartbeat_timer and WorkerInfo.last_heartbeat_at
                        //acquire worker_heartbeat_timer lock first

                        let worker_uuid = match uuid::Uuid::from_slice(&ping.worker_id) {
                            Ok(uuid) => uuid,
                            Err(e) => {
                                let _ = tx.send(Err(Status::internal(format!("invalid worker uuid: {e}")))).await;
                                break;
                            }
                        };

                        let now = std::time::Instant::now();
                        
                        if let Ok(mut guard) = heartbeat_timer_lock.lock() {
                            let worker_timer = guard
                                .iter()
                                .find(|(_, uuid)| *uuid == worker_uuid)
                                .map(|(instant, _)| *instant);

                            if let Some(old_instant) = worker_timer {
                                guard.remove(&(old_instant, worker_uuid));
                            } //find and replace old Instant with new one at now + 15s

                            guard.insert((now + NEXT_HEARTBEAT_DEADLINE, worker_uuid));

                        } else {
                            let _ = tx.send(Err(Status::internal("worker_heartbeat_timer mutex was poisoned"))).await;
                            break;
                        }
                        
                        //now modify the workers to update last_heartbeat_at
                        if let Some(mut workers_mutref) = workers.get_mut(&worker_uuid) {
                            workers_mutref.last_heartbeat_at = now;
                        } else {
                            let _ = tx.send(Err(Status::internal("invalid worker uuid present in workers"))).await;
                            break;
                        }

                        


                        let ack = HeartbeatControl {
                            directive: Some(proto::heartbeat_control::Directive::Ack(proto::Ack {}))
                        }; //construct ack 

                        if tx.send(Ok(ack)).await.is_err() {
                            //if the client/worker's receiving end is gone
                            break;
                        }

                    },

                    Ok(None) => {
                        //stream closed by worker
                        break;
                    },

                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                    

                }
            }
        });

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        return Ok(Response::new(Box::pin(output_stream)));
        
        //return back stream with HeartbeatStream trait to caller

    }
    

}

