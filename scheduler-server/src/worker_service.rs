use std::collections::HashMap;

use scheduler_core::{job_data_structures::{Job, JobState, now_millis}, proto::worker_service_server::WorkerService};
use scheduler_core::conversion;

use tonic::{Request, Response, Status};
use scheduler_core::proto;
use futures::stream::BoxStream;

use crate::scheduler_state::{RunningJob, RunningPhase, SchedulerState};

pub struct MyWorkerService {
    scheduler_state: SchedulerState, //copy SchedulerState over from MySchedulerService during initialization
    //safe to copy since all fields are Arc wrapped
}

impl MyWorkerService {

}

#[tonic::async_trait]
impl WorkerService for MyWorkerService {
    async fn register(&self, request: Request<proto::Worker>) -> Result<Response<proto::AssignedWorkerId>, Status> {
        Err(Status::unimplemented("register not yet implemented"))
    }

    async fn report_result(&self, request: Request<proto::JobResult>) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("report_result not yet implemented"))
    }

    async fn request_work(&self, request: Request<proto::AssignedWorkerId>) -> Result<Response<proto::RequestWorkResponse>, Status> {
        //check that worker exists and worker has enough space in job pool

        
        let job_requesting_worker_id = request.into_inner().worker_id;

        let mut worker_capabilities: HashMap<String, String> = HashMap::new();

        if let Some(mut worker) = self.scheduler_state.workers.get_mut(&job_requesting_worker_id) {
            worker_capabilities = worker.capabilities.clone();

            if worker.max_concurrent_jobs <= worker.assigned_jobs.len() as u32 { //check to see if the job pool is full
                return Err(Status::resource_exhausted(format!("no space in worker id: {} job pool for more jobs", job_requesting_worker_id)))
            } else {

                if let Ok(mut job_queue_guard) = self.scheduler_state.job_queue.lock() {

                let mut extractor = job_queue_guard.extract_if(.., |job| {
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
        Err(Status::unimplemented("heartbeat not yet implemented"))
    }
    

}

