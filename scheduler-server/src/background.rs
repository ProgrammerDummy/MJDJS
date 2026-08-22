//for the functions that run asynchronously in the background, like for observing the retry_queue and popping off when timers expire
//also for heartbeat detection function as well

use scheduler_core::job_data_structures::{Job, JobState, now_millis};
use scheduler_core::worker::WorkerId;
use scheduler_core::state_machine::{JobEvent, determine_reclaim_event, transition};
use crate::scheduler_state::{CompletedJob, CompletedJobOutcome, QueuedJob, RunningJob, RunningPhase, SchedulerState};
use crate::worker::WorkerState;
use std::time::Instant;
use std::collections::{BTreeSet, HashSet};
use std::sync::{Arc};

use parking_lot::Mutex;





//spawn these in separate tokio threads
//do this during initialization of services

pub async fn run_retry_queue_monitor(state: SchedulerState) {

    loop {

        match retry_deadline_check(state.retry_queue.clone()).await {
            DeadlineResult::DeadlineExpired(job_uuid) => {
                let recycled_job = state.running_jobs.remove(&job_uuid); //pop it out of running_jobs
                
                let Some((_, recycled_job)) = recycled_job else {
                    tracing::warn!(
                        job_id = %job_uuid,
                        "job present in retry_queue but not in running_jobs"
                    );
                    continue;
                };


                let new_queued_job = QueuedJob {
                    id: job_uuid,
                    job_type: recycled_job.job_type,
                    payload: recycled_job.payload,
                    priority: recycled_job.priority,
                    created_at: recycled_job.created_at,
                    retry_count: recycled_job.retry_count,
                    infra_interruptions: recycled_job.infra_interruptions,
                    retry_policy: recycled_job.retry_policy,
                    requirements: recycled_job.requirements,
                    metadata: recycled_job.metadata,
                };

                {
                let mut job_queue_guard = state.job_queue.lock();
                    job_queue_guard.insert(new_queued_job);
                }
            },

            DeadlineResult::Waiting(deadline) => {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline.into()) => {},
                    _ = state.retry_notify.notified() => {},
                }
            },

            DeadlineResult::EmptyQueue => {
                state.retry_notify.notified().await;
            },

        }

    }
}

pub enum DeadlineResult {
    DeadlineExpired(uuid::Uuid),
    Waiting(std::time::Instant),
    EmptyQueue,
}

pub async fn retry_deadline_check(retry_queue: Arc<Mutex<BTreeSet<(Instant, uuid::Uuid)>>>) -> DeadlineResult {
    
    let now = Instant::now();
    {
        let mut retry_queue_guard = retry_queue.lock();
        if let Some(&(timeout, job_uuid)) = retry_queue_guard.first() {
            if timeout <= now {
                retry_queue_guard.pop_first();
                return DeadlineResult::DeadlineExpired(job_uuid);
            } else {
                return DeadlineResult::Waiting(timeout);
            }
        } else {
            return DeadlineResult::EmptyQueue;
        }
    } 
}

//notify_one should be called after inserting into retry_queue everytime

pub async fn run_death_detector(state: SchedulerState) {
    loop {
        //possible cases?
        /*
        1. there are workers with an expired TTL, mark it as dead, remove its jobs from WorkerInfo.assigned_jobs and redistribute into job_queue, and pop it out of worker_heartbeat_timer too
        for each job within the hashset for the worker, increment infra_interruptions 
        2. sleep until the next duration, race it against 
         */

        match worker_heartbeat_deadline_check(state.worker_heartbeat_timer.clone()).await {
            DeadlineResult::DeadlineExpired(worker_uuid) => {

                let mut jobs_to_be_reassigned = HashSet::new();
                

                if let Some(mut worker_mutref) = state.workers.get_mut(&worker_uuid) {
                    worker_mutref.state = WorkerState::Dead;
                    jobs_to_be_reassigned = std::mem::replace(&mut worker_mutref.assigned_jobs, HashSet::new());
                    //extract out jobs
                } else {
                    continue;
                }

                if jobs_to_be_reassigned.is_empty() {
                    continue;
                }

                let mut new_queued_jobs = vec![];
                let mut new_deadlettered_jobs = vec![];

                for job_uuid in jobs_to_be_reassigned {
                    if let Some((_, recycled_job)) = state.running_jobs.remove(&job_uuid) {

                        //check here if infra_interruptions reached the max cap
                        //if so, then redistribute it to completed_jobs instead 
                        
                        let mut new_queued_job;

                        if let RunningPhase::Executing { worker_id, started_at } = recycled_job.running_phase {
                            new_queued_job = Job {
                                id: recycled_job.id,
                                job_type: recycled_job.job_type.clone(),
                                payload: recycled_job.payload,
                                priority: recycled_job.priority,
                                created_at: recycled_job.created_at,
                                retry_count: recycled_job.retry_count,
                                infra_interruptions: recycled_job.infra_interruptions,
                                requirements: recycled_job.requirements.clone(),
                                metadata: recycled_job.metadata.clone(),
                                retry_policy: recycled_job.retry_policy.clone(),
                                state: JobState::Running { worker_id, started_at },
                            };
                            
                        } else { 
                            state.running_jobs.insert(recycled_job.id, recycled_job); //reinsert if it its not in the executing phase
                            continue;
                        }

                        let next = determine_reclaim_event(&new_queued_job);

                        if let Err(e) = transition(&mut new_queued_job, next) {
                            tracing::warn!(
                                job_id = %&new_queued_job.id,
                                "transition for job retrieved from lost worker failed"
                            );
                        }

                        match new_queued_job.state {
                            JobState::Queued => {
                                new_queued_jobs.push(QueuedJob {
                                    id: new_queued_job.id,
                                    job_type: new_queued_job.job_type,
                                    payload: new_queued_job.payload,
                                    priority: new_queued_job.priority,
                                    created_at: new_queued_job.created_at,
                                    retry_count: new_queued_job.retry_count,
                                    infra_interruptions: new_queued_job.infra_interruptions,
                                    requirements: new_queued_job.requirements,
                                    metadata: new_queued_job.metadata,
                                    retry_policy: new_queued_job.retry_policy,
                                });
                            },

                            JobState::DeadLettered { reason } => {
                                new_deadlettered_jobs.push(CompletedJob {
                                    id: new_queued_job.id,
                                    job_type: new_queued_job.job_type,
                                    payload: new_queued_job.payload,
                                    priority: new_queued_job.priority,
                                    created_at: new_queued_job.created_at,
                                    retry_count: new_queued_job.retry_count,
                                    infra_interruptions: new_queued_job.infra_interruptions,
                                    requirements: new_queued_job.requirements,
                                    metadata: new_queued_job.metadata,
                                    retry_policy: new_queued_job.retry_policy,
                                    completed_at: now_millis(),
                                    outcome: CompletedJobOutcome::DeadLettered { reason },
                                });
                            },

                            _ => {
                                state.running_jobs.insert(recycled_job.id, recycled_job);
                                continue;
                            }, //line 208
                        }

                    } else { //line 211
                        continue;
                    }   

                }

                if !new_queued_jobs.is_empty() {
                    {
                        let mut job_queue_guard = state.job_queue.lock();
                        for new_queued_job in new_queued_jobs {
                            job_queue_guard.insert(new_queued_job);
                        }
                    }
                    
                }

                if !new_deadlettered_jobs.is_empty() {
                    for new_dlq_job in new_deadlettered_jobs {
                        state.completed_jobs.insert(new_dlq_job.id, new_dlq_job);
                    }
                }


            },

            DeadlineResult::Waiting(deadline) => {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline.into()) => {},
                    _ = state.new_worker_deadline_notify.notified() => {},
                }
            },

            DeadlineResult::EmptyQueue => {
                state.new_worker_deadline_notify.notified().await;
            },

        }
        
    }
}

pub async fn worker_heartbeat_deadline_check(worker_heartbeat_timer: Arc<Mutex<BTreeSet<(Instant, WorkerId)>>>) -> DeadlineResult {
    
    let now = Instant::now();

    {
        let mut heartbeat_timer_guard = worker_heartbeat_timer.lock();
        if let Some(&(deadline, worker_uuid)) = heartbeat_timer_guard.first() {
            if deadline <= now {
                heartbeat_timer_guard.pop_first();
                return DeadlineResult::DeadlineExpired(worker_uuid);
            } else {
                return DeadlineResult::Waiting(deadline);
            }
        } else {
            return DeadlineResult::EmptyQueue;
        }
    } 
    
}