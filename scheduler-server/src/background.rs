//for the functions that run asynchronously in the background, like for observing the retry_queue and popping off when timers expire
//also for heartbeat detection function as well

use crate::scheduler_state::{
    CompletedJob, CompletedJobOutcome, QueuedJob, RunningJob, RunningPhase, SchedulerState,
};
use scheduler_core::job_data_structures::{Job, JobState, now_millis};
use scheduler_core::state_machine::{determine_reclaim_event, transition};
use scheduler_core::worker::WorkerId;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::warn;

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
            }

            DeadlineResult::Waiting(deadline) => {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline.into()) => {},
                    _ = state.retry_notify.notified() => {},
                }
            }

            DeadlineResult::EmptyQueue => {
                state.retry_notify.notified().await;
            }
        }
    }
}

pub enum DeadlineResult {
    DeadlineExpired(uuid::Uuid),
    Waiting(std::time::Instant),
    EmptyQueue,
}

pub async fn retry_deadline_check(
    retry_queue: Arc<Mutex<BTreeSet<(Instant, uuid::Uuid)>>>,
) -> DeadlineResult {
    let now = Instant::now();
    {
        let mut retry_queue_guard = retry_queue.lock();
        if let Some(&(timeout, job_uuid)) = retry_queue_guard.first() {
            if timeout <= now {
                retry_queue_guard.pop_first();
                DeadlineResult::DeadlineExpired(job_uuid)
            } else {
                DeadlineResult::Waiting(timeout)
            }
        } else {
            DeadlineResult::EmptyQueue
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
                //the worker is removed outright, a returning worker must re-register under a new id
                let Some((_, removed_worker)) = state.workers.remove(&worker_uuid) else {
                    continue;
                };

                let jobs_to_be_reassigned = removed_worker.assigned_jobs;

                if jobs_to_be_reassigned.is_empty() {
                    continue;
                }

                let mut new_queued_jobs = vec![];
                let mut new_deadlettered_jobs = vec![];

                for job_uuid in jobs_to_be_reassigned.into_keys() {
                    let Some((_, recycled_job)) = state.running_jobs.remove(&job_uuid) else {
                        continue;
                    };

                    //requeue it, or deadletter it once infra_interruptions reaches the cap
                    match reclaim_job(recycled_job) {
                        ReclaimOutcome::Requeued(queued_job) => new_queued_jobs.push(queued_job),

                        ReclaimOutcome::DeadLettered(deadlettered_job) => {
                            new_deadlettered_jobs.push(deadlettered_job)
                        }

                        ReclaimOutcome::NotExecuting(running_job) => {
                            warn!(
                                job_id = %&running_job.id,
                                "RunningPhase of job was in Retrying phase during assigned_job removal of Worker Lost event, currently only referenced by retry_queue"
                            );
                            //reinsert if it is not in the executing phase
                            state.running_jobs.insert(running_job.id, running_job);
                        }
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
                        state.total_dead_lettered.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }

            DeadlineResult::Waiting(deadline) => {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline.into()) => {},
                    _ = state.new_worker_deadline_notify.notified() => {},
                }
            }

            DeadlineResult::EmptyQueue => {
                state.new_worker_deadline_notify.notified().await;
            }
        }
    }
}

/// What became of a job taken back from a worker that lost it.
#[derive(Debug)]
pub enum ReclaimOutcome {
    /// back in line for dispatch, with infra_interruptions incremented
    Requeued(QueuedJob),
    /// infra_interruptions hit the cap (treated as a poison pill), or the transition failed
    DeadLettered(CompletedJob),
    /// the job was not Executing (the retry queue owns it), so there was nothing to reclaim.
    /// handed back unchanged so the caller can put it back.
    NotExecuting(RunningJob),
}

/// Decides what happens to a job whose dispatch was lost: the worker died (run_death_detector)
/// or the job went missing from its heartbeats (heartbeat reconciliation).
///
/// Pure: it takes a job the caller has already removed from running_jobs and touches no shared
/// state. Where the result goes, and which locks that needs, is the caller's decision.
/// A lost dispatch is an infrastructure fault, so this spends infra_interruptions, never retry_count.
pub fn reclaim_job(recycled_job: RunningJob) -> ReclaimOutcome {
    let RunningPhase::Executing {
        worker_id,
        started_at,
        dispatch_token,
    } = recycled_job.running_phase
    else {
        return ReclaimOutcome::NotExecuting(recycled_job);
    };

    let mut staging_job = Job {
        id: recycled_job.id,
        job_type: recycled_job.job_type,
        payload: recycled_job.payload,
        priority: recycled_job.priority,
        created_at: recycled_job.created_at,
        retry_count: recycled_job.retry_count,
        infra_interruptions: recycled_job.infra_interruptions,
        dispatch_token,
        requirements: recycled_job.requirements,
        metadata: recycled_job.metadata,
        retry_policy: recycled_job.retry_policy,
        state: JobState::Running {
            worker_id,
            started_at,
        },
    };

    //WorkerLost (requeue) or DeadLetter (infra_interruptions cap reached)
    let next = determine_reclaim_event(&staging_job);

    if let Err(_e) = transition(&mut staging_job, next) {
        tracing::warn!(
            job_id = %&staging_job.id,
            "transition for job retrieved from lost worker failed, will be deadlettered"
        );
    }

    let dead_letter_reason = match staging_job.state {
        JobState::Queued => {
            return ReclaimOutcome::Requeued(QueuedJob {
                id: staging_job.id,
                job_type: staging_job.job_type,
                payload: staging_job.payload,
                priority: staging_job.priority,
                created_at: staging_job.created_at,
                retry_count: staging_job.retry_count,
                infra_interruptions: staging_job.infra_interruptions,
                requirements: staging_job.requirements,
                metadata: staging_job.metadata,
                retry_policy: staging_job.retry_policy,
            });
        }

        JobState::DeadLettered { reason } => reason,

        //transition failed, so the state was left as Running
        _ => "transition failed during lost worker event".to_string(),
    };

    ReclaimOutcome::DeadLettered(CompletedJob {
        id: staging_job.id,
        job_type: staging_job.job_type,
        payload: staging_job.payload,
        priority: staging_job.priority,
        created_at: staging_job.created_at,
        retry_count: staging_job.retry_count,
        infra_interruptions: staging_job.infra_interruptions,
        requirements: staging_job.requirements,
        metadata: staging_job.metadata,
        retry_policy: staging_job.retry_policy,
        completed_at: now_millis(),
        outcome: CompletedJobOutcome::DeadLettered {
            reason: dead_letter_reason,
        },
    })
}

pub async fn worker_heartbeat_deadline_check(
    worker_heartbeat_timer: Arc<Mutex<BTreeSet<(Instant, WorkerId)>>>,
) -> DeadlineResult {
    let now = Instant::now();

    {
        let mut heartbeat_timer_guard = worker_heartbeat_timer.lock();
        if let Some(&(deadline, worker_uuid)) = heartbeat_timer_guard.first() {
            if deadline <= now {
                heartbeat_timer_guard.pop_first();
                DeadlineResult::DeadlineExpired(worker_uuid)
            } else {
                DeadlineResult::Waiting(deadline)
            }
        } else {
            DeadlineResult::EmptyQueue
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler_core::job_data_structures::RetryPolicy;
    use scheduler_core::state_machine::MAX_INFRA_INTERRUPTIONS;
    use std::collections::HashMap;

    fn running_job(running_phase: RunningPhase, infra_interruptions: u64) -> RunningJob {
        RunningJob {
            id: uuid::Uuid::now_v7(),
            job_type: "test".to_string(),
            payload: 0,
            priority: 1,
            created_at: 0,
            retry_count: 2,
            running_phase,
            infra_interruptions,
            requirements: HashMap::new(),
            metadata: HashMap::new(),
            retry_policy: RetryPolicy::NoRetry,
        }
    }

    fn executing() -> RunningPhase {
        RunningPhase::Executing {
            worker_id: uuid::Uuid::now_v7(),
            started_at: 0,
            dispatch_token: 7,
        }
    }

    #[test]
    fn reclaim_requeues_and_spends_an_infra_interruption() {
        let job = running_job(executing(), 0);
        let id = job.id;

        match reclaim_job(job) {
            ReclaimOutcome::Requeued(queued) => {
                assert_eq!(queued.id, id);
                assert_eq!(queued.infra_interruptions, 1);
                assert_eq!(
                    queued.retry_count, 2,
                    "a lost dispatch must not spend a retry"
                );
            }
            other => panic!("expected Requeued, got {other:?}"),
        }
    }

    #[test]
    fn reclaim_deadletters_at_the_infra_interruption_cap() {
        let job = running_job(executing(), MAX_INFRA_INTERRUPTIONS);
        let id = job.id;

        match reclaim_job(job) {
            ReclaimOutcome::DeadLettered(completed) => {
                assert_eq!(completed.id, id);
                assert!(matches!(
                    completed.outcome,
                    CompletedJobOutcome::DeadLettered { .. }
                ));
            }
            other => panic!("expected DeadLettered, got {other:?}"),
        }
    }

    #[test]
    fn reclaim_hands_back_a_job_that_is_not_executing() {
        let job = running_job(RunningPhase::Retrying, 0);
        let expected = job.clone();

        match reclaim_job(job) {
            ReclaimOutcome::NotExecuting(returned) => assert_eq!(returned, expected),
            other => panic!("expected NotExecuting, got {other:?}"),
        }
    }
}
