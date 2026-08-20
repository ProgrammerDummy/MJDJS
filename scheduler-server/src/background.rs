//for the functions that run asynchronously in the background, like for observing the retry_queue and popping off when timers expire
//also for heartbeat detection function as well

use crate::scheduler_state::{SchedulerState, QueuedJob};
use std::time::Instant;

//spawn these in separate tokio threads
//do this during initialization of services

pub async fn run_retry_queue_monitor(state: SchedulerState) {
    loop {
        let now = Instant::now();

        let expired_job_uuid = {
            let mut retry_queue_guard = state.retry_queue.lock().unwrap();

            match retry_queue_guard.first() {
                Some((timeout, _)) if (*timeout <= now) => {
                    retry_queue_guard.pop_first().map(|(_, job_uuid) | job_uuid)
                    //remove if expired
                },

                _ => None,
            }           
        };

        let Some(job_uuid) = expired_job_uuid else {
            //queue was empty or there was no job whose deadline had expired in this case
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            continue;
        };

        //remove job from running_job, pop it out for recycling into QueuedJob
        //construct new QueuedJob and insert into job_queue

        let recycled_job = state.running_jobs.remove(&job_uuid); //pop it out of running_jobs
        
        let Some((_, recycled_job)) = recycled_job else {
            tracing::warn!(
                job_id = %&&job_uuid,
                "job present in retry_queue but not in running_jobs"
            );
            return;
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

        let mut job_queue_guard = state.job_queue.lock().unwrap();

        job_queue_guard.insert(new_queued_job);

        drop(job_queue_guard);

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    }
}

pub async fn run_death_detector(state: SchedulerState) {
    loop {
        
    }
}