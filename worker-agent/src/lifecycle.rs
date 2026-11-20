//! Worker lifecycle: registering with the scheduler, re-registering after the lease is lost, and
//! the forced-shutdown path. This logic used to live inline in main(); it was moved here unchanged
//! so tests can drive it against FakeScheduler. main() still owns the select loop and calls these.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use scheduler_core::proto;
use tokio_util::sync::CancellationToken;
use tonic::Request;

use crate::heartbeat::sleep_timer;
use crate::poll_loop::{TokenMap, connect_with_backoff};

/// What handle_reregister did with one signal from reg_rx.
#[derive(Debug, PartialEq, Eq)]
pub enum ReregisterOutcome {
    /// The signal carried an id that is no longer current (a duplicate, or one sent before the
    /// last swap), so it was ignored.
    Stale,
    /// Every held dispatch was revoked and cancelled, and the worker registered again;
    /// worker_uuid now holds this id.
    Reregistered(uuid::Uuid),
    /// register_with_retry gave up. main() exits on this.
    RegistrationFailed,
}

pub async fn register_with_retry(addr: SocketAddr, worker: &proto::Worker) -> Option<uuid::Uuid> {
    let mut client = connect_with_backoff(addr).await;

    //derive job_types from register

    let mut retries = 0;

    loop {
        if retries != 0 {
            sleep_timer(retries).await;
        }

        let uuid = match client.register(Request::new(worker.clone())).await {
            Ok(worker_uuid) => worker_uuid,

            Err(e) => {
                if e.code() == tonic::Code::Unknown
                    || e.code() == tonic::Code::Unavailable
                    || e.code() == tonic::Code::DeadlineExceeded
                {
                    retries += 1;
                    continue;
                }

                tracing::warn!("the registration request failed for non-transport related reasons");
                return None;
            }
        };

        let worker_uuid = uuid.into_inner();

        let Ok(worker_uuid) = uuid::Uuid::from_slice(&worker_uuid.worker_id) else {
            retries += 1;
            tracing::warn!("invalid uuid was given for worker id, should be impossible");
            continue;
        };

        return Some(worker_uuid);
    }
}

/// Handles one re-register signal received from reg_rx.
pub async fn handle_reregister(
    received_uuid: uuid::Uuid,
    worker_uuid: &Mutex<uuid::Uuid>,
    token_map: &TokenMap,
    addr: SocketAddr,
    worker: &proto::Worker,
) -> ReregisterOutcome {
    if received_uuid != *worker_uuid.lock() {
        return ReregisterOutcome::Stale;
    }

    //in the case where the needs_reregistration signal is called, the jobs that will finish will not be accepted by scheduler server
    //so just cancel all of them
    {
        let token_map_guard = token_map.lock();

        //during reregistration, the worker loses its lease on all jobs so
        //revoke flag is set and all jobs are cancelled
        for (cancel_token, revoked) in token_map_guard.values() {
            revoked.store(true, Ordering::SeqCst);
            cancel_token.cancel();
        }
    }

    let new_worker_id = match register_with_retry(addr, worker).await {
        Some(id) => id,
        None => {
            //if it returned a None, then it means that the worker can't register
            //already logged wtihin register_with_retry if it is returning None, so exit
            return ReregisterOutcome::RegistrationFailed;
        }
    };

    {
        let mut guard = worker_uuid.lock();
        *guard = new_worker_id;
    }

    ReregisterOutcome::Reregistered(new_worker_id)
}

/// The forced-shutdown path, taken when jobs have not drained within the grace period.
pub fn force_shutdown(token_map: &TokenMap, main_token: &CancellationToken) {
    {
        let token_map_guard = token_map.lock();

        //during a forced shutdown, since the user isnt explicitly cancelling jobs, but rather shutting the worker machine down
        //it will count as a lost lease, and will not report anything, and the death detector will reclaim the jobs after heartbeats stop
        for (_cancel_token, revoked) in token_map_guard.values() {
            revoked.store(true, Ordering::SeqCst);
        }
    }

    //force a shutdown if taking too long
    main_token.cancel();
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::poll_loop::*;
    use crate::test_support::*;

    use parking_lot::Mutex;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn duplicate_signal_handle_reregister() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let worker_id = fake.add_worker();

        let locked_worker_id = Arc::new(Mutex::new(worker_id));

        let token_map = empty_token_map();

        let test_worker = test_worker();

        let outcome =
            handle_reregister(worker_id, &locked_worker_id, &token_map, addr, &test_worker).await;

        assert_eq!(
            outcome,
            ReregisterOutcome::Reregistered(*locked_worker_id.lock())
        );
        assert_eq!(fake.registrations(), 1);

        let outcome =
            handle_reregister(worker_id, &locked_worker_id, &token_map, addr, &test_worker).await;

        assert_eq!(outcome, ReregisterOutcome::Stale);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reregistration_revokes_held_dispatch() {
        let (addr, _fake) = FakeScheduler::spawn().await;

        let test_worker = test_worker();
        let worker_id = register_with_retry(addr, &test_worker).await.unwrap();

        let locked_worker_id = Arc::new(Mutex::new(worker_id));

        let token_map = empty_token_map();
        let root = CancellationToken::new();

        let (_, job_one_uuid) = test_job(300, 7);
        let (_, job_two_uuid) = test_job(300, 77);

        insert_dispatch(&token_map.clone(), job_one_uuid, 7, &root);

        insert_dispatch(&token_map.clone(), job_two_uuid, 77, &root);

        let outcome =
            handle_reregister(worker_id, &locked_worker_id, &token_map, addr, &test_worker).await;

        assert_eq!(
            outcome,
            ReregisterOutcome::Reregistered(*locked_worker_id.lock())
        );

        {
            let token_map_guard = token_map.lock();
            let dum = token_map_guard.get(&(job_one_uuid, 7)).unwrap();
            assert!(dum.0.is_cancelled());
            assert!(dum.1.load(Ordering::SeqCst));
        }

        {
            let token_map_guard = token_map.lock();
            let dum = token_map_guard.get(&(job_two_uuid, 77)).unwrap();
            assert!(dum.0.is_cancelled());
            assert!(dum.1.load(Ordering::SeqCst));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reject_registrations_test() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let worker_id = fake.add_worker();

        let locked_worker_id = Arc::new(Mutex::new(worker_id));

        let token_map = empty_token_map();

        let test_worker = test_worker();

        fake.reject_registrations();

        let outcome =
            handle_reregister(worker_id, &locked_worker_id, &token_map, addr, &test_worker).await;

        assert_eq!(*locked_worker_id.lock(), worker_id);
        assert_eq!(outcome, ReregisterOutcome::RegistrationFailed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn force_shutdown_test() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let worker_id = fake.add_worker();
        let client = client(addr).await;

        let token_map = empty_token_map();

        let (job, job_uuid) = test_job(2000, 7);

        let root = CancellationToken::new();

        let (cancel, revoked) = insert_dispatch(&token_map, job_uuid, 7, &root);

        let wait = tokio::spawn(run_job(
            job,
            job_uuid,
            sleep_executor(),
            cancel,
            client,
            worker_id,
            token_map.clone(),
            permit().await,
        ));

        force_shutdown(&token_map, &root);

        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), wait)
            .await
            .expect("run_job timed out");

        assert!(fake.reports().is_empty());

        assert!(revoked.load(Ordering::SeqCst));

        {
            let token_map_guard = token_map.lock();

            assert!(token_map_guard.is_empty());
        }
    }
}
