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
