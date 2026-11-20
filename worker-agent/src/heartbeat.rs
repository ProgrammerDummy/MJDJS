use std::net::SocketAddr;

use scheduler_core::proto::{self, ActiveJob, CancelReason};

use scheduler_core::job_data_structures::now_millis;

use proto::HeartbeatPing;

use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc::Sender;

use crate::poll_loop::TokenMap;

//shortened under cfg(test) so tests that need more than the first (immediate) ping don't wait 5s each
#[cfg(not(test))]
const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(test)]
pub(crate) const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

pub async fn send_heartbeats(
    addr: SocketAddr,
    worker_id: Arc<Mutex<uuid::Uuid>>,
    stop_accepting_work: Arc<AtomicBool>,
    token_map: TokenMap, //read to build active_jobs, and to find the dispatch a CancelJob targets
    reg_tx: Sender<uuid::Uuid>,
) {
    //needs to open the bidirectional stream for heartbeat RPC
    //fire a HeartbeatPing every 5 seconds
    //have broken stream reconnect after a certain timeout
    //when StopAcceptingWork is received from server, worker is dead, have flag set when it happens?
    //self kill after?

    let mut retry_count = 0;
    let mut consecutive_nones = 0;

    //client creation
    loop {
        if retry_count != 0 {
            sleep_timer(retry_count).await;
        }

        let displayable_worker_id = {
            let guard = worker_id.lock();
            *guard
        };

        let worker_id_bytes = { displayable_worker_id.as_bytes().to_vec() };

        let channel = match tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
        {
            Ok(channel) => channel,
            Err(_e) => {
                tracing::warn!(
                    worker_id = %displayable_worker_id,
                    "heartbeat could not connect to scheduler server, retrying"
                );
                retry_count += 1;
                continue;
            }
        };

        tracing::info!("gRPC channel for worker: {displayable_worker_id} is connected to {addr}");

        let mut worker_client = proto::worker_service_client::WorkerServiceClient::new(channel);

        let (tx, rx) = tokio::sync::mpsc::channel(16);

        let output_stream = tokio_stream::wrappers::ReceiverStream::from(rx);

        let mut server_response = match worker_client.heartbeat(output_stream).await {
            Ok(response) => response.into_inner(),

            Err(e) => {
                tracing::warn!(
                    worker_id = %displayable_worker_id,
                    "server's response to client heartbeat RPC call returned an error: {e} for worker: {displayable_worker_id}"
                );
                retry_count += 1;
                continue;
            }
        };

        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        //set a 5 second interval timer to fire heartbeats off, a missed tick will be skipped

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(_e) = tx.send(HeartbeatPing {
                        worker_id: worker_id_bytes.clone(),
                        sent_at: now_millis(),
                        active_jobs: {
                            //building ActiveJob vector from token_map

                            let token_map_guard = token_map.lock();

                            let owned_keys : Vec<(uuid::Uuid, u64)> = token_map_guard.keys().copied().collect();

                            let mut active_jobs_vec = vec![];

                            for (job_uuid, token) in owned_keys {

                                active_jobs_vec.push(ActiveJob {
                                    job_id: job_uuid.clone().as_bytes().to_vec(),
                                    dispatch_token: token,
                                });
                            }

                            active_jobs_vec

                        },
                    }).await {

                        //server receiving end is gone

                        tracing::warn!(
                            worker_id = %displayable_worker_id,
                            "failed to send heartbeat, server side receiver stream broken for worker: {displayable_worker_id}"
                        );
                        break;
                    }
                }

                msg = server_response.message() => {
                    match msg {
                        Ok(Some(heartbeat_control)) => {
                            match heartbeat_control {
                                proto::HeartbeatControl {
                                    directive: Some(proto::heartbeat_control::Directive::Ack(proto::Ack {}))
                                } => {
                                    retry_count = 0;
                                },

                                proto::HeartbeatControl {
                                    directive: Some(proto::heartbeat_control::Directive::StopAcceptingWork(proto::StopAcceptingWork {}))
                                } => {

                                    stop_accepting_work.store(true, Ordering::Relaxed); //set flag

                                    //this flag should be shared with the request work worker-side handler, if set then stop requesting
                                    //within 15 seconds should drain
                                },

                                proto::HeartbeatControl {
                                    directive: Some(proto::heartbeat_control::Directive::CancelJob(cancel)),
                                } => {
                                    //looks up (job_id, dispatch_token) in token_map and for CancelReason::Revoked
                                    //sets the revoked flag first, then cancels. CancelReason::User cancels without the flag.

                                    {
                                        let mut token_map_guard = token_map.lock();

                                        let Ok(job_id) = uuid::Uuid::from_slice(&cancel.job_id) else {
                                            tracing::warn!("uuid conversion failed during CancelJob handling at worker: {displayable_worker_id}");
                                            continue;
                                        };

                                        let dispatch_token = cancel.dispatch_token;

                                        if let Some(entry) = token_map_guard.get_mut(&(job_id, dispatch_token)) {
                                            //reason() is prost's typed getter: it returns the CancelReason enum, and
                                            //maps any number this worker doesn't recognise to Unspecified
                                            match cancel.reason() {
                                                CancelReason::User => {
                                                    //user requested cancellation: flag stays false, so run_job reports Cancelled
                                                    entry.0.cancel();
                                                },

                                                //Revoked: the scheduler took the job back, so report nothing.
                                                //Unspecified is treated the same way. Reporting Cancelled is terminal,
                                                //while staying silent lets the scheduler reclaim the job, which can be undone.
                                                CancelReason::Revoked | CancelReason::Unspecified => {
                                                    entry.1.store(true, Ordering::SeqCst);
                                                    entry.0.cancel();
                                                },
                                            }
                                        } else {
                                            tracing::warn!("cancel_job failed to locate entry for token_map at worker: {displayable_worker_id}");
                                            continue;
                                        };


                                    }

                                },

                                proto::HeartbeatControl {
                                    directive: None,
                                } => {
                                    //this is an error
                                    tracing::warn!("HeartbeatControl received by worker: {displayable_worker_id} was None");
                                    break;
                                }


                            }
                            consecutive_nones = 0;
                        }

                        Ok(None) => {
                            consecutive_nones += 1;
                            //signal to scheduler that new registration is needed for this worker
                            if consecutive_nones >= 2 {
                                match reg_tx.try_send(displayable_worker_id) {
                                    Ok(_) => {}

                                    Err(_) => {
                                        break;
                                    }
                                }
                            }
                            break;
                            //stream was closed gracefully by the server
                            //the stream is closed by the server whenever break is hit in heartbeat RPC
                            //for example workers.get_mut(&worker_uuid) misses or invalid uuid in system
                            //so registration was stale, and new registration needed
                        }


                        Err(e) => {
                            if e.code() == tonic::Code::NotFound {
                                consecutive_nones += 1;
                                //signal to scheduler that new registration is needed for this worker
                                let _ = reg_tx.try_send(displayable_worker_id);

                                break;
                            }

                            //stream/transport broke, reconnect
                            consecutive_nones = 0;
                            tracing::warn!("transport layer broke for worker: {displayable_worker_id} with status: {e}");
                            break;
                        }
                    }
                }
            }
        }

        retry_count += 1;
    }
}

pub async fn sleep_timer(retry_count: u32) {
    const BASE_MS: u64 = 100;
    const CEILING_MS: u64 = 2000;

    let exp = retry_count.min(5);

    let sleep_time_ms = std::cmp::min(BASE_MS << exp, CEILING_MS);

    let jittered = (sleep_time_ms as f64 * rand::random_range(0.75..1.25)) as u64;

    tokio::time::sleep(std::time::Duration::from_millis(jittered)).await;
}

#[cfg(test)]
mod tests {

    use scheduler_core::proto::heartbeat_control::Directive;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::poll_loop::run_job;
    use crate::test_support::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn first_ping_test() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let worker_id = fake.add_worker();

        let locked_worker_id = Arc::new(Mutex::new(worker_id));
        let stop_accepting_work = Arc::new(AtomicBool::new(false));

        let (reg_tx, _reg_rx) = tokio::sync::mpsc::channel::<uuid::Uuid>(16);

        let token_map = empty_token_map();

        let root = CancellationToken::new();
        let dispatch_token = 7;
        let (_, job_uuid) = test_job(10, dispatch_token);
        insert_dispatch(&token_map, job_uuid, dispatch_token, &root);

        tokio::spawn(send_heartbeats(
            addr,
            locked_worker_id,
            stop_accepting_work,
            token_map,
            reg_tx,
        ));

        eventually(
            std::time::Duration::from_millis(400),
            "atleast 1 ping to come in",
            || !fake.pings().is_empty(),
        )
        .await;

        let pings = fake.pings();

        assert_eq!(pings.len(), 1);

        assert_eq!(pings[0].active_jobs.len(), 1);

        assert_eq!(
            uuid::Uuid::from_slice(&pings[0].active_jobs[0].job_id).unwrap(),
            job_uuid
        );
        assert_eq!(pings[0].active_jobs[0].dispatch_token, dispatch_token);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn after_run_job_dispatch_gone_from_ping() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let worker_id = fake.add_worker();

        let locked_worker_id = Arc::new(Mutex::new(worker_id));
        let stop_accepting_work = Arc::new(AtomicBool::new(false));

        let (reg_tx, _reg_rx) = tokio::sync::mpsc::channel::<uuid::Uuid>(16);

        let token_map = empty_token_map();

        let root = CancellationToken::new();
        let dispatch_token = 7;
        let (job, job_uuid) = test_job(10, dispatch_token);
        let (cancel, _revoked) = insert_dispatch(&token_map, job_uuid, dispatch_token, &root);

        tokio::spawn(send_heartbeats(
            addr,
            locked_worker_id,
            stop_accepting_work,
            token_map.clone(),
            reg_tx,
        ));

        eventually(
            std::time::Duration::from_millis(400),
            "atleast 1 ping to come in",
            || !fake.pings().is_empty(),
        )
        .await;

        run_job(
            job,
            job_uuid,
            sleep_executor(),
            cancel,
            client(addr).await,
            worker_id,
            token_map.clone(),
            permit().await,
        )
        .await;

        let n = fake.pings().len();

        eventually(
            std::time::Duration::from_millis(800),
            "atleast 2 pings to come in",
            || fake.pings().len() >= 2 + n,
        )
        .await;

        let pings = fake.pings();

        assert!(pings[n + 1].active_jobs.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancel_job_with_revoked() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let worker_id = fake.add_worker();

        let locked_worker_id = Arc::new(Mutex::new(worker_id));
        let stop_accepting_work = Arc::new(AtomicBool::new(false));

        let (reg_tx, _reg_rx) = tokio::sync::mpsc::channel::<uuid::Uuid>(16);

        let token_map = empty_token_map();

        let root = CancellationToken::new();
        let dispatch_token = 7;
        let (job, job_uuid) = test_job(300, dispatch_token);
        let (cancel, _revoked) = insert_dispatch(&token_map, job_uuid, dispatch_token, &root);

        tokio::spawn(send_heartbeats(
            addr,
            locked_worker_id,
            stop_accepting_work,
            token_map.clone(),
            reg_tx,
        ));

        eventually(
            std::time::Duration::from_millis(400),
            "atleast 1 ping to come in",
            || !fake.pings().is_empty(),
        )
        .await;

        fake.push_directive(Directive::CancelJob(proto::CancelJob {
            job_id: job.id,
            dispatch_token,
            reason: CancelReason::Revoked as i32,
        }))
        .await;

        eventually(
            std::time::Duration::from_millis(200),
            "checking if cancelled",
            || cancel.is_cancelled(),
        )
        .await;

        {
            let token_map_guard = token_map.lock();
            let dum = token_map_guard.get(&(job_uuid, dispatch_token)).unwrap();
            assert!(dum.1.load(Ordering::SeqCst));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancel_job_with_unspecified() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let worker_id = fake.add_worker();

        let locked_worker_id = Arc::new(Mutex::new(worker_id));
        let stop_accepting_work = Arc::new(AtomicBool::new(false));

        let (reg_tx, _reg_rx) = tokio::sync::mpsc::channel::<uuid::Uuid>(16);

        let token_map = empty_token_map();

        let root = CancellationToken::new();
        let dispatch_token = 7;
        let (job, job_uuid) = test_job(300, dispatch_token);
        let (cancel, _revoked) = insert_dispatch(&token_map, job_uuid, dispatch_token, &root);

        tokio::spawn(send_heartbeats(
            addr,
            locked_worker_id,
            stop_accepting_work,
            token_map.clone(),
            reg_tx,
        ));

        eventually(
            std::time::Duration::from_millis(400),
            "atleast 1 ping to come in",
            || !fake.pings().is_empty(),
        )
        .await;

        fake.push_directive(Directive::CancelJob(proto::CancelJob {
            job_id: job.id,
            dispatch_token,
            reason: CancelReason::Unspecified as i32,
        }))
        .await;

        eventually(
            std::time::Duration::from_millis(200),
            "checking if cancelled",
            || cancel.is_cancelled(),
        )
        .await;

        {
            let token_map_guard = token_map.lock();
            let dum = token_map_guard.get(&(job_uuid, dispatch_token)).unwrap();
            assert!(dum.1.load(Ordering::SeqCst));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancel_job_with_user() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let worker_id = fake.add_worker();

        let locked_worker_id = Arc::new(Mutex::new(worker_id));
        let stop_accepting_work = Arc::new(AtomicBool::new(false));

        let (reg_tx, _reg_rx) = tokio::sync::mpsc::channel::<uuid::Uuid>(16);

        let token_map = empty_token_map();

        let root = CancellationToken::new();
        let dispatch_token = 7;
        let (job, job_uuid) = test_job(300, dispatch_token);
        let (cancel, _revoked) = insert_dispatch(&token_map, job_uuid, dispatch_token, &root);

        tokio::spawn(send_heartbeats(
            addr,
            locked_worker_id,
            stop_accepting_work,
            token_map.clone(),
            reg_tx,
        ));

        eventually(
            std::time::Duration::from_millis(400),
            "atleast 1 ping to come in",
            || !fake.pings().is_empty(),
        )
        .await;

        fake.push_directive(Directive::CancelJob(proto::CancelJob {
            job_id: job.id,
            dispatch_token,
            reason: CancelReason::User as i32,
        }))
        .await;

        eventually(
            std::time::Duration::from_millis(200),
            "checking if cancelled",
            || cancel.is_cancelled(),
        )
        .await;

        {
            let token_map_guard = token_map.lock();
            let dum = token_map_guard.get(&(job_uuid, dispatch_token)).unwrap();
            assert!(!dum.1.load(Ordering::SeqCst));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancel_job_for_unknown_jobtoken_pair() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let worker_id = fake.add_worker();

        let locked_worker_id = Arc::new(Mutex::new(worker_id));
        let stop_accepting_work = Arc::new(AtomicBool::new(false));

        let (reg_tx, _reg_rx) = tokio::sync::mpsc::channel::<uuid::Uuid>(16);

        let token_map = empty_token_map();

        let root = CancellationToken::new();
        let dispatch_token = 7;
        let (job, job_uuid) = test_job(300, dispatch_token);
        insert_dispatch(&token_map, job_uuid, dispatch_token, &root);

        tokio::spawn(send_heartbeats(
            addr,
            locked_worker_id,
            stop_accepting_work,
            token_map.clone(),
            reg_tx,
        ));

        eventually(
            std::time::Duration::from_millis(400),
            "atleast 1 ping to come in",
            || !fake.pings().is_empty(),
        )
        .await;

        fake.push_directive(Directive::CancelJob(proto::CancelJob {
            job_id: job.id,
            dispatch_token: dispatch_token + 1,
            reason: CancelReason::User as i32,
        }))
        .await;

        let n = fake.pings().len();

        eventually(
            std::time::Duration::from_millis(800),
            "atleast 1 ping to come in",
            || fake.pings().len() >= n + 2,
        )
        .await;

        {
            let token_map_guard = token_map.lock();
            let dum = token_map_guard.get(&(job_uuid, dispatch_token)).unwrap();
            assert!(!dum.1.load(Ordering::SeqCst));
            assert!(!dum.0.is_cancelled());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn worker_forgotten() {
        let (addr, fake) = FakeScheduler::spawn().await;
        let worker_id = fake.add_worker();

        let locked_worker_id = Arc::new(Mutex::new(worker_id));
        let stop_accepting_work = Arc::new(AtomicBool::new(false));

        let (reg_tx, mut reg_rx) = tokio::sync::mpsc::channel::<uuid::Uuid>(16);

        let token_map = empty_token_map();

        let root = CancellationToken::new();
        let dispatch_token = 7;
        let (_, job_uuid) = test_job(300, dispatch_token);
        insert_dispatch(&token_map, job_uuid, dispatch_token, &root);

        tokio::spawn(send_heartbeats(
            addr,
            locked_worker_id,
            stop_accepting_work,
            token_map.clone(),
            reg_tx,
        ));

        eventually(
            std::time::Duration::from_millis(400),
            "atleast 1 ping to come in",
            || !fake.pings().is_empty(),
        )
        .await;

        fake.forget_worker(worker_id);

        tokio::time::timeout(std::time::Duration::from_secs(2), reg_rx.recv())
            .await
            .expect("expected a uuid sent through the channel to signal a new registration");
    }
}
