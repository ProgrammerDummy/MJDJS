use std::net::SocketAddr;

use scheduler_core::proto;

use scheduler_core::job_data_structures::now_millis;

use proto::{HeartbeatPing};

use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use parking_lot::Mutex;

const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);



pub async fn send_heartbeats(addr: SocketAddr, worker_id: Arc<Mutex<uuid::Uuid>>, stop_accepting_work: Arc<AtomicBool>, new_registration_needed: Arc<AtomicBool>) {

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
            guard.clone()
        };

        let worker_id = {
            worker_id.lock().as_bytes().to_vec()
        };

        let channel = match tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await 
        {

            Ok(channel) => {
                channel
                
            }
            Err(e) => {
                tracing::warn!(
                    worker_id = %displayable_worker_id,
                    "could not connect to scheduler server, retrying"
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
            Ok(response) => {
                response.into_inner()
            }

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
                    if let Err(e) = tx.send(HeartbeatPing { worker_id: worker_id.clone(), sent_at: now_millis() }).await {

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
                            if(consecutive_nones >= 2) {
                                new_registration_needed.store(true, Ordering::Relaxed);
                            }
                            break;
                            //stream was closed gracefully by the server
                            //the stream is closed by the server whenever break is hit in heartbeat RPC
                            //for example workers.get_mut(&worker_uuid) misses or invalid uuid in system
                            //so registration was stale, and new registration needed 
                        }

                        Err(e) => {
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

async fn sleep_timer(retry_count: u32) {

    const BASE_MS : u64 = 100;
    const CEILING_MS : u64 = 2000;

    let exp = retry_count.min(5);

    let sleep_time_ms = std::cmp::min(BASE_MS << exp, CEILING_MS);
    
    let jittered = (sleep_time_ms as f64 * rand::random_range(0.75..1.25)) as u64;

    tokio::time::sleep(std::time::Duration::from_millis(jittered)).await;
}