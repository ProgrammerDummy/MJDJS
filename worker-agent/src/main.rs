mod executor;
mod heartbeat;
mod poll_loop;

use parking_lot::Mutex;
use scheduler_core::proto;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::Notify;
use tonic::Request;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use clap::Parser;
use std::net::SocketAddr;

use executor::{AlwaysFailJob, ExecutorRegistry, SleepJob};

use tokio_util::sync::CancellationToken;

use crate::heartbeat::{send_heartbeats, sleep_timer};
use crate::poll_loop::{connect_with_backoff, poll_loop};

pub fn cached_hostname() -> &'static str {
    static HOSTNAME: OnceLock<String> = OnceLock::new();

    HOSTNAME.get_or_init(|| gethostname::gethostname().to_string_lossy().into_owned())
}

#[derive(Parser)]
struct Args {
    #[arg(long)]
    scheduler_addr: SocketAddr,
    #[arg(long, default_value = "1")]
    max_concurrent_jobs: u32,
    #[arg(long, value_parser = parse_kv)]
    capabilities: Vec<(String, String)>,
    #[arg(long, value_parser = parse_kv)]
    tags: Vec<(String, String)>,
}

fn parse_kv(s: &str) -> Result<(String, String), String> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| format!("expected key=value, got `{s}`"))?;
    Ok((k.to_string(), v.to_string()))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let mut registry = ExecutorRegistry::new();

    registry.register("sleep".into(), Arc::new(SleepJob {}));
    registry.register("always-fail".into(), Arc::new(AlwaysFailJob {}));
    let registry = Arc::new(registry);

    let worker_id = register_with_retry(&args, &registry).await;

    /*
    now here we can check if we got a valid worker_id
     */

    let worker_id = match worker_id {
        Some(id) => id,
        None => {
            //if it returned a None, then it means that the worker can't register
            //already logged wtihin register_with_retry if it is returning None, so exit
            return;
        }
    };

    let worker_uuid = Arc::new(Mutex::new(worker_id));
    let token_map: Arc<Mutex<HashMap<uuid::Uuid, CancellationToken>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let main_token = CancellationToken::new();
    let stop_accepting_work = Arc::new(AtomicBool::new(false));
    let shutting_down = Arc::new(AtomicBool::new(false));
    let needs_reregister = Arc::new(Notify::new());

    //spawn tasks with clone and wait for reregistration or user shutdown

    tokio::spawn(send_heartbeats(
        args.scheduler_addr,
        worker_uuid.clone(),
        stop_accepting_work.clone(),
        needs_reregister.clone(),
    ));

    let work_handle = tokio::spawn(poll_loop(
        args.scheduler_addr,
        worker_uuid.clone(),
        args.max_concurrent_jobs,
        token_map.clone(),
        main_token.clone(),
        stop_accepting_work.clone(),
        shutting_down.clone(),
        registry.clone(),
        needs_reregister.clone(),
    ));

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {

                shutting_down.store(true, Ordering::Relaxed);
                //let the jobs finish
                break;
            }

            _ = needs_reregister.notified() => {

                //in the case where the needs_reregistration signal is called, the jobs that will finish will not be accepted by scheduler server
                //so just cancel all of them

                {
                    let mut token_map_guard = token_map.lock();

                    //cancel all the jobs

                    for (_, cancel_token) in &mut *token_map_guard {
                        cancel_token.cancel();
                    }


                }

                let new_worker_id = match register_with_retry(&args, &registry).await {
                    Some(id) => id,
                    None => {
                        //if it returned a None, then it means that the worker can't register
                        //already logged wtihin register_with_retry if it is returning None, so exit
                        return;
                    }
                };

                {
                    let mut guard = worker_uuid.lock();
                    *guard = new_worker_id;
                }
            }



        }
    }

    tokio::select! {
        _ = work_handle => {

        }

        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
            main_token.cancel();
            //force a shutdown if taking too long
        }
    }
}

/*
needs CLI with arguments:

scheduler server addr (socket)
max concurrent jobs to run
capabilites
tags?

job types is now derived from registry rather than the other way around
*/

/*

struct for a worker:
job pool: arc and parking_lot mutex with a vector or hashmap of jobs?
executor registry --> this ties into worker capabiliites
metadata about worker?
*/

async fn register_with_retry(args: &Args, registry: &Arc<ExecutorRegistry>) -> Option<uuid::Uuid> {
    let mut client = connect_with_backoff(args.scheduler_addr).await;

    let worker = proto::Worker {
        capabilities: args.capabilities.clone().into_iter().collect(),
        hostname: cached_hostname().into(),
        tags: args.tags.clone().into_iter().collect(),
        job_types_supported: registry.job_types(),
        max_concurrent_jobs: args.max_concurrent_jobs,
    };

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
