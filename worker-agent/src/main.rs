mod executor;
mod heartbeat;
mod lifecycle;
mod poll_loop;
#[cfg(test)]
mod test_support;

use parking_lot::Mutex;
use scheduler_core::proto;
use std::sync::Arc;
use std::sync::OnceLock;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use clap::Parser;
use std::net::SocketAddr;

use executor::{AlwaysFailJob, ExecutorRegistry, SleepJob};

use tokio_util::sync::CancellationToken;

use crate::heartbeat::send_heartbeats;
use crate::lifecycle::{ReregisterOutcome, force_shutdown, handle_reregister, register_with_retry};
use crate::poll_loop::{TokenMap, poll_loop};

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

    let worker = worker_descriptor(&args, &registry);

    let worker_id = register_with_retry(args.scheduler_addr, &worker).await;

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
    let token_map: TokenMap = Arc::new(Mutex::new(HashMap::new()));
    let main_token = CancellationToken::new();
    let stop_accepting_work = Arc::new(AtomicBool::new(false));
    let shutting_down = Arc::new(AtomicBool::new(false));

    let (reg_tx, mut reg_rx) = tokio::sync::mpsc::channel::<uuid::Uuid>(16);

    //spawn tasks with clone and wait for reregistration or user shutdown

    tokio::spawn(send_heartbeats(
        args.scheduler_addr,
        worker_uuid.clone(),
        stop_accepting_work.clone(),
        token_map.clone(),
        reg_tx.clone(),
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
        reg_tx.clone(),
    ));

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {

                shutting_down.store(true, Ordering::Relaxed);
                //let the jobs finish
                break;
            }

            Some(received_uuid) = reg_rx.recv() => {
                //lifecycle::handle_reregister ignores stale ids, otherwise revokes and cancels every
                //held dispatch, registers again and swaps worker_uuid
                match handle_reregister(
                    received_uuid,
                    &worker_uuid,
                    &token_map,
                    args.scheduler_addr,
                    &worker,
                ).await {
                    ReregisterOutcome::Stale | ReregisterOutcome::Reregistered(_) => {}
                    ReregisterOutcome::RegistrationFailed => return,
                }
            }

            else => {
                return;
            }



        }
    }

    tokio::select! {
        _ = work_handle => {

        }

        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
            //force a shutdown if taking too long: revoke every held dispatch, then cancel
            force_shutdown(&token_map, &main_token);
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

/// What this worker tells the scheduler about itself at Register.
fn worker_descriptor(args: &Args, registry: &ExecutorRegistry) -> proto::Worker {
    proto::Worker {
        capabilities: args.capabilities.clone().into_iter().collect(),
        hostname: cached_hostname().into(),
        tags: args.tags.clone().into_iter().collect(),
        //job_types is derived from the registry
        job_types_supported: registry.job_types(),
        max_concurrent_jobs: args.max_concurrent_jobs,
    }
}
