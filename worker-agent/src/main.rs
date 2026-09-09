mod executor;
mod heartbeat;
mod poll_loop;

fn main() {
    println!("Hello, world!");
}

use std::sync::OnceLock;

pub fn cached_hostname() -> &'static str {
    static HOSTNAME: OnceLock<String> = OnceLock::new();

    HOSTNAME.get_or_init(|| gethostname::gethostname().to_string_lossy().into_owned())
}

/*
needs CLI with arguments:

scheduler server addr (socket)
max concurrent jobs to run
job types
capabilites
tags?
*/

/*

struct for a worker:
job pool: arc and parking_lot mutex with a vector or hashmap of jobs?
executor registry --> this ties into worker capabiliites
metadata about worker?
*/
