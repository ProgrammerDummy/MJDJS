//call RequestWork rpc
//if there is no work available, then retry after a timeout, fixed (100 ms)? vs exponential backoff?
//remember to add jitter as well
//if there is work, then call executor and after call report_result RPC
//keep in-flight jobs in memory as well so request_work is not called to overload

//tracking mechanism for in-flight jobs since jobs run concurrentlyt and call executor concurrently too
//semaphore or atomic int?

use std::{collections::HashMap, sync::{Arc, atomic::AtomicBool}};

use parking_lot::Mutex;
use tokio::sync::Semaphore;

use scheduler_core::proto;
use tokio_util::sync::CancellationToken;
use std::net::SocketAddr;


pub async fn poll_loop(
    addr: SocketAddr,
    worker_uuid: Arc<Mutex<uuid::Uuid>>, 
    max_concurrent_jobs: u32, 
    token_map: Arc<Mutex<HashMap<CancellationToken, uuid::Uuid>>>,
    capabilities: HashMap<String, String>,
    stop_accepting_work: Arc<AtomicBool>,
    ) {

    loop {

        let displayable_worker_id = {
            let guard = worker_uuid.lock();
            guard.clone()
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
                    "poll_loop could not connect to scheduler server, retrying"  
                );
                continue;
            }
        };

        //create a semaphore from max_concurrent_jobs
        let current_jobs = Semaphore::new(max_concurrent_jobs as usize);
        
        
        //always have the cancellationtoken ready
        //race it with sleeps or entire loop?


        
        loop {
            //check if we have space by incrementing job pool semaphore
            if let Err(e) = current_jobs.acquire().await {
                tracing::warn!("acquiring current_jobs semaphore failed");
            } 


            //
            //check if work is available
            //if available, receive Job and insert into worker


        }
    
    }


}


/*

2 loops similar idea to heartbeat.rs

outer loop handles connections and configuration

inner loop handles job request and execution and reporting of results


for a cancellationtoken to job mapping, use a parent cancellationtoken for main process and 
child_token for jobs

parent token is called for shutdowns 
child token called by scheduler-client cancel_job RPC, processed by server and sent through HeartbeatControl


todo:
- add a new oneof variant to HeartbeatControl for signaling for job cancellation

*/