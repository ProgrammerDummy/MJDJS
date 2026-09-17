mod background;
mod scheduler_state;
mod service;
mod worker;
mod worker_service;

use scheduler_core::proto::{scheduler_service_server::SchedulerServiceServer, worker_service_server::{WorkerService, WorkerServiceServer}};

use crate::{service::MySchedulerService, worker_service::MyWorkerService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("SCHEDULER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;

    let scheduler_service = MySchedulerService::new();
    let worker_service = MyWorkerService::new(scheduler_service.scheduler_state.clone());

    println!("scheduler-server listening on: {addr}");
    
    tokio::spawn(background::run_retry_queue_monitor(scheduler_service.scheduler_state.clone()));
    tokio::spawn(background::run_death_detector(scheduler_service.scheduler_state.clone()));


    tonic::transport::Server::builder()
        .add_service(SchedulerServiceServer::new(scheduler_service))
        .add_service(WorkerServiceServer::new(worker_service))
        .serve(addr)
        .await?;

    Ok(())
}
