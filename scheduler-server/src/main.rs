mod service;
use scheduler_core::proto::scheduler_service_server::SchedulerServiceServer;

use crate::service::MySchedulerService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("SCHEDULER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;

    let service = MySchedulerService::new();

    println!("scheduler-server listening on {addr}");

    tonic::transport::Server::builder()
        .add_service(SchedulerServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}