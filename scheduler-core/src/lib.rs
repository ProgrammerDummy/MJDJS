pub mod state_machine;
pub mod job_data_structures;

pub mod proto {
    tonic::include_proto!("scheduler");
}

//you can find the generated functions from proto file in target/debug/build/scheduler-core.../out/scheduler.rs