fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .type_attribute(
            "scheduler.RequestWorkResponse.result",
            "#[allow(clippy::large_enum_variant)]",
        )
        .compile_protos(&["proto/scheduler.proto"], &["proto"])?;
    Ok(())
}
