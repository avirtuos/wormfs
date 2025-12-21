fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile protocol buffers
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(
            &[
                "proto/common.proto",
                "proto/filesystem.proto",
                "proto/chunk.proto",
                "proto/snapshot.proto",
                "proto/transaction_log.proto",
                "proto/health.proto",
            ],
            &["proto"],
        )?;

    Ok(())
}
