// Build script for WormFS
//
// This script generates Rust code from protobuf definitions

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile protobuf files with clippy suppressions for generated code
    tonic_build::configure()
        .build_server(true) // Generate gRPC server code for SnapshotTransferService
        .build_client(true) // Generate gRPC client code for SnapshotTransferService
        .compile(&["proto/wormfs.proto"], &["proto"])?;

    // Tell cargo to rerun this build script if the proto file changes
    println!("cargo:rerun-if-changed=proto/wormfs.proto");

    Ok(())
}
