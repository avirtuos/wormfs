// Build script for WormFS
//
// This script generates Rust code from protobuf definitions

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile protobuf files
    tonic_build::configure()
        .build_server(false) // We don't need gRPC server code yet (Phase 3A)
        .build_client(false) // We don't need gRPC client code yet (Phase 3A)
        .compile(&["proto/wormfs.proto"], &["proto"])?;

    // Tell cargo to rerun this build script if the proto file changes
    println!("cargo:rerun-if-changed=proto/wormfs.proto");

    Ok(())
}
