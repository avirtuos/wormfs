fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false) // We don't need server stubs for metadata protocol
        .build_client(false) // We don't need client stubs for metadata protocol
        .compile(&["proto/wormfs.proto"], &["proto"])?;
    Ok(())
}
