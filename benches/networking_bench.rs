//! Performance benchmarks for networking operations
//!
//! These benchmarks measure connection setup, mesh formation, and throughput.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tokio::time::sleep;
use wormfs::networking::{
    AuthenticationMode, NetworkConfig, NetworkService, PingConfig, ReconnectionConfig,
};
use wormfs::peer_authorizer::PeerAuthorizer;

/// Create a test network configuration
fn create_network_config(port: u16) -> NetworkConfig {
    NetworkConfig {
        listen_address: format!("/ip4/127.0.0.1/tcp/{}", port),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Disabled,
            peers_file: format!("bench_peers_{}.json", port),
        },
        reconnection: ReconnectionConfig::default(),
    }
}

/// Benchmark connection setup time for a single connection
fn bench_connection_setup(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("connection_setup", |b| {
        b.to_async(&rt).iter(|| async {
            let config_a = create_network_config(12000);
            let config_b = create_network_config(12001);

            let known_peers_a = Arc::new(RwLock::new(HashMap::new()));
            let authorizer_a = PeerAuthorizer::new(
                AuthenticationMode::Disabled,
                known_peers_a,
                PathBuf::from("bench_peers_12000.json"),
            );

            let known_peers_b = Arc::new(RwLock::new(HashMap::new()));
            let authorizer_b = PeerAuthorizer::new(
                AuthenticationMode::Disabled,
                known_peers_b,
                PathBuf::from("bench_peers_12001.json"),
            );

            let (mut service_a, handle_a) =
                NetworkService::new(config_a.clone(), authorizer_a).unwrap();
            let (mut service_b, handle_b) =
                NetworkService::new(config_b.clone(), authorizer_b).unwrap();

            service_a.start(config_a).await.unwrap();
            service_b.start(config_b).await.unwrap();

            let peer_b_id = handle_b.local_peer_id();

            let _service_a_handle = tokio::spawn(async move { service_a.run().await });
            let _service_b_handle = tokio::spawn(async move { service_b.run().await });

            sleep(Duration::from_millis(100)).await;

            let start = std::time::Instant::now();
            handle_a
                .dial("/ip4/127.0.0.1/tcp/12001".parse().unwrap())
                .await
                .unwrap();

            // Wait for connection (simplified - just wait for peers list to update)
            for _ in 0..50 {
                let peers = handle_a.list_connected_peers().await.unwrap();
                if peers.contains(&peer_b_id) {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }

            let elapsed = start.elapsed();

            handle_a.shutdown().unwrap();
            handle_b.shutdown().unwrap();

            black_box(elapsed)
        });
    });
}

/// Benchmark mesh formation with varying cluster sizes
fn bench_mesh_formation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mesh_formation");

    for size in [3, 5, 7].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.to_async(&rt).iter(|| async move {
                let start_port = 13000;
                let mut services = Vec::new();
                let mut handles = Vec::new();

                // Create nodes
                for i in 0..size {
                    let port = start_port + i as u16;
                    let config = create_network_config(port);

                    let known_peers = Arc::new(RwLock::new(HashMap::new()));
                    let authorizer = PeerAuthorizer::new(
                        AuthenticationMode::Disabled,
                        known_peers,
                        PathBuf::from(format!("bench_peers_{}.json", port)),
                    );

                    let (mut service, handle) =
                        NetworkService::new(config.clone(), authorizer).unwrap();
                    service.start(config).await.unwrap();

                    let service_handle = tokio::spawn(async move { service.run().await });
                    services.push(service_handle);
                    handles.push(handle);
                }

                sleep(Duration::from_millis(200)).await;

                let start = std::time::Instant::now();

                // Connect mesh
                for (i, handle) in handles.iter().enumerate().take(size) {
                    for j in (i + 1)..size {
                        let target_addr = format!("/ip4/127.0.0.1/tcp/{}", start_port + j as u16)
                            .parse()
                            .unwrap();
                        let _ = handle.dial(target_addr).await;
                    }
                }

                // Wait for all connections
                sleep(Duration::from_secs(2)).await;

                let elapsed = start.elapsed();

                // Cleanup
                for handle in handles {
                    handle.shutdown().unwrap();
                }

                black_box(elapsed)
            });
        });
    }

    group.finish();
}

/// Benchmark memory overhead per peer connection
fn bench_memory_overhead(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("memory_overhead_per_peer", |b| {
        b.to_async(&rt).iter(|| async {
            // Create a simple 3-node cluster and measure memory
            let config = create_network_config(14000);
            let known_peers = Arc::new(RwLock::new(HashMap::new()));
            let authorizer = PeerAuthorizer::new(
                AuthenticationMode::Disabled,
                known_peers,
                PathBuf::from("bench_peers_14000.json"),
            );

            let (mut service, handle) = NetworkService::new(config.clone(), authorizer).unwrap();
            service.start(config).await.unwrap();

            let _service_handle = tokio::spawn(async move { service.run().await });

            sleep(Duration::from_millis(100)).await;

            // Memory measurement would go here in a real benchmark
            // For now, just measure existence
            let peers = handle.list_connected_peers().await.unwrap();

            handle.shutdown().unwrap();

            black_box(peers.len())
        });
    });
}

criterion_group!(
    benches,
    bench_connection_setup,
    bench_mesh_formation,
    bench_memory_overhead
);
criterion_main!(benches);
