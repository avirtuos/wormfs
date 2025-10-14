# Phase 5: Observability & Testing - Detailed Implementation Plan

## Overview
**Duration**: 1 week (5 working days)
**Goal**: Add comprehensive observability and testing capabilities to make WormFS production-ready
**Success Criteria**: System has full metrics coverage, health monitoring, automated testing, and is ready for production deployment
**Prerequisites**: Phase 4 complete (robustness and recovery working)

## Component Implementation Order

### Days 1-2: MetricService & Observability (Steps 1-2)

#### Step 1: Metrics Collection Infrastructure
**File**: `src/metric_service/implementation.rs`

**Tasks**:
1. Implement core MetricService with channel-based collection
   ```rust
   use tokio::sync::mpsc;
   use std::collections::HashMap;
   use std::sync::Arc;
   use parking_lot::RwLock;

   #[derive(Clone)]
   pub struct MetricServiceImpl {
       event_tx: mpsc::UnboundedSender<MetricEvent>,
       registry: Arc<RwLock<MetricRegistry>>,
   }

   struct MetricRegistry {
       counters: HashMap<String, Counter>,
       gauges: HashMap<String, Gauge>,
       histograms: HashMap<String, Histogram>,
   }

   #[derive(Debug, Clone)]
   pub struct MetricEvent {
       pub name: String,
       pub value: f64,
       pub metric_type: MetricType,
       pub unit: UnitType,
       pub labels: HashMap<String, String>,
       pub timestamp: SystemTime,
   }

   impl MetricServiceImpl {
       pub fn new(config: Config) -> Result<Self, Error> {
           let (event_tx, event_rx) = mpsc::unbounded_channel();

           let registry = Arc::new(RwLock::new(MetricRegistry {
               counters: HashMap::new(),
               gauges: HashMap::new(),
               histograms: HashMap::new(),
           }));

           let service = Self {
               event_tx,
               registry: registry.clone(),
           };

           // Start aggregation loop
           service.start_aggregation_loop(event_rx, registry);

           Ok(service)
       }

       fn start_aggregation_loop(
           &self,
           mut event_rx: mpsc::UnboundedReceiver<MetricEvent>,
           registry: Arc<RwLock<MetricRegistry>>,
       ) {
           tokio::spawn(async move {
               while let Some(event) = event_rx.recv().await {
                   let mut reg = registry.write();

                   match event.metric_type {
                       MetricType::Counter => {
                           let counter = reg.counters.entry(event.name.clone())
                               .or_insert_with(|| Counter::new(&event.name));
                           counter.increment(event.value);
                       }
                       MetricType::Gauge => {
                           let gauge = reg.gauges.entry(event.name.clone())
                               .or_insert_with(|| Gauge::new(&event.name));
                           gauge.set(event.value);
                       }
                       MetricType::Histogram => {
                           let histogram = reg.histograms.entry(event.name.clone())
                               .or_insert_with(|| Histogram::new(&event.name));
                           histogram.observe(event.value);
                       }
                   }
               }
           });
       }
   }

   #[async_trait]
   impl MetricService for MetricServiceImpl {
       fn publish_counter(&self, name: &str, value: f64, unit: UnitType) -> Result<(), Error> {
           let event = MetricEvent {
               name: name.to_string(),
               value,
               metric_type: MetricType::Counter,
               unit,
               labels: HashMap::new(),
               timestamp: SystemTime::now(),
           };

           self.event_tx.send(event)
               .map_err(|_| Error::ChannelClosed)?;

           Ok(())
       }

       fn publish_gauge(&self, name: &str, value: f64, unit: UnitType) -> Result<(), Error> {
           let event = MetricEvent {
               name: name.to_string(),
               value,
               metric_type: MetricType::Gauge,
               unit,
               labels: HashMap::new(),
               timestamp: SystemTime::now(),
           };

           self.event_tx.send(event)
               .map_err(|_| Error::ChannelClosed)?;

           Ok(())
       }

       fn publish_histogram(&self, name: &str, value: f64, unit: UnitType) -> Result<(), Error> {
           let event = MetricEvent {
               name: name.to_string(),
               value,
               metric_type: MetricType::Histogram,
               unit,
               labels: HashMap::new(),
               timestamp: SystemTime::now(),
           };

           self.event_tx.send(event)
               .map_err(|_| Error::ChannelClosed)?;

           Ok(())
       }

       fn publish_labeled(
           &self,
           name: &str,
           value: f64,
           metric_type: MetricType,
           unit: UnitType,
           labels: HashMap<String, String>,
       ) -> Result<(), Error> {
           let event = MetricEvent {
               name: name.to_string(),
               value,
               metric_type,
               unit,
               labels,
               timestamp: SystemTime::now(),
           };

           self.event_tx.send(event)
               .map_err(|_| Error::ChannelClosed)?;

           Ok(())
       }

       async fn get_snapshot(&self) -> Result<MetricSnapshot, Error> {
           let registry = self.registry.read();

           Ok(MetricSnapshot {
               counters: registry.counters.values().cloned().collect(),
               gauges: registry.gauges.values().cloned().collect(),
               histograms: registry.histograms.values().cloned().collect(),
               timestamp: SystemTime::now(),
           })
       }
   }
   ```

2. Define key metrics for all components
   ```rust
   // Raft metrics
   pub const RAFT_PROPOSALS_TOTAL: &str = "raft_proposals_total";
   pub const RAFT_PROPOSAL_LATENCY: &str = "raft_proposal_latency_seconds";
   pub const RAFT_LEADER_ELECTIONS: &str = "raft_leader_elections_total";
   pub const RAFT_LOG_SIZE: &str = "raft_log_size_bytes";

   // FileStore metrics
   pub const FILESTORE_CHUNK_WRITES: &str = "filestore_chunk_writes_total";
   pub const FILESTORE_CHUNK_READS: &str = "filestore_chunk_reads_total";
   pub const FILESTORE_WRITE_LATENCY: &str = "filestore_write_latency_seconds";
   pub const FILESTORE_READ_LATENCY: &str = "filestore_read_latency_seconds";
   pub const FILESTORE_CHUNKS_TOTAL: &str = "filestore_chunks_total";
   pub const FILESTORE_DISK_USAGE: &str = "filestore_disk_usage_bytes";

   // MetadataStore metrics
   pub const METADATA_OPERATIONS: &str = "metadata_operations_total";
   pub const METADATA_QUERY_LATENCY: &str = "metadata_query_latency_seconds";
   pub const METADATA_DB_SIZE: &str = "metadata_db_size_bytes";

   // Network metrics
   pub const NETWORK_BYTES_SENT: &str = "network_bytes_sent_total";
   pub const NETWORK_BYTES_RECEIVED: &str = "network_bytes_received_total";
   pub const NETWORK_ACTIVE_CONNECTIONS: &str = "network_active_connections";

   // Watchdog metrics
   pub const WATCHDOG_SHALLOW_CHECKS: &str = "watchdog_shallow_checks_total";
   pub const WATCHDOG_DEEP_CHECKS: &str = "watchdog_deep_checks_total";
   pub const WATCHDOG_REPAIRS: &str = "watchdog_repairs_total";
   pub const WATCHDOG_ORPHANS_CLEANED: &str = "watchdog_orphans_cleaned_total";
   ```

**Deliverables**:
- Lock-free metrics collection
- Background aggregation
- Core metric types (counter, gauge, histogram)
- Comprehensive metric definitions

#### Step 2: Prometheus Exporter & Health Endpoints
**File**: `src/metric_service/prometheus.rs`

**Tasks**:
1. Implement Prometheus exporter
   ```rust
   use prometheus::{Encoder, TextEncoder, Registry};

   pub struct PrometheusExporter {
       registry: prometheus::Registry,
       metrics: Arc<RwLock<MetricRegistry>>,
   }

   impl PrometheusExporter {
       pub fn new(metrics: Arc<RwLock<MetricRegistry>>) -> Result<Self, Error> {
           let registry = Registry::new();

           Ok(Self {
               registry,
               metrics,
           })
       }

       pub fn export(&self) -> Result<String, Error> {
           let metric_families = self.registry.gather();
           let encoder = TextEncoder::new();

           let mut buffer = Vec::new();
           encoder.encode(&metric_families, &mut buffer)?;

           Ok(String::from_utf8(buffer)?)
       }

       pub async fn register_metrics(&self) -> Result<(), Error> {
           let metrics = self.metrics.read();

           // Register all counters
           for (name, counter) in &metrics.counters {
               let opts = prometheus::Opts::new(name, counter.description());
               let prom_counter = prometheus::Counter::with_opts(opts)?;
               prom_counter.inc_by(counter.value());
               self.registry.register(Box::new(prom_counter))?;
           }

           // Register all gauges
           for (name, gauge) in &metrics.gauges {
               let opts = prometheus::Opts::new(name, gauge.description());
               let prom_gauge = prometheus::Gauge::with_opts(opts)?;
               prom_gauge.set(gauge.value());
               self.registry.register(Box::new(prom_gauge))?;
           }

           // Register all histograms
           for (name, histogram) in &metrics.histograms {
               let opts = prometheus::HistogramOpts::new(name, histogram.description());
               let prom_histogram = prometheus::Histogram::with_opts(opts)?;
               for value in histogram.samples() {
                   prom_histogram.observe(*value);
               }
               self.registry.register(Box::new(prom_histogram))?;
           }

           Ok(())
       }
   }
   ```

2. Add HTTP endpoints for metrics
   ```rust
   // In storage_endpoint/metrics_handler.rs
   use axum::{Router, routing::get, response::IntoResponse};

   pub fn metrics_routes(metric_service: Arc<MetricServiceImpl>) -> Router {
       Router::new()
           .route("/metrics", get(handle_metrics))
           .route("/health", get(handle_health))
           .route("/health/live", get(handle_liveness))
           .route("/health/ready", get(handle_readiness))
           .with_state(metric_service)
   }

   async fn handle_metrics(
       State(metrics): State<Arc<MetricServiceImpl>>,
   ) -> impl IntoResponse {
       let exporter = PrometheusExporter::new(metrics.registry.clone()).unwrap();
       exporter.register_metrics().await.unwrap();

       match exporter.export() {
           Ok(output) => (StatusCode::OK, output),
           Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)),
       }
   }

   async fn handle_health(
       State(metrics): State<Arc<MetricServiceImpl>>,
   ) -> impl IntoResponse {
       // Aggregate health from all components
       let health = HealthStatus {
           status: "healthy",
           timestamp: SystemTime::now(),
           components: vec![
               ComponentHealth { name: "raft", status: "healthy" },
               ComponentHealth { name: "metadata_store", status: "healthy" },
               ComponentHealth { name: "file_store", status: "healthy" },
               ComponentHealth { name: "network", status: "healthy" },
           ],
       };

       Json(health)
   }

   async fn handle_liveness() -> impl IntoResponse {
       // Simple liveness check - is the process running?
       (StatusCode::OK, "OK")
   }

   async fn handle_readiness(
       State(storage_node): State<Arc<StorageNode>>,
   ) -> impl IntoResponse {
       // Readiness check - is the node ready to serve requests?
       if storage_node.is_ready().await {
           (StatusCode::OK, "Ready")
       } else {
           (StatusCode::SERVICE_UNAVAILABLE, "Not Ready")
       }
   }
   ```

3. Integrate metrics into all components
   ```rust
   // Example: Adding metrics to FileStore
   impl FileStoreImpl {
       pub async fn write_stripe_distributed(
           &self,
           file_id: FileId,
           stripe_id: StripeId,
           data: Vec<u8>,
           policy: StoragePolicy,
       ) -> Result<StripeMetadata, Error> {
           let start = Instant::now();

           // ... existing implementation ...

           // Record metrics
           self.metrics.publish_counter(
               FILESTORE_CHUNK_WRITES,
               chunks.len() as f64,
               UnitType::Operations,
           )?;

           self.metrics.publish_histogram(
               FILESTORE_WRITE_LATENCY,
               start.elapsed().as_secs_f64(),
               UnitType::Seconds,
           )?;

           Ok(stripe_metadata)
       }
   }
   ```

**Deliverables**:
- Prometheus exporter
- HTTP metrics endpoint
- Health check endpoints
- Metrics integration across all components

---

### Days 3-4: WormValidator & Integration Testing (Steps 3-6)

#### Step 3: Embedded Cluster Manager
**File**: `src/worm_validator/cluster_manager.rs`

**Tasks**:
1. Implement cluster bootstrapping
   ```rust
   pub struct ClusterManager {
       config: ClusterConfig,
       nodes: Vec<TestNode>,
       temp_dir: PathBuf,
   }

   pub struct TestNode {
       node_id: NodeId,
       process: Child,
       data_dir: PathBuf,
       listen_addr: SocketAddr,
       raft_addr: SocketAddr,
   }

   impl ClusterManager {
       pub async fn new(config: ClusterConfig) -> Result<Self, Error> {
           let temp_dir = tempfile::tempdir()?.into_path();

           Ok(Self {
               config,
               nodes: Vec::new(),
               temp_dir,
           })
       }

       pub async fn start_cluster(&mut self, node_count: usize) -> Result<(), Error> {
           info!("Starting test cluster with {} nodes", node_count);

           // Start nodes sequentially
           for i in 0..node_count {
               let node = self.start_node(i).await?;
               self.nodes.push(node);

               // Wait for node to be ready
               tokio::time::sleep(Duration::from_secs(2)).await;
           }

           // Initialize Raft cluster
           self.initialize_raft_cluster().await?;

           // Wait for leader election
           self.wait_for_leader().await?;

           info!("Cluster started successfully");

           Ok(())
       }

       async fn start_node(&self, index: usize) -> Result<TestNode, Error> {
           let node_id = NodeId::new(index as u64);
           let data_dir = self.temp_dir.join(format!("node_{}", index));
           fs::create_dir_all(&data_dir).await?;

           let listen_port = 8000 + index;
           let raft_port = 9000 + index;

           let listen_addr: SocketAddr = format!("127.0.0.1:{}", listen_port).parse()?;
           let raft_addr: SocketAddr = format!("127.0.0.1:{}", raft_port).parse()?;

           // Create node configuration
           let config = self.create_node_config(node_id, &data_dir, listen_addr, raft_addr);
           let config_path = data_dir.join("config.toml");
           fs::write(&config_path, toml::to_string(&config)?).await?;

           // Start storage node process
           let process = Command::new("./target/debug/wormfs-storage-node")
               .arg("--config")
               .arg(&config_path)
               .stdout(Stdio::piped())
               .stderr(Stdio::piped())
               .spawn()?;

           Ok(TestNode {
               node_id,
               process,
               data_dir,
               listen_addr,
               raft_addr,
           })
       }

       async fn initialize_raft_cluster(&self) -> Result<(), Error> {
           // Connect to first node and initialize cluster
           let leader = &self.nodes[0];
           let mut client = self.get_admin_client(leader.listen_addr).await?;

           let member_addrs: Vec<_> = self.nodes.iter()
               .map(|n| (n.node_id, n.raft_addr))
               .collect();

           client.initialize_cluster(InitializeClusterRequest {
               members: member_addrs,
           }).await?;

           Ok(())
       }

       async fn wait_for_leader(&self) -> Result<NodeId, Error> {
           for _ in 0..30 {
               for node in &self.nodes {
                   let mut client = self.get_admin_client(node.listen_addr).await?;
                   let status = client.get_cluster_status(GetClusterStatusRequest {}).await?;

                   if let Some(leader_id) = status.into_inner().leader_id {
                       return Ok(NodeId::new(leader_id));
                   }
               }

               tokio::time::sleep(Duration::from_secs(1)).await;
           }

           Err(Error::NoLeaderElected)
       }

       pub async fn stop_cluster(&mut self) -> Result<(), Error> {
           for node in &mut self.nodes {
               node.process.kill().await?;
           }

           // Clean up temp directory
           if !self.config.keep_data {
               fs::remove_dir_all(&self.temp_dir).await?;
           }

           Ok(())
       }
   }
   ```

**Deliverables**:
- Cluster bootstrapping
- Multi-node test cluster
- Automatic cleanup
- Process management

#### Step 4: FUSE Client Simulator
**File**: `src/worm_validator/client_simulator.rs`

**Tasks**:
1. Implement gRPC client for filesystem operations
   ```rust
   pub struct FuseClientSimulator {
       client: FilesystemServiceClient<Channel>,
       current_dir: PathBuf,
   }

   impl FuseClientSimulator {
       pub async fn connect(addr: SocketAddr) -> Result<Self, Error> {
           let endpoint = format!("http://{}", addr);
           let client = FilesystemServiceClient::connect(endpoint).await?;

           Ok(Self {
               client,
               current_dir: PathBuf::from("/"),
           })
       }

       pub async fn create_file(&mut self, path: &str, mode: u32) -> Result<FileHandle, Error> {
           let request = CreateFileRequest {
               path: path.to_string(),
               mode,
               uid: 1000,
               gid: 1000,
           };

           let response = self.client.create_file(request).await?;
           let resp = response.into_inner();

           Ok(FileHandle {
               inode: resp.inode,
               file_id: resp.file_id,
           })
       }

       pub async fn write_file(
           &mut self,
           inode: u64,
           offset: u64,
           data: &[u8],
       ) -> Result<usize, Error> {
           let request = WriteFileRequest {
               inode,
               offset,
               data: data.to_vec(),
               flags: 0,
           };

           let response = self.client.write_file(request).await?;
           Ok(response.into_inner().bytes_written as usize)
       }

       pub async fn read_file(
           &mut self,
           inode: u64,
           offset: u64,
           size: usize,
       ) -> Result<Vec<u8>, Error> {
           let request = ReadFileRequest {
               inode,
               offset,
               size: size as u64,
           };

           let mut stream = self.client.read_file(request).await?.into_inner();
           let mut data = Vec::new();

           while let Some(chunk) = stream.next().await {
               data.extend_from_slice(&chunk?.data);
           }

           Ok(data)
       }

       pub async fn mkdir(&mut self, path: &str, mode: u32) -> Result<(), Error> {
           let request = CreateDirectoryRequest {
               path: path.to_string(),
               mode,
               uid: 1000,
               gid: 1000,
           };

           self.client.create_directory(request).await?;
           Ok(())
       }

       pub async fn list_directory(&mut self, path: &str) -> Result<Vec<DirEntry>, Error> {
           let request = ListDirectoryRequest {
               path: path.to_string(),
           };

           let response = self.client.list_directory(request).await?;
           Ok(response.into_inner().entries)
       }
   }
   ```

**Deliverables**:
- FUSE client simulator
- File operation wrappers
- Directory operation wrappers
- Error handling

#### Step 5: Test Scenarios
**File**: `src/worm_validator/scenarios/`

**Tasks**:
1. Implement basic file operation scenarios
   ```rust
   // scenarios/basic_operations.rs
   pub async fn test_create_and_write(client: &mut FuseClientSimulator) -> Result<(), Error> {
       // Create file
       let file = client.create_file("/test.txt", 0o644).await?;

       // Write data
       let data = b"Hello, WormFS!";
       let written = client.write_file(file.inode, 0, data).await?;
       assert_eq!(written, data.len());

       // Read back
       let read_data = client.read_file(file.inode, 0, data.len()).await?;
       assert_eq!(read_data, data);

       Ok(())
   }

   pub async fn test_large_file(client: &mut FuseClientSimulator) -> Result<(), Error> {
       let file = client.create_file("/large.dat", 0o644).await?;

       // Write 100MB
       let chunk_size = 1024 * 1024; // 1MB
       let chunks = 100;

       for i in 0..chunks {
           let data = vec![i as u8; chunk_size];
           let offset = i * chunk_size as u64;
           client.write_file(file.inode, offset, &data).await?;
       }

       // Verify random reads
       for i in 0..10 {
           let offset = (i * 10) * chunk_size as u64;
           let data = client.read_file(file.inode, offset, chunk_size).await?;
           assert_eq!(data, vec![(i * 10) as u8; chunk_size]);
       }

       Ok(())
   }
   ```

2. Implement distributed operation scenarios
   ```rust
   // scenarios/distributed.rs
   pub async fn test_node_failure_during_write(
       cluster: &mut ClusterManager,
       client: &mut FuseClientSimulator,
   ) -> Result<(), Error> {
       let file = client.create_file("/fail_test.dat", 0o644).await?;

       // Write some data
       let data = vec![0xAB; 10 * 1024 * 1024];
       client.write_file(file.inode, 0, &data).await?;

       // Kill a node
       let node_to_kill = cluster.nodes[1].node_id;
       cluster.stop_node(node_to_kill).await?;

       // Continue writing
       let more_data = vec![0xCD; 10 * 1024 * 1024];
       client.write_file(file.inode, data.len() as u64, &more_data).await?;

       // Verify read still works
       let read_data = client.read_file(file.inode, 0, data.len() + more_data.len()).await?;
       assert_eq!(&read_data[..data.len()], &data[..]);
       assert_eq!(&read_data[data.len()..], &more_data[..]);

       Ok(())
   }
   ```

3. Implement chaos testing scenarios
   ```rust
   // scenarios/chaos.rs
   pub async fn test_random_node_restarts(
       cluster: &mut ClusterManager,
       client: &mut FuseClientSimulator,
   ) -> Result<(), Error> {
       let duration = Duration::from_secs(60);
       let start = Instant::now();

       // Background task: random node restarts
       let cluster_handle = cluster.clone();
       let restart_task = tokio::spawn(async move {
           let mut rng = rand::thread_rng();
           while start.elapsed() < duration {
               let node_idx = rng.gen_range(0..cluster_handle.nodes.len());
               cluster_handle.restart_node(node_idx).await.unwrap();
               tokio::time::sleep(Duration::from_secs(5)).await;
           }
       });

       // Foreground task: continuous file operations
       let mut operation_count = 0;
       while start.elapsed() < duration {
           match client.create_file(&format!("/chaos_{}.dat", operation_count), 0o644).await {
               Ok(file) => {
                   let data = vec![operation_count as u8; 1024];
                   let _ = client.write_file(file.inode, 0, &data).await;
                   operation_count += 1;
               }
               Err(_) => {
                   // Expected during restarts
                   tokio::time::sleep(Duration::from_millis(100)).await;
               }
           }
       }

       restart_task.await?;

       info!("Completed {} operations during chaos test", operation_count);
       assert!(operation_count > 100, "Should complete significant operations");

       Ok(())
   }
   ```

**Deliverables**:
- Basic operation tests
- Large file tests
- Distributed failure tests
- Chaos testing scenarios

#### Step 6: Test Runner & Reporting
**File**: `src/worm_validator/scenario_runner.rs`

**Tasks**:
1. Implement test orchestration
   ```rust
   pub struct ScenarioRunner {
       cluster: ClusterManager,
       client: Option<FuseClientSimulator>,
       scenarios: Vec<Box<dyn TestScenario>>,
   }

   #[async_trait]
   pub trait TestScenario: Send + Sync {
       fn name(&self) -> &str;
       fn description(&self) -> &str;
       async fn run(
           &self,
           cluster: &mut ClusterManager,
           client: &mut FuseClientSimulator,
       ) -> Result<(), Error>;
   }

   impl ScenarioRunner {
       pub async fn run_all(&mut self) -> TestResults {
           let mut results = TestResults {
               total: self.scenarios.len(),
               passed: 0,
               failed: 0,
               skipped: 0,
               scenario_results: Vec::new(),
           };

           for scenario in &self.scenarios {
               info!("Running scenario: {}", scenario.name());

               let start = Instant::now();
               let result = scenario.run(&mut self.cluster, self.client.as_mut().unwrap()).await;
               let duration = start.elapsed();

               let scenario_result = match result {
                   Ok(()) => {
                       info!("✓ {} passed in {:?}", scenario.name(), duration);
                       results.passed += 1;
                       ScenarioResult {
                           name: scenario.name().to_string(),
                           status: TestStatus::Passed,
                           duration,
                           error: None,
                       }
                   }
                   Err(e) => {
                       error!("✗ {} failed: {}", scenario.name(), e);
                       results.failed += 1;
                       ScenarioResult {
                           name: scenario.name().to_string(),
                           status: TestStatus::Failed,
                           duration,
                           error: Some(format!("{}", e)),
                       }
                   }
               };

               results.scenario_results.push(scenario_result);
           }

           results
       }
   }
   ```

2. Implement result reporting
   ```rust
   // report.rs
   pub fn generate_report(results: &TestResults) -> String {
       let mut report = String::new();

       report.push_str(&format!("WormFS Validation Report\n"));
       report.push_str(&format!("========================\n\n"));
       report.push_str(&format!("Total scenarios: {}\n", results.total));
       report.push_str(&format!("Passed: {} ({}%)\n", results.passed,
           (results.passed as f64 / results.total as f64 * 100.0) as u32));
       report.push_str(&format!("Failed: {}\n", results.failed));
       report.push_str(&format!("Skipped: {}\n\n", results.skipped));

       report.push_str("Detailed Results:\n");
       report.push_str("-----------------\n\n");

       for result in &results.scenario_results {
           let status_symbol = match result.status {
               TestStatus::Passed => "✓",
               TestStatus::Failed => "✗",
               TestStatus::Skipped => "○",
           };

           report.push_str(&format!("{} {} ({:?})\n",
               status_symbol, result.name, result.duration));

           if let Some(error) = &result.error {
               report.push_str(&format!("  Error: {}\n", error));
           }
       }

       report
   }

   pub fn generate_html_report(results: &TestResults) -> String {
       // Generate HTML report with charts
       format!(r#"
       <!DOCTYPE html>
       <html>
       <head>
           <title>WormFS Validation Report</title>
           <style>
               body {{ font-family: Arial, sans-serif; margin: 20px; }}
               .passed {{ color: green; }}
               .failed {{ color: red; }}
               .summary {{ background: #f0f0f0; padding: 10px; margin: 10px 0; }}
           </style>
       </head>
       <body>
           <h1>WormFS Validation Report</h1>
           <div class="summary">
               <p>Total: {}</p>
               <p class="passed">Passed: {}</p>
               <p class="failed">Failed: {}</p>
           </div>
           <!-- scenario details -->
       </body>
       </html>
       "#, results.total, results.passed, results.failed)
   }
   ```

**Deliverables**:
- Test orchestration
- Parallel test execution
- Text and HTML reports
- CI integration

---

### Day 5: Production Readiness (Steps 7-8)

#### Step 7: Graceful Shutdown & Lifecycle Management
**File**: `src/storage_node/lifecycle.rs`

**Tasks**:
1. Implement graceful shutdown
   ```rust
   impl StorageNode {
       pub async fn start_with_shutdown(
           &self,
           shutdown_signal: tokio::sync::oneshot::Receiver<()>,
       ) -> Result<(), Error> {
           // Start all components
           self.start().await?;

           // Wait for shutdown signal
           shutdown_signal.await.ok();

           // Initiate graceful shutdown
           info!("Shutdown signal received, starting graceful shutdown");
           self.shutdown().await?;

           Ok(())
       }

       pub async fn shutdown(&self) -> Result<(), Error> {
           info!("Shutting down StorageNode");

           // 1. Stop accepting new requests
           self.storage_endpoint.stop_accepting_requests().await?;

           // 2. Wait for in-flight requests to complete (with timeout)
           tokio::select! {
               _ = self.storage_endpoint.wait_for_requests() => {
                   info!("All requests completed");
               }
               _ = tokio::time::sleep(Duration::from_secs(30)) => {
                   warn!("Shutdown timeout, forcing termination");
               }
           }

           // 3. Stop watchdog if running
           if self.is_leader().await {
               self.watchdog.stop().await?;
           }

           // 4. Stop Raft (flush any pending operations)
           self.raft_member.shutdown().await?;

           // 5. Flush metadata store
           self.metadata_store.flush().await?;

           // 6. Close network connections
           self.storage_network.shutdown().await?;

           info!("StorageNode shutdown complete");

           Ok(())
       }

       pub async fn is_healthy(&self) -> bool {
           // Check all component health
           self.metadata_store.is_healthy().await &&
           self.file_store.is_healthy().await &&
           self.raft_member.is_healthy().await &&
           self.storage_network.is_healthy().await
       }

       pub async fn is_ready(&self) -> bool {
           // Ready means able to serve requests
           self.is_healthy().await &&
           self.raft_member.has_leader().await
       }
   }
   ```

2. Add signal handling
   ```rust
   // In main binary
   #[tokio::main]
   async fn main() -> Result<(), Box<dyn std::error::Error>> {
       // Setup signal handlers
       let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

       tokio::spawn(async move {
           match tokio::signal::ctrl_c().await {
               Ok(()) => {
                   info!("Received Ctrl-C, initiating shutdown");
                   let _ = shutdown_tx.send(());
               }
               Err(e) => {
                   error!("Failed to listen for Ctrl-C: {}", e);
               }
           }
       });

       // Start node
       let config = load_config()?;
       let node = StorageNode::new(config).await?;
       node.start_with_shutdown(shutdown_rx).await?;

       Ok(())
   }
   ```

**Deliverables**:
- Graceful shutdown
- Component health checks
- Signal handling
- Startup/shutdown logging

#### Step 8: Performance Benchmarking & Documentation
**File**: `tests/benchmarks/performance.rs`

**Tasks**:
1. Implement performance benchmarks
   ```rust
   use criterion::{criterion_group, criterion_main, Criterion};

   fn bench_write_throughput(c: &mut Criterion) {
       let rt = tokio::runtime::Runtime::new().unwrap();

       c.bench_function("write_1mb_file", |b| {
           b.to_async(&rt).iter(|| async {
               let cluster = TestCluster::new(3).await;
               let client = WormFsClient::connect(cluster.nodes()).await.unwrap();

               let file = client.create_file("/bench.dat", 0o644).await.unwrap();
               let data = vec![0u8; 1024 * 1024];

               client.write_file(file.inode, 0, &data).await.unwrap();
           });
       });
   }

   fn bench_read_throughput(c: &mut Criterion) {
       // Similar to write benchmark
   }

   fn bench_metadata_operations(c: &mut Criterion) {
       let rt = tokio::runtime::Runtime::new().unwrap();

       c.bench_function("create_1000_files", |b| {
           b.to_async(&rt).iter(|| async {
               let cluster = TestCluster::new(3).await;
               let client = WormFsClient::connect(cluster.nodes()).await.unwrap();

               for i in 0..1000 {
                   client.create_file(&format!("/file_{}.txt", i), 0o644).await.unwrap();
               }
           });
       });
   }

   criterion_group!(benches, bench_write_throughput, bench_read_throughput, bench_metadata_operations);
   criterion_main!(benches);
   ```

2. Create production deployment documentation
   ```markdown
   # WormFS Production Deployment Guide

   ## System Requirements

   - OS: Linux (kernel 5.x+)
   - CPU: 4+ cores recommended
   - RAM: 8GB+ per node
   - Storage: SSD recommended for metadata, HDD acceptable for chunks
   - Network: 1Gbps+ between nodes

   ## Installation

   1. Build release binary:
      ```bash
      cargo build --release --features full
      ```

   2. Create system user:
      ```bash
      sudo useradd -r -s /bin/false wormfs
      ```

   3. Create directories:
      ```bash
      sudo mkdir -p /var/lib/wormfs/{metadata,chunks,snapshots,logs}
      sudo chown -R wormfs:wormfs /var/lib/wormfs
      ```

   ## Configuration

   Create `/etc/wormfs/config.toml`:
   ```toml
   [node]
   node_id = 1
   listen_addr = "0.0.0.0:8080"

   [metadata]
   path = "/var/lib/wormfs/metadata/metadata.db"

   [file_store]
   data_paths = ["/var/lib/wormfs/chunks"]
   stripe_size = 1048576
   data_shards = 4
   parity_shards = 2

   [raft]
   addr = "0.0.0.0:9090"
   peers = ["node1:9090", "node2:9090", "node3:9090"]

   [metrics]
   prometheus_port = 9100
   ```

   ## Monitoring

   - Prometheus metrics: `http://localhost:9100/metrics`
   - Health check: `http://localhost:8080/health`
   - Grafana dashboard: Import `grafana/wormfs-dashboard.json`

   ## Operations

   ### Starting the cluster

   1. Start first node (will become leader)
   2. Start remaining nodes
   3. Verify cluster formation: `curl localhost:8080/health`

   ### Adding a node

   ```bash
   curl -X POST localhost:8080/admin/add-node \
     -d '{"node_id": 4, "address": "node4:9090"}'
   ```

   ### Removing a node

   ```bash
   curl -X POST localhost:8080/admin/remove-node \
     -d '{"node_id": 4}'
   ```

   ## Troubleshooting

   See docs/troubleshooting.md
   ```

3. Create monitoring dashboards
   ```json
   // grafana/wormfs-dashboard.json
   {
     "dashboard": {
       "title": "WormFS Cluster",
       "panels": [
         {
           "title": "Write Throughput",
           "targets": [
             {
               "expr": "rate(filestore_chunk_writes_total[5m])"
             }
           ]
         },
         {
           "title": "Raft Proposal Latency",
           "targets": [
             {
               "expr": "histogram_quantile(0.99, rate(raft_proposal_latency_seconds_bucket[5m]))"
             }
           ]
         }
       ]
     }
   }
   ```

**Deliverables**:
- Performance benchmarks
- Production deployment guide
- Monitoring dashboards
- Troubleshooting documentation
- Phase 5 complete - system production-ready!

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Metrics Coverage | >95% | Percentage of operations with metrics |
| Health Endpoint Latency | <10ms | Time to respond to health checks |
| Test Coverage | 100% scenarios | All critical paths tested |
| Benchmark Performance | Document baseline | Write/read throughput benchmarks |
| Documentation Complete | 100% | All operational docs written |

## Dependencies

### External Crates:
- `prometheus` - Metrics exporter
- `axum` - HTTP server for metrics
- `criterion` - Benchmarking
- `tempfile` - Test cluster setup

## Integration Points

All components integrate with MetricService:
- **MetadataStore**: Query latency, operation counts
- **FileStore**: Chunk operations, disk usage
- **StorageRaftMember**: Proposals, elections, log size
- **StorageNetwork**: Bandwidth, connections
- **StorageWatchdog**: Check counts, repairs

## Production Checklist

- [ ] All metrics instrumented
- [ ] Health endpoints responding
- [ ] Prometheus exporter working
- [ ] Grafana dashboards created
- [ ] All test scenarios passing
- [ ] Performance benchmarks documented
- [ ] Deployment guide written
- [ ] Troubleshooting guide complete
- [ ] Graceful shutdown tested
- [ ] Signal handling verified

## Notes

- Focus on observability over features
- Document everything for operators
- Test failure scenarios extensively
- Benchmark before optimization
- System is now production-ready!
