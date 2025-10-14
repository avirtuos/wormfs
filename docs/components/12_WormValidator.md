# WormValidator Component Design

## Purpose & Responsibilities

WormValidator is a standalone binary that provides integration testing capabilities for WormFS by embedding a single-node storage cluster and acting as a simulated FUSE client. Its responsibilities include:

- Bootstrapping an embedded single-node storage cluster for testing
- Acting as a FUSE client simulator (without kernel FUSE module)
- Exercising all FilesystemService APIs through gRPC
- Validating end-to-end system behavior from client perspective
- Providing reproducible integration testing environment
- Generating detailed test reports for debugging
- Supporting progressive test scenario development
- Enabling manual testing without multi-node VM setup

## Architecture & Design

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│              WormValidator Binary                        │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌─────────────────────────────────────────────────┐   │
│  │     Embedded Storage Cluster                     │   │
│  │  ┌─────────────────────────────────────────┐   │   │
│  │  │         StorageNode                      │   │   │
│  │  │  - StorageRaftMember (single-node)      │   │   │
│  │  │  - FileStore (temp storage)             │   │   │
│  │  │  - MetadataStore (temp DB)              │   │   │
│  │  │  - StorageEndpoint (localhost gRPC)     │   │   │
│  │  │  - All other components                 │   │   │
│  │  └─────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────┘   │
│                         ↑                                │
│                         │ gRPC                           │
│                         ↓                                │
│  ┌─────────────────────────────────────────────────┐   │
│  │     FUSE Client Simulator                       │   │
│  │  - gRPC client to FilesystemService             │   │
│  │  - Simulates FUSE operations                    │   │
│  │  - Validates responses                          │   │
│  └─────────────────────────────────────────────────┘   │
│                         ↓                                │
│  ┌─────────────────────────────────────────────────┐   │
│  │     Test Scenario Runner                        │   │
│  │  - Orchestrates test scenarios                  │   │
│  │  - Collects metrics and results                 │   │
│  │  - Generates reports                            │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Component Breakdown

#### 1. ClusterManager
Responsible for bootstrapping and managing the embedded storage cluster:
- Initializes temporary directories for storage
- Configures single-node Raft cluster
- Starts StorageNode with all components
- Manages cluster lifecycle (startup/shutdown)
- Handles cleanup of temporary resources

#### 2. FuseClientSimulator
Acts as a gRPC client that mimics FUSE filesystem operations:
- Connects to localhost StorageEndpoint
- Implements FUSE-like operation wrappers
- Translates filesystem operations to gRPC calls
- Validates response correctness
- Maintains client-side state (open files, locks, etc.)

#### 3. TestScenarioRunner
Orchestrates execution of test scenarios:
- Loads and executes test scenarios
- Manages test dependencies and ordering
- Collects timing and performance metrics
- Handles test failures and retries
- Generates structured test results

#### 4. ValidationEngine
Verifies expected outcomes:
- Compares actual vs expected results
- Validates data integrity (checksums, content)
- Checks metadata consistency
- Verifies system state after operations
- Reports validation errors with context

#### 5. ReportGenerator
Creates detailed test reports:
- Summarizes test execution results
- Generates performance metrics
- Creates detailed failure diagnostics
- Supports multiple output formats (JSON, HTML, text)
- Includes timing breakdowns and resource usage

## Test Scenarios

### Category 1: Basic File Operations

**Scenario: Create-Read-Write-Delete**
1. Create a new file
2. Write data to file
3. Read data back and verify
4. Delete file
5. Verify file no longer exists

**Scenario: Large File Handling**
1. Create file and write 1GB of data
2. Verify stripe creation and distribution
3. Read entire file back
4. Verify data integrity with checksums

**Scenario: Concurrent File Access**
1. Create file
2. Simulate multiple concurrent reads
3. Verify all reads succeed
4. Verify data consistency across reads

### Category 2: Directory Operations

**Scenario: Directory Tree Creation**
1. Create nested directory structure
2. List directories at each level
3. Verify hierarchy integrity

**Scenario: Directory Listing**
1. Create directory with multiple files
2. List directory contents
3. Verify all entries present
4. Verify metadata accuracy

### Category 3: Metadata Operations

**Scenario: Metadata Updates**
1. Create file with initial metadata
2. Update permissions (chmod)
3. Update ownership (chown)
4. Verify metadata changes persist

**Scenario: File Stats**
1. Create file and write data
2. Get file stats (getattr)
3. Verify size, timestamps, permissions
4. Modify file and verify stat updates

### Category 4: Lock Operations

**Scenario: Exclusive Lock**
1. Create file
2. Acquire write lock
3. Attempt second lock (should fail)
4. Release lock
5. Verify second lock now succeeds

**Scenario: Shared Locks**
1. Create file
2. Acquire multiple read locks
3. Verify all succeed
4. Attempt write lock (should fail)
5. Release read locks
6. Verify write lock succeeds

**Scenario: Lock Expiration**
1. Acquire lock with short timeout
2. Wait for expiration
3. Verify lock automatically released
4. Acquire new lock successfully

### Category 5: Stripe Operations

**Scenario: Direct Stripe Read/Write**
1. Write data to specific stripe
2. Read stripe back
3. Verify data integrity
4. Test stripe-level erasure coding

**Scenario: Stripe Distribution**
1. Write large file
2. Verify stripe creation
3. Check chunk distribution across disks
4. Verify redundancy requirements met

### Category 6: Error Handling

**Scenario: Invalid Operations**
1. Attempt to read non-existent file
2. Attempt to delete open file
3. Attempt to write to read-locked file
4. Verify appropriate error responses

**Scenario: Resource Exhaustion**
1. Fill disk to capacity
2. Attempt write operations
3. Verify graceful failure
4. Cleanup and verify recovery

### Category 7: Consistency Validation

**Scenario: Metadata Consistency**
1. Perform various operations
2. Query metadata store directly
3. Verify consistency with filesystem view

**Scenario: Snapshot and Recovery**
1. Perform file operations
2. Trigger snapshot
3. Verify snapshot contents
4. Simulate recovery from snapshot

## Interfaces

### Public API (CLI)

```bash
# Run all test scenarios
wormfs-validator

# Run specific scenarios
wormfs-validator --scenarios basic,locks

# Use custom temp directory
wormfs-validator --temp-dir /tmp/wormfs-test

# Keep data after tests for inspection
wormfs-validator --keep-data

# Generate detailed report
wormfs-validator --report /tmp/report.html

# Verbose logging
wormfs-validator --verbose

# Run performance benchmarks
wormfs-validator --benchmark
```

### Rust Implementation

```rust
pub struct WormValidator {
    config: ValidatorConfig,
    cluster_manager: ClusterManager,
    client_simulator: FuseClientSimulator,
    scenario_runner: TestScenarioRunner,
}

impl WormValidator {
    pub fn new(config: ValidatorConfig) -> Result<Self, ValidatorError>;
    
    pub async fn run_all_tests(&mut self) -> TestResults;
    
    pub async fn run_scenarios(&mut self, scenarios: &[String]) -> TestResults;
    
    pub async fn cleanup(&mut self) -> Result<(), ValidatorError>;
}

pub struct ClusterManager {
    temp_dir: PathBuf,
    storage_node: Option<Arc<StorageNode>>,
    endpoint_address: SocketAddr,
}

impl ClusterManager {
    pub async fn start(&mut self) -> Result<(), ValidatorError>;
    
    pub async fn stop(&mut self) -> Result<(), ValidatorError>;
    
    pub fn endpoint_address(&self) -> SocketAddr;
}

pub struct FuseClientSimulator {
    grpc_client: FilesystemServiceClient<Channel>,
    open_files: HashMap<FileHandle, FileId>,
    locks: HashMap<LockId, FileId>,
}

impl FuseClientSimulator {
    pub async fn connect(endpoint: SocketAddr) -> Result<Self, ValidatorError>;
    
    // FUSE-like operations
    pub async fn create_file(&mut self, path: &str, mode: u32) -> Result<FileHandle, ValidatorError>;
    
    pub async fn read_file(&mut self, fh: FileHandle, offset: u64, size: u32) -> Result<Vec<u8>, ValidatorError>;
    
    pub async fn write_file(&mut self, fh: FileHandle, offset: u64, data: &[u8]) -> Result<u64, ValidatorError>;
    
    pub async fn delete_file(&mut self, fh: FileHandle) -> Result<(), ValidatorError>;
    
    pub async fn get_attr(&mut self, fh: FileHandle) -> Result<FileAttr, ValidatorError>;
    
    pub async fn set_attr(&mut self, fh: FileHandle, attr: FileAttr) -> Result<(), ValidatorError>;
    
    pub async fn mkdir(&mut self, path: &str, mode: u32) -> Result<(), ValidatorError>;
    
    pub async fn readdir(&mut self, dir: FileHandle) -> Result<Vec<DirEntry>, ValidatorError>;
    
    pub async fn acquire_lock(&mut self, fh: FileHandle, lock_type: LockType) -> Result<LockId, ValidatorError>;
    
    pub async fn release_lock(&mut self, lock_id: LockId) -> Result<(), ValidatorError>;
}

pub struct TestScenarioRunner {
    scenarios: Vec<Box<dyn TestScenario>>,
    results: Vec<ScenarioResult>,
}

impl TestScenarioRunner {
    pub fn load_scenarios(&mut self, filter: Option<&[String]>);
    
    pub async fn run_scenarios(&mut self, client: &mut FuseClientSimulator) -> TestResults;
}

pub trait TestScenario: Send + Sync {
    fn name(&self) -> &str;
    
    fn category(&self) -> &str;
    
    async fn execute(&self, client: &mut FuseClientSimulator) -> ScenarioResult;
}
```

### Data Structures

```rust
pub struct ValidatorConfig {
    pub temp_dir: PathBuf,
    pub verbose: bool,
    pub keep_data: bool,
    pub scenarios: Option<Vec<String>>,
    pub report_path: Option<PathBuf>,
    pub benchmark_mode: bool,
}

pub struct TestResults {
    pub total_scenarios: usize,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub duration: Duration,
    pub scenario_results: Vec<ScenarioResult>,
}

pub struct ScenarioResult {
    pub name: String,
    pub category: String,
    pub status: TestStatus,
    pub duration: Duration,
    pub error: Option<String>,
    pub metrics: HashMap<String, f64>,
}

pub enum TestStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Debug, thiserror::Error)]
pub enum ValidatorError {
    #[error("Cluster startup failed: {0}")]
    ClusterStartupFailed(String),
    
    #[error("Client connection failed: {0}")]
    ClientConnectionFailed(String),
    
    #[error("Test scenario failed: {0}")]
    TestScenarioFailed(String),
    
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}
```

## Dependencies

### Direct Dependencies
- **StorageNode**: Full embedded storage cluster
- **FilesystemService gRPC client**: For client simulation
- **tonic**: gRPC framework
- **tokio**: Async runtime

### External Dependencies
- `clap`: CLI argument parsing
- `serde`: Configuration serialization
- `tracing`: Logging framework
- `tempfile`: Temporary directory management
- `uuid`: Test data generation

## Configuration

### Default Configuration

```toml
[validator]
temp_dir = "/tmp/wormfs-validator"
verbose = false
keep_data = false
benchmark_mode = false

[validator.cluster]
raft_heartbeat_ms = 100
metadata_store_path = "{temp_dir}/metadata.db"
file_store_path = "{temp_dir}/filestore"
snapshot_store_path = "{temp_dir}/snapshots"
transaction_log_path = "{temp_dir}/txlog"

[validator.client]
endpoint = "127.0.0.1:7000"
timeout_secs = 30
max_retries = 3

[validator.scenarios]
# Enable/disable scenario categories
basic_file_ops = true
directory_ops = true
metadata_ops = true
lock_ops = true
stripe_ops = true
error_handling = true
consistency = true
```

## Testing Strategy

### Validator Self-Testing

While WormValidator is primarily a testing tool, it should have its own test suite:

**Unit Tests**
- Test scenario parsing and loading
- Test result aggregation logic
- Test report generation
- Test configuration parsing

**Integration Tests**
- Test that validator can start/stop cluster
- Test that client simulator can connect
- Test basic scenario execution
- Test cleanup functionality

## Usage Examples

### Basic Usage

```bash
# Run all tests
wormfs-validator

# Run with verbose output
wormfs-validator --verbose

# Keep test data for inspection
wormfs-validator --keep-data --temp-dir /tmp/wormfs-debug
```

### Selective Testing

```bash
# Run only basic file operation tests
wormfs-validator --scenarios basic

# Run multiple specific categories
wormfs-validator --scenarios basic,locks,metadata
```

### Development Workflow

```bash
# During feature development, run relevant tests
cargo build && wormfs-validator --scenarios stripe_ops --verbose

# After changes, run full test suite
cargo build && wormfs-validator --report /tmp/report.html

# Debug a specific failure
wormfs-validator --scenarios consistency --keep-data --verbose
```

### CI/CD Integration

```bash
# In CI pipeline
cargo build --release
./target/release/wormfs-validator --report /tmp/validator-report.json
if [ $? -ne 0 ]; then
    echo "WormValidator tests failed"
    exit 1
fi
```

## Open Questions

1. **Scenario Definition Format**: Should test scenarios be defined in code only, or should we support external scenario definitions (e.g., YAML/JSON)? Answer: Code only for now.

2. **Performance Benchmarking**: Should we include performance benchmarks as part of the standard test suite, or keep them separate? Answer: Not at the moment. We will handle benchmarks using native rush bench concepts.

3. **Multi-Node Testing**: Should we eventually support multi-node embedded clusters for more comprehensive testing? Answer: yes, we will eventually expand this to support multi-node.

4. **Failure Injection**: Should we add capabilities to inject failures (network, disk, etc.) for chaos testing? Answer: Not at the moment.

5. **Test Data Generation**: Should we include utilities for generating realistic test data (file trees, workload patterns)? Answer: Not at the moment.

6. **Continuous Validation**: Should validator support long-running modes that continuously exercise the system? Answer: Not at the moment.

7. **Scenario Prioritization**: Should scenarios have priority levels to enable quick smoke tests vs full validation? Answer: Not at the moment.

8. **Result Comparison**: Should we support comparing test results across runs to detect regressions? Answer: Not at the moment.
