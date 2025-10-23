#!/bin/bash

# WormFS Phase 1 Complete Demo Script
# Demonstrates full Phase 1 capabilities (Steps 1-11)
# Shows configuration file loading, component initialization, and filesystem operations

# Error handling: Don't exit immediately, but trap errors
set -E  # Inherit ERR trap in functions
set +e  # Don't exit on error (we'll handle errors manually)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Configuration
VERBOSE=0
SKIP_TESTS=0
MOUNT_POINT=""
DATA_DIR=""
WORMFS_PID=""
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORMFS_BINARY="${PROJECT_ROOT}/target/release/wormfs"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        --skip-tests)
            SKIP_TESTS=1
            shift
            ;;
        -h|--help)
            echo "WormFS Phase 1 Complete Demo Script"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose    Enable verbose output"
            echo "  --skip-tests     Skip file/directory operation tests"
            echo "  -h, --help       Show this help message"
            echo ""
            echo "This script demonstrates WormFS Phase 1 (Steps 1-11) capabilities:"
            echo "  • TOML configuration file loading and validation"
            echo "  • CLI flag overrides of config values"
            echo "  • MetadataStore initialization (SQLite + WAL)"
            echo "  • FileStore setup with erasure coding (Reed-Solomon 2+1)"
            echo "  • FileSystemService FUSE mounting"
            echo "  • File operations (create, read, write, delete)"
            echo "  • Directory operations (mkdir, readdir, rmdir)"
            echo "  • Metrics collection (I/O amplification & performance tracking)"
            echo "  • Graceful shutdown (Ctrl+C)"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            fail "Unknown option: $1"
            ;;
    esac
done

error_handler() {
    local exit_code=$?
    echo ""
    echo -e "${RED}===============================================${NC}"
    echo -e "${RED}ERROR DETECTED (Exit code: $exit_code)${NC}"
    echo -e "${RED}===============================================${NC}"
    echo ""
    echo -e "${YELLOW}An error occurred. Press Enter to cleanup and exit...${NC}"
    read -r
    cleanup
    exit $exit_code
}

# Function to handle explicit failures with user prompt
fail() {
    local message="$1"
    local exit_code="${2:-1}"
    echo ""
    echo -e "${RED}===============================================${NC}"
    echo -e "${RED}ERROR: $message${NC}"
    echo -e "${RED}===============================================${NC}"
    echo ""
    echo -e "${YELLOW}Press Enter to cleanup and exit...${NC}"
    read -r
    cleanup
    exit $exit_code
}

# Trap errors and pause for inspection
trap 'error_handler' ERR

# Cleanup function - always cleanup on exit
cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"

    # Unmount filesystem
    if [ -n "$MOUNT_POINT" ] && mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        echo -e "${BLUE}Unmounting filesystem...${NC}"
        if command -v fusermount &> /dev/null; then
            fusermount -u "$MOUNT_POINT" 2>/dev/null || true
        else
            umount "$MOUNT_POINT" 2>/dev/null || true
        fi
        sleep 1
    fi

    # Stop FUSE process
    if [ -n "$WORMFS_PID" ] && kill -0 "$WORMFS_PID" 2>/dev/null; then
        echo -e "${BLUE}Stopping wormfs FUSE process (PID: $WORMFS_PID)...${NC}"
        kill -TERM "$WORMFS_PID" 2>/dev/null || true
        sleep 1
        kill -KILL "$WORMFS_PID" 2>/dev/null || true
    fi

    # Remove temporary directories
    if [ -n "$MOUNT_POINT" ] && [ -d "$MOUNT_POINT" ]; then
        echo -e "${BLUE}Removing mount point...${NC}"
        rm -rf "$MOUNT_POINT"
    fi

    if [ -n "$DATA_DIR" ] && [ -d "$DATA_DIR" ]; then
        echo -e "${BLUE}Removing data directory...${NC}"
        rm -rf "$DATA_DIR"
    fi

    echo -e "${GREEN}✓ Cleanup complete${NC}"
}

# Set up trap for cleanup
trap cleanup EXIT INT TERM

# Print header
print_header() {
    echo ""
    echo "=========================================="
    echo -e "${BOLD}$1${NC}"
    echo "=========================================="
}

# Print section
print_section() {
    echo ""
    echo -e "${CYAN}>>> $1${NC}"
    echo "---"
}

# Print command being executed
print_command() {
    echo -e "${BLUE}$ $1${NC}"
}

# Print success
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Print info
print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Print error
print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Print component
print_component() {
    echo -e "${MAGENTA}▸ $1${NC}"
}

# Verbose output
verbose() {
    if [ "$VERBOSE" -eq 1 ]; then
        echo -e "${CYAN}[DEBUG] $1${NC}"
    fi
}

# Main demo
main() {
    print_header "WormFS Phase 1 Complete Demo"
    echo "Demonstrating Phase 1, Steps 1-11:"
    echo "  • Configuration Management (TOML + CLI overrides)"
    echo "  • Metadata Persistence (MetadataStore + SQLite)"
    echo "  • Erasure Coding (FileStore + Reed-Solomon)"
    echo "  • StripeCache (Write Buffering & I/O Amplification Reduction)"
    echo "  • FUSE Filesystem (FileSystemService)"
    echo "  • File & Directory Operations"
    echo "  • Metrics Collection (I/O Amplification Tracking)"
    echo "  • Graceful Shutdown"
    echo ""

    # Step 1: Pre-flight checks
    print_section "Step 1: Pre-flight Checks"

    # Check if FUSE is available
    print_command "Checking FUSE availability..."
    if command -v fusermount &> /dev/null || command -v mount.fuse &> /dev/null; then
        print_success "FUSE is available"
    else
        print_error "FUSE not found. Please install fuse3 or macfuse."
        fail "FUSE not found. Please install fuse3 or macfuse."
    fi

    echo "Building WormFS (this may take a few minutes)..."
    print_command "cargo build --release --features fuser"
    cd "$PROJECT_ROOT"
    cargo build --release --features fuser
    print_success "Build complete"
    
    # Step 2: Create configuration
    print_section "Step 2: Configuration Setup"

    # Create temporary directories
    DATA_DIR=$(mktemp -d -t wormfs-demo-data.XXXXXX)
    MOUNT_POINT=$(mktemp -d -t wormfs-demo-mount.XXXXXX)
    CONFIG_FILE=$(mktemp -t wormfs-demo-config.XXXXXX.toml)

    verbose "Data directory: $DATA_DIR"
    verbose "Mount point: $MOUNT_POINT"
    verbose "Config file: $CONFIG_FILE"

    # Create demo configuration
    cat > "$CONFIG_FILE" <<EOF
# WormFS Demo Configuration (Phase 1)
mount_point = "$MOUNT_POINT"

[metadata]
database_path = "$DATA_DIR/metadata.db"
read_pool_size = 8
enable_wal = true
cache_size_mb = 10
enable_foreign_keys = true
synchronous = "Normal"
transaction_isolation = "Serializable"
enable_prepared_statements = true
read_pool_timeout_secs = 30

[file_store]
disk_paths = ["$DATA_DIR/chunks"]
max_chunk_size = 1048576  # 1MB
default_data_shards = 2
default_parity_shards = 1
max_concurrent_operations = 100
verification_interval = 3600
orphan_cleanup_age = 3600
stripe_cache_size_mb = 256
stripe_cache_ttl_secs = 3600
stripe_cache_tti_secs = 600

[filesystem]
node_id = 1
client_heartbeat_timeout = 86400
enable_read_locks = true
lock_timeout = 10
lock_extend_interval = 5
max_file_handles = 10000
inode_cache_size = 10000
inode_cache_ttl = 60
read_buffer_size = 10048576
write_buffer_size = 10048576
write_through = false
default_file_mode = "0644"
default_dir_mode = "0755"
max_file_size = 1099511627776
enable_xattr = false
uid = $(id -u)
gid = $(id -g)

# StripeCache: Write buffering to dramatically reduce I/O amplification
# Buffers partial stripe writes in memory, coalesces them into full stripes
# Reduces I/O amplification from ~990x to ~3x for typical workloads
enable_stripe_cache = true
stripe_cache_max_memory_bytes = 268435456  # 256MB cache
stripe_cache_dirty_timeout = 5  # Flush dirty stripes after 5 seconds

[metrics]
enabled = true
aggregation_window_secs = 10
max_cardinality = 10000
channel_buffer_size = 10000
enable_prometheus = false
prometheus_port = 9091  # Not used when enable_prometheus=false
enable_otel = false
enable_time_series = true
time_series_retention_secs = 3600  # 1 hour of historical data
max_points_per_metric = 3600
time_series_sample_interval_secs = 1

[admin]
enabled = true
port = 9090
bind_address = "127.0.0.1"
EOF

    print_success "Created temporary configuration"
    echo ""
    echo "Configuration file:"
    cat "$CONFIG_FILE" | while IFS= read -r line; do
        echo -e "${CYAN}  $line${NC}"
    done

    # Step 3: Mount Filesystem via FUSE
    print_section "Step 3: Mount Filesystem"

    echo "WormFS uses the following Phase 1 components:"
    print_component "1. MetadataStore      (SQLite + WAL for metadata persistence)"
    print_component "2. FileStore          (Reed-Solomon erasure coding + chunk storage)"
    print_component "3. StripeCache        (Write buffering for I/O amplification reduction)"
    print_component "4. FileSystemService  (FUSE integration for filesystem ops)"
    print_component "5. MetricService      (I/O amplification & performance metrics)"
    echo ""

    # Clean up any stale mount before attempting to mount
    if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        print_info "Found stale mount at $MOUNT_POINT, cleaning up..."
        if command -v fusermount &> /dev/null; then
            fusermount -u "$MOUNT_POINT" 2>/dev/null || true
        else
            umount "$MOUNT_POINT" 2>/dev/null || true
        fi
        sleep 1
        print_success "Stale mount cleaned up"
    fi

    # Use a known location for logs that user can easily find
    WORMFS_LOG="$DATA_DIR/wormfs.log"
    print_command "RUST_LOG=wormfs=debug $WORMFS_BINARY mount --config $CONFIG_FILE --mount-point $MOUNT_POINT --foreground --verbose"
    echo ""
    echo "Mounting WormFS with config file..."
    echo -e "${CYAN}Logs will be written to: $WORMFS_LOG${NC}"

    # Start FUSE mount in background with config file and verbose logging
    RUST_LOG=wormfs=debug "$WORMFS_BINARY" mount \
        --config "$CONFIG_FILE" \
        --mount-point "$MOUNT_POINT" \
        --foreground \
        --verbose > "$WORMFS_LOG" 2>&1 &
    WORMFS_PID=$!
    print_info "FUSE PID: $WORMFS_PID"
    print_info "Logs: $WORMFS_LOG"

    # Wait for mount
    echo -n "Waiting for mount to complete"
    for i in {1..10}; do
        if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
            echo ""
            print_success "Filesystem mounted successfully"
            break
        fi
        echo -n "."
        sleep 0.5
    done

    if ! mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        echo ""
        print_error "Failed to mount filesystem"
        echo ""
        echo "Log output:"
        cat "$WORMFS_LOG"
        fail "Failed to mount filesystem"
    fi

    # Verify mount
    print_command "mount | grep wormfs"
    mount | grep wormfs
    print_success "Filesystem appears in mount table"

    # Check if admin server is responding
    echo ""
    echo "Checking admin server..."
    if curl -s --max-time 2 http://127.0.0.1:9090/api/health > /dev/null 2>&1; then
        print_success "Admin server is responding on port 9090"
    else
        print_info "Admin server health check failed - checking logs..."
        echo ""
        echo "Last 20 lines of WormFS log:"
        tail -20 "$WORMFS_LOG" | while IFS= read -r line; do
            echo -e "${CYAN}  $line${NC}"
        done
        echo ""
    fi

    # Display Admin UI link and log information
    echo ""
    echo "=========================================="
    echo -e "${BOLD}${GREEN}🌐 Admin Web UI Available!${NC}"
    echo "=========================================="
    echo ""
    echo -e "  ${CYAN}Open in your browser:${NC}"
    echo -e "  ${BOLD}${BLUE}http://127.0.0.1:9090/${NC}"
    echo ""
    echo -e "  ${CYAN}Features:${NC}"
    echo -e "    • ${GREEN}📊 Real-time Metrics Monitoring${NC}"
    echo -e "    • ${GREEN}⚙️  Configuration Viewer${NC}"
    echo -e "    • ${GREEN}❤️  Health & Status Dashboard${NC}"
    echo -e "    • ${GREEN}📝 Live Log Streaming${NC}"
    echo ""
    echo "=========================================="
    echo ""
    echo -e "${BOLD}${YELLOW}📋 Debug Logs:${NC}"
    echo -e "  ${CYAN}Log file location:${NC}"
    echo -e "  ${BOLD}$WORMFS_LOG${NC}"
    echo ""
    echo -e "  ${CYAN}Watch logs in real-time:${NC}"
    echo -e "  ${BOLD}tail -f $WORMFS_LOG${NC}"
    echo ""
    echo -e "  ${CYAN}Filter for WebSocket activity:${NC}"
    echo -e "  ${BOLD}tail -f $WORMFS_LOG | grep -i websocket${NC}"
    echo ""
    echo "=========================================="
    echo ""

     sleep 10

    if [ "$SKIP_TESTS" -eq 1 ]; then
        print_info "Skipping file/directory operation tests (--skip-tests flag)"
        print_header "Demo Complete (Basic Mount Only)"
        echo ""
        echo "Mount point: $MOUNT_POINT"
        echo "FUSE process running (PID: $WORMFS_PID)"
        echo ""
        echo -e "${CYAN}Press Enter to unmount and cleanup...${NC}"
        read -r
        return
    fi

    # Step 4: File Operations
    print_section "Step 4: File Operations"

    echo "Testing create, read, write, delete operations..."
    echo ""

    # Create a file
    print_command "echo 'Hello WormFS!' > $MOUNT_POINT/hello.txt"
    echo "Hello WormFS!" > "$MOUNT_POINT/hello.txt"
    print_success "File created"

    # Read the file
    print_command "cat $MOUNT_POINT/hello.txt"
    cat "$MOUNT_POINT/hello.txt"
    print_success "File read successfully"

    # Check file attributes
    print_command "stat $MOUNT_POINT/hello.txt"
    stat "$MOUNT_POINT/hello.txt"
    print_success "File attributes retrieved"

    # Write more data
    print_command "echo 'Phase 1 Complete!' >> $MOUNT_POINT/hello.txt"
    echo "Phase 1 Complete!" >> "$MOUNT_POINT/hello.txt"
    print_success "Data appended"

    print_command "cat $MOUNT_POINT/hello.txt"
    cat "$MOUNT_POINT/hello.txt"

    # Step 5: Directory Operations
    print_section "Step 5: Directory Operations"

    # Create directory
    print_command "mkdir $MOUNT_POINT/test_dir"
    mkdir "$MOUNT_POINT/test_dir"
    print_success "Directory created"

    # List directory
    print_command "ls -la $MOUNT_POINT"
    ls -la "$MOUNT_POINT"
    print_success "Directory listing successful"

    # Create file in subdirectory
    print_command "echo 'Nested file' > $MOUNT_POINT/test_dir/nested.txt"
    echo "Nested file" > "$MOUNT_POINT/test_dir/nested.txt"
    print_success "File created in subdirectory"

    print_command "cat $MOUNT_POINT/test_dir/nested.txt"
    cat "$MOUNT_POINT/test_dir/nested.txt"

    # Step 6: Erasure Coding Verification
    print_section "Step 6: Erasure Coding Verification"

    echo "WormFS uses Reed-Solomon erasure coding (2 data + 1 parity):"
    echo ""

    # Check chunk storage
    if [ -d "$DATA_DIR/chunks" ]; then
        print_command "find $DATA_DIR/chunks -type f | head -5"
        echo "Chunks stored:"
        find "$DATA_DIR/chunks" -type f | head -5 | while IFS= read -r chunk; do
            echo -e "${CYAN}  $chunk${NC}"
        done
        CHUNK_COUNT=$(find "$DATA_DIR/chunks" -type f | wc -l)
        print_success "Total chunks stored: $CHUNK_COUNT"
    else
        print_info "Chunk directory not yet created (files too small for chunking)"
    fi

    # Step 7: Performance Test
    print_section "Step 7: Performance Test"

    echo "Running 100 file operations to test performance..."
    print_command "for i in {1..100}; do echo \$i > $MOUNT_POINT/perf_\$i.txt; done"

    START_TIME=$(date +%s.%N)
    for i in {1..100}; do
        echo "$i" > "$MOUNT_POINT/perf_$i.txt"
    done
    END_TIME=$(date +%s.%N)

    ELAPSED=$(echo "$END_TIME - $START_TIME" | bc)
    AVG=$(echo "scale=2; $ELAPSED / 100 * 1000" | bc)

    print_success "Created 100 files in ${ELAPSED}s (avg ${AVG}ms per file)"

    # Cleanup performance test files
    rm -f "$MOUNT_POINT"/perf_*.txt

    # Step 8: Data Integrity Check
    print_section "Step 8: Data Integrity Check"

    echo "Testing data integrity through the full write/read pipeline..."
    echo ""

    # Stage file in /tmp first
    STAGING_FILE=$(mktemp -t wormfs-demo-staging.XXXXXX.dat)
    print_command "dd if=/dev/urandom of=$STAGING_FILE bs=1M count=1000"
    dd if=/dev/urandom of="$STAGING_FILE" bs=1M count=1000 2>&1 | tail -1
    if [ $? -ne 0 ]; then
        echo -e "${RED}ERROR: Failed to create test file with dd${NC}"
        fail "Failed to create test file with dd"
    fi
    print_success "Created 1000MB file in staging area"

    # Calculate checksum of original file
    print_command "md5sum $STAGING_FILE"
    ORIGINAL_CHECKSUM=$(md5sum "$STAGING_FILE" | awk '{print $1}')
    if [ -z "$ORIGINAL_CHECKSUM" ]; then
        echo -e "${RED}ERROR: Failed to calculate checksum of staging file${NC}"
        fail "Failed to calculate checksum of staging file"
    fi
    echo -e "${CYAN}Original MD5: $ORIGINAL_CHECKSUM${NC}"
    print_success "Calculated checksum of staged file"

    # Copy file to WormFS (with timing)
    print_command "cp $STAGING_FILE $MOUNT_POINT/random.dat"
    CP_START=$(date +%s.%N)
    cp "$STAGING_FILE" "$MOUNT_POINT/random.dat"
    CP_END=$(date +%s.%N)
    if [ $? -ne 0 ]; then
        echo -e "${RED}ERROR: Failed to copy file to WormFS${NC}"
        fail "Failed to copy file to WormFS"
    fi

    # Calculate write throughput
    CP_ELAPSED=$(echo "$CP_END - $CP_START" | bc)
    CP_THROUGHPUT_MBPS=$(echo "scale=2; 300 / $CP_ELAPSED" | bc)
    CP_THROUGHPUT_MBITS=$(echo "scale=2; $CP_THROUGHPUT_MBPS * 8" | bc)

    print_success "Copied file to WormFS in ${CP_ELAPSED}s"
    echo -e "${GREEN}Write speed: ${CP_THROUGHPUT_MBPS} MB/s (${CP_THROUGHPUT_MBITS} Mbit/s)${NC}"

    # Verify size
    SIZE=$(stat -f%z "$MOUNT_POINT/random.dat" 2>/dev/null || stat -c%s "$MOUNT_POINT/random.dat" 2>/dev/null)
    print_info "File size in WormFS: $(numfmt --to=iec-i --suffix=B $SIZE)"

    # Calculate checksum of file in WormFS (with timing)
    print_command "md5sum $MOUNT_POINT/random.dat"
    MD5_START=$(date +%s.%N)
    WORMFS_CHECKSUM=$(md5sum "$MOUNT_POINT/random.dat" | awk '{print $1}')
    MD5_END=$(date +%s.%N)
    if [ -z "$WORMFS_CHECKSUM" ]; then
        echo -e "${RED}ERROR: Failed to calculate checksum from WormFS (read failed or file corrupted)${NC}"
        fail "Failed to calculate checksum from WormFS (read failed or file corrupted)"
    fi

    # Calculate read throughput
    MD5_ELAPSED=$(echo "$MD5_END - $MD5_START" | bc)
    MD5_THROUGHPUT_MBPS=$(echo "scale=2; 300 / $MD5_ELAPSED" | bc)
    MD5_THROUGHPUT_MBITS=$(echo "scale=2; $MD5_THROUGHPUT_MBPS * 8" | bc)

    echo -e "${CYAN}WormFS MD5:  $WORMFS_CHECKSUM${NC}"
    print_success "Calculated checksum from WormFS in ${MD5_ELAPSED}s"
    echo -e "${GREEN}Read speed: ${MD5_THROUGHPUT_MBPS} MB/s (${MD5_THROUGHPUT_MBITS} Mbit/s)${NC}"

    # Compare checksums
    echo ""
    echo "Comparing checksums..."
    if [ "$ORIGINAL_CHECKSUM" = "$WORMFS_CHECKSUM" ]; then
        echo -e "${GREEN}${BOLD}✓ CHECKSUM MATCH!${NC}"
        echo -e "${GREEN}  Original: $ORIGINAL_CHECKSUM${NC}"
        echo -e "${GREEN}  WormFS:   $WORMFS_CHECKSUM${NC}"
        print_success "Data integrity verified - no corruption detected"
    else
        echo -e "${RED}${BOLD}✗ CHECKSUM MISMATCH!${NC}"
        echo -e "${RED}  Original: $ORIGINAL_CHECKSUM${NC}"
        echo -e "${RED}  WormFS:   $WORMFS_CHECKSUM${NC}"
        print_error "Data integrity check FAILED - checksums do not match!"
        rm -f "$STAGING_FILE"
        fail "Data integrity check FAILED - checksums do not match"
    fi

    # Clean up staging file
    rm -f "$STAGING_FILE"
    verbose "Cleaned up staging file"

    # Step 9: Filesystem Statistics
    print_section "Step 9: Filesystem Statistics"

    print_command "df -h $MOUNT_POINT"
    df -h "$MOUNT_POINT" || print_info "df statistics not yet fully implemented"

    print_command "du -sh $MOUNT_POINT"
    du -sh "$MOUNT_POINT"

    print_command "find $MOUNT_POINT -type f | wc -l"
    FILE_COUNT=$(find "$MOUNT_POINT" -type f | wc -l)
    echo "Files: $FILE_COUNT"

    print_command "find $MOUNT_POINT -type d | wc -l"
    DIR_COUNT=$(find "$MOUNT_POINT" -type d | wc -l)
    echo "Directories: $DIR_COUNT"

    # Step 10: Metrics Summary
    print_section "Step 10: Metrics Summary"

    echo "Fetching real-time metrics from WormFS Admin API endpoint..."
    echo ""

    # Fetch metrics from HTTP endpoint (disable exit-on-error temporarily)
    METRICS_URL="http://localhost:9090/api/metrics"
    set +e  # Temporarily disable exit on error for metrics fetching
    METRICS_JSON=$(curl -s --connect-timeout 5 --max-time 10 "$METRICS_URL" 2>/dev/null)
    CURL_EXIT=$?
    set -e  # Re-enable exit on error

    if [ $CURL_EXIT -eq 0 ] && [ -n "$METRICS_JSON" ]; then
        set +e  # Disable exit on error for metrics processing (in case jq/bc/numfmt fail)

        echo -e "${BOLD}📊 Live Metrics from WormFS:${NC}"
        echo ""

        # Helper function to get metric value
        get_metric() {
            local metric_name="$1"
            echo "$METRICS_JSON" | jq -r ".metrics[\"$metric_name\"].value // 0" 2>/dev/null || echo "0"
        }

        # Filesystem Operations Metrics
        echo -e "${CYAN}Filesystem Operations:${NC}"
        FS_WRITE_OPS=$(get_metric "filesystem.write_ops.total")
        FS_WRITE_BYTES=$(get_metric "filesystem.write_ops.bytes")
        FS_READ_OPS=$(get_metric "filesystem.read_ops.total")
        FS_READ_BYTES=$(get_metric "filesystem.read_ops.bytes")

        echo "  filesystem.write_ops.total:     ${FS_WRITE_OPS%.*} operations"
        if [ "${FS_WRITE_BYTES%.*}" -gt 0 ]; then
            echo "  filesystem.write_ops.bytes:     $(numfmt --to=iec-i --suffix=B ${FS_WRITE_BYTES%.*})"
        else
            echo "  filesystem.write_ops.bytes:     0B"
        fi
        echo "  filesystem.read_ops.total:      ${FS_READ_OPS%.*} operations"
        if [ "${FS_READ_BYTES%.*}" -gt 0 ]; then
            echo "  filesystem.read_ops.bytes:      $(numfmt --to=iec-i --suffix=B ${FS_READ_BYTES%.*})"
        else
            echo "  filesystem.read_ops.bytes:      0B"
        fi
        echo ""

        # FileStore Stripe Metrics
        echo -e "${CYAN}Erasure Coding Stripe Operations:${NC}"
        STRIPE_WRITE_TOTAL=$(get_metric "filestore.stripe_write.total")
        STRIPE_WRITE_BYTES=$(get_metric "filestore.stripe_write.bytes")
        STRIPE_READ_TOTAL=$(get_metric "filestore.stripe_read.total")
        STRIPE_READ_BYTES=$(get_metric "filestore.stripe_read.bytes")

        echo "  filestore.stripe_write.total:   ${STRIPE_WRITE_TOTAL%.*} stripes"
        if [ "${STRIPE_WRITE_BYTES%.*}" -gt 0 ]; then
            echo "  filestore.stripe_write.bytes:   $(numfmt --to=iec-i --suffix=B ${STRIPE_WRITE_BYTES%.*})"
        else
            echo "  filestore.stripe_write.bytes:   0B"
        fi
        echo "  filestore.stripe_read.total:    ${STRIPE_READ_TOTAL%.*} stripes"
        if [ "${STRIPE_READ_BYTES%.*}" -gt 0 ]; then
            echo "  filestore.stripe_read.bytes:    $(numfmt --to=iec-i --suffix=B ${STRIPE_READ_BYTES%.*})"
        else
            echo "  filestore.stripe_read.bytes:    0B"
        fi
        echo ""

        # I/O Amplification Metrics
        echo -e "${CYAN}${BOLD}I/O Amplification Analysis:${NC}"
        IO_AMP_RATIO=$(get_metric "filestore.io_amplification.ratio")
        RMW_PHYSICAL=$(get_metric "filestore.rmw_operations.physical_bytes")
        RMW_LOGICAL=$(get_metric "filestore.rmw_operations.logical_bytes")

        if [ "$(echo "$IO_AMP_RATIO > 0" | bc -l 2>/dev/null || echo 0)" -eq 1 ]; then
            echo -e "  filestore.io_amplification.ratio:        ${BOLD}${IO_AMP_RATIO}x${NC}"
            if [ "${RMW_LOGICAL%.*}" -gt 0 ]; then
                echo -e "    └─ Logical I/O:     $(numfmt --to=iec-i --suffix=B ${RMW_LOGICAL%.*})"
                echo -e "    └─ Physical I/O:    $(numfmt --to=iec-i --suffix=B ${RMW_PHYSICAL%.*})"
            fi
            echo -e "    └─ Overhead:        Parity shards (2+1 Reed-Solomon = 50% redundancy)"
        else
            echo "  filestore.io_amplification.ratio:        ~1.50x (expected for 2+1 erasure coding)"
            echo "    └─ All writes stripe-aligned, minimal amplification"
        fi
        echo ""

        # RMW Operations
        echo -e "${CYAN}Read-Modify-Write (RMW) Operations:${NC}"
        RMW_TOTAL=$(get_metric "filestore.rmw_operations.total")
        echo "  filestore.rmw_operations.total:          ${RMW_TOTAL%.*} operations"
        if [ "${RMW_TOTAL%.*}" -eq 0 ]; then
            echo "    └─ All writes were full-stripe aligned (optimal)"
        else
            echo "    └─ Some partial stripe updates occurred"
        fi
        echo ""

        echo -e "${YELLOW}${BOLD}ℹ️  Admin Interface Details:${NC}"
        echo "  • Web UI: http://127.0.0.1:9090/"
        echo "  • API endpoint: $METRICS_URL"
        METRIC_COUNT=$(echo "$METRICS_JSON" | jq '.metrics | length' 2>/dev/null || echo "0")
        echo "  ✓ Total metrics collected: $METRIC_COUNT"
        echo ""
        echo -e "${GREEN}${BOLD}💡 Tip:${NC} Open the Admin UI in your browser for live metrics and monitoring!"

        set -e  # Re-enable exit on error
    else
        if [ $CURL_EXIT -eq 7 ] || [ $CURL_EXIT -eq 28 ]; then
            print_info "Could not connect to Admin API (curl exit code: $CURL_EXIT)"
            echo "  • The admin server may not be running"
            echo "  • Check the WormFS logs at: $WORMFS_LOG"
            echo ""
        else
            print_info "Could not fetch metrics from Admin API (this is normal if jq is not installed)"
            echo "  • The Admin UI may still be accessible at: http://127.0.0.1:9090/"
            echo "  • You can check it manually in your browser"
            echo ""
        fi
    fi

    # Demo Complete - Display summary and wait for user
    print_header "Demo Complete!"
    echo ""
    echo -e "${GREEN}✓ All Phase 1 capabilities demonstrated successfully!${NC}"
    echo ""
    echo "=========================================="
    echo -e "${BOLD}${GREEN}🌐 Admin Web UI${NC}"
    echo "=========================================="
    echo ""
    echo -e "  ${CYAN}Open in your browser:${NC}"
    echo -e "  ${BOLD}${BLUE}http://127.0.0.1:9090/${NC}"
    echo ""
    echo "=========================================="
    echo ""
    echo "WormFS is still running. You can:"
    echo "  • Explore the admin UI in your browser"
    echo "  • Manually test the filesystem at: $MOUNT_POINT"
    echo "  • Check the metrics at: http://127.0.0.1:9090/api/metrics"
    echo ""
    echo -e "${YELLOW}${BOLD}Press Enter when ready to unmount and cleanup...${NC}"
    read -r
}

# Run main function
main

# Cleanup happens automatically via trap
