#!/bin/bash

# WormFS Integration Test Runner
# Runs Phase 1 complete FUSE integration tests with automatic cleanup
# These tests mount actual FUSE filesystems and require proper permissions

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
BUILD_ONLY=0
KEEP_ARTIFACTS=0
SPECIFIC_TEST=""
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORMFS_BINARY="${PROJECT_ROOT}/target/release/wormfs"
TEST_FAILED=0
START_TIME=$(date +%s)

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        -b|--build-only)
            BUILD_ONLY=1
            shift
            ;;
        --keep-artifacts)
            KEEP_ARTIFACTS=1
            shift
            ;;
        -t|--test)
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        -h|--help)
            echo "WormFS Integration Test Runner"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose         Enable verbose test output"
            echo "  -b, --build-only      Build binary but don't run tests"
            echo "  -t, --test NAME       Run only the specified test"
            echo "  --keep-artifacts      Don't clean up temp directories (for debugging)"
            echo "  -h, --help            Show this help message"
            echo ""
            echo "This script runs FUSE integration tests from phase1_complete_test.rs"
            echo "These tests require:"
            echo "  • FUSE3 to be installed (libfuse3-dev)"
            echo "  • Permission to mount filesystems"
            echo "  • No other wormfs processes running"
            echo ""
            echo "Tests run sequentially to avoid mount point conflicts."
            echo ""
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Print header
print_header() {
    echo ""
    echo "=========================================="
    echo -e "${BOLD}$1${NC}"
    echo "=========================================="
}

# Print step
print_step() {
    echo ""
    echo -e "${CYAN}▶ $1${NC}"
}

# Print success
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Print error
print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Print warning
print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Comprehensive cleanup function
cleanup() {
    echo ""
    print_header "Cleanup"

    # Find and unmount any lingering FUSE mounts
    print_step "Checking for lingering FUSE mounts..."

    # Check for wormfs mounts in /tmp
    LINGERING_MOUNTS=$(mount | grep -E "wormfs|/tmp/.*mount" | awk '{print $3}' || true)

    if [ -n "$LINGERING_MOUNTS" ]; then
        print_warning "Found lingering mounts, attempting to unmount..."
        while IFS= read -r mount_point; do
            if [ -n "$mount_point" ]; then
                echo "  Unmounting: $mount_point"
                if command -v fusermount &> /dev/null; then
                    fusermount -u "$mount_point" 2>/dev/null || true
                elif command -v fusermount3 &> /dev/null; then
                    fusermount3 -u "$mount_point" 2>/dev/null || true
                else
                    umount "$mount_point" 2>/dev/null || true
                fi
                sleep 0.5
            fi
        done <<< "$LINGERING_MOUNTS"
        print_success "Attempted to unmount lingering mounts"
    else
        print_success "No lingering mounts found"
    fi

    # Kill any orphaned wormfs processes
    print_step "Checking for orphaned wormfs processes..."
    ORPHANED_PIDS=$(pgrep -f "target/release/wormfs|target/debug/wormfs" || true)

    if [ -n "$ORPHANED_PIDS" ]; then
        print_warning "Found orphaned wormfs processes, terminating..."
        echo "$ORPHANED_PIDS" | while read -r pid; do
            if [ -n "$pid" ]; then
                echo "  Killing PID: $pid"
                kill -TERM "$pid" 2>/dev/null || true
            fi
        done
        sleep 1

        # Force kill if still running
        STILL_RUNNING=$(pgrep -f "target/release/wormfs|target/debug/wormfs" || true)
        if [ -n "$STILL_RUNNING" ]; then
            echo "$STILL_RUNNING" | while read -r pid; do
                if [ -n "$pid" ]; then
                    echo "  Force killing PID: $pid"
                    kill -KILL "$pid" 2>/dev/null || true
                fi
            done
        fi
        print_success "Terminated orphaned processes"
    else
        print_success "No orphaned processes found"
    fi

    # Clean up temporary directories (unless --keep-artifacts)
    if [ $KEEP_ARTIFACTS -eq 0 ]; then
        print_step "Cleaning up temporary directories..."

        # Find and remove wormfs test directories in /tmp
        TEST_DIRS=$(find /tmp -maxdepth 1 -type d -name ".tmp*" -o -name "wormfs-test-*" 2>/dev/null || true)

        if [ -n "$TEST_DIRS" ]; then
            echo "$TEST_DIRS" | while read -r dir; do
                if [ -n "$dir" ] && [ -d "$dir" ]; then
                    echo "  Removing: $dir"
                    rm -rf "$dir" 2>/dev/null || true
                fi
            done
            print_success "Removed temporary directories"
        else
            print_success "No temporary directories to clean"
        fi
    else
        print_warning "Keeping artifacts for debugging (--keep-artifacts)"
    fi

    # Report final status
    echo ""
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))

    if [ $TEST_FAILED -eq 0 ]; then
        print_success "Cleanup complete (took ${DURATION}s)"
    else
        print_error "Cleanup complete (took ${DURATION}s)"
    fi
}

# Set up trap for cleanup
trap cleanup EXIT INT TERM

# Error handler
error_handler() {
    local exit_code=$?
    echo ""
    print_error "Error detected (Exit code: $exit_code)"
    TEST_FAILED=1
    # Cleanup will be called by EXIT trap
    exit $exit_code
}

trap 'error_handler' ERR

# Pre-flight checks
print_header "Pre-flight Checks"

# Check if FUSE is installed
print_step "Checking for FUSE..."
if command -v fusermount &> /dev/null || command -v fusermount3 &> /dev/null; then
    print_success "FUSE is installed"
else
    print_error "FUSE not found"
    echo ""
    echo "Please install FUSE3:"
    echo "  Ubuntu/Debian: sudo apt-get install fuse3 libfuse3-dev"
    echo "  macOS: brew install macfuse"
    echo ""
    exit 1
fi

# Check for required Rust toolchain
print_step "Checking Rust toolchain..."
if command -v cargo &> /dev/null; then
    RUST_VERSION=$(cargo --version)
    print_success "Rust found: $RUST_VERSION"
else
    print_error "Cargo not found - please install Rust"
    exit 1
fi

# Check if we're in the right directory
print_step "Checking project structure..."
if [ ! -f "$PROJECT_ROOT/Cargo.toml" ]; then
    print_error "Not in WormFS project root"
    exit 1
fi

if [ ! -f "$PROJECT_ROOT/tests/integration/phase1_complete_test.rs" ]; then
    print_error "Integration tests not found"
    exit 1
fi

print_success "Project structure verified"

# Build the binary
print_header "Building WormFS Binary"

print_step "Building release binary..."
cd "$PROJECT_ROOT"

if [ $VERBOSE -eq 1 ]; then
    cargo build --release
    BUILD_EXIT=$?
else
    cargo build --release 2>&1 | grep -E "Compiling|Finished|error|warning" || true
    BUILD_EXIT=${PIPESTATUS[0]}
fi

if [ $BUILD_EXIT -ne 0 ]; then
    print_error "Build failed"
    exit 1
fi

if [ ! -f "$WORMFS_BINARY" ]; then
    print_error "Binary not found at $WORMFS_BINARY"
    exit 1
fi

print_success "Binary built successfully: $WORMFS_BINARY"

# Exit if build-only mode
if [ $BUILD_ONLY -eq 1 ]; then
    print_header "Build Complete"
    print_success "Build-only mode: skipping tests"
    exit 0
fi

# Run integration tests
print_header "Running Integration Tests"

print_step "Preparing test environment..."

# Ensure no stale mounts before starting
EXISTING_MOUNTS=$(mount | grep -E "wormfs" || true)
if [ -n "$EXISTING_MOUNTS" ]; then
    print_warning "Found existing wormfs mounts - cleaning up first..."
    cleanup
    sleep 1
fi

print_success "Test environment ready"

# Build test command
TEST_CMD="cargo test --test fuse_integration_test"

if [ -n "$SPECIFIC_TEST" ]; then
    TEST_CMD="$TEST_CMD $SPECIFIC_TEST --exact"
    print_step "Running specific test: $SPECIFIC_TEST"
else
    print_step "Running all integration tests..."
fi

# Add flags
TEST_CMD="$TEST_CMD -- --ignored --test-threads=1 --nocapture"

if [ $VERBOSE -eq 0 ]; then
    TEST_CMD="$TEST_CMD --quiet"
fi

# Run tests
echo ""
echo "Command: $TEST_CMD"
echo ""

eval "$TEST_CMD"
TEST_EXIT=$?

# Check test results
echo ""
if [ $TEST_EXIT -eq 0 ]; then
    print_header "Test Results"
    print_success "All integration tests passed!"
    TEST_FAILED=0
else
    print_header "Test Results"
    print_error "Some integration tests failed (exit code: $TEST_EXIT)"
    TEST_FAILED=1
fi

# Exit with test exit code (cleanup will be called by EXIT trap)
exit $TEST_EXIT
