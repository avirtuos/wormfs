#!/bin/bash

# WormFS Validation Script
# Runs comprehensive quality checks: build, test, format, and clippy
# Fails if any command produces errors or warnings

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Track overall success
FAILED=0

# Track results and timings
STEP_NAMES=()
STEP_RESULTS=()
STEP_DURATIONS=()
TOTAL_START_TIME=$(date +%s)

echo "=========================================="
echo "WormFS Cargo Quality Validation"
echo "=========================================="
echo ""

# Function to run a command and check for failure
run_check() {
    local name="$1"
    local cmd="$2"
    local start_time=$(date +%s)

    echo -e "${BLUE}Running: $name${NC}"
    echo "Command: $cmd"
    echo "---"

    if eval "$cmd"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        STEP_NAMES+=("$name")
        STEP_RESULTS+=("PASS")
        STEP_DURATIONS+=("$duration")
        echo -e "${GREEN}✓ $name passed (${duration}s)${NC}"
        echo ""
        return 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        STEP_NAMES+=("$name")
        STEP_RESULTS+=("FAIL")
        STEP_DURATIONS+=("$duration")
        echo -e "${RED}✗ $name failed (${duration}s)${NC}"
        echo ""
        FAILED=1
        return 1
    fi
}

# 1. Cargo Build - Check for errors and warnings
echo -e "${YELLOW}Step 1/7: Building project...${NC}"
run_check "Cargo Build" "cargo build 2>&1 | tee /tmp/wormfs_build.log && ! grep 'error:' /tmp/wormfs_build.log"

# 2. Cargo Test - Run all tests (hide successful test output)
echo -e "${YELLOW}Step 2/7: Running tests...${NC}"
run_check "Cargo Test" "cargo test 2>&1 | grep -v ' ... ok$' | grep -v '^$'"

# 3. Cargo Test (Integration Tests) - Run integration tests with test-utils feature
echo -e "${YELLOW}Step 3/7: Running integration tests...${NC}"
run_check "Cargo Integration Tests" "cargo test --tests --features test-utils 2>&1 | grep -v ' ... ok$' | grep -v '^$'"

# 4. FUSE Integration Tests - Run ignored integration tests that mount filesystems
echo -e "${YELLOW}Step 4/7: Running FUSE integration tests (ignored)...${NC}"
run_check "FUSE Integration Tests" "./scripts/run_fuse_integration_tests.sh"

# 5. Cargo Check (test-utils feature) - Verify test utilities compile
echo -e "${YELLOW}Step 5/7: Checking test-utils feature...${NC}"
run_check "Cargo Check test-utils" "cargo check --features test-utils"

# 6. Cargo Fmt Check - Verify code formatting
echo -e "${YELLOW}Step 6/7: Checking code format...${NC}"
run_check "Cargo Format Check" "cargo fmt --all -- --check"

# 7. Cargo Clippy - Lint with warnings as errors
echo -e "${YELLOW}Step 7/7: Running clippy linter...${NC}"
run_check "Cargo Clippy" "cargo clippy --all-targets --all-features -- -D warnings"

# Print summary table function
print_summary_table() {
    local total_end_time=$(date +%s)
    local total_duration=$((total_end_time - TOTAL_START_TIME))

    echo ""
    echo "=========================================="
    echo "Validation Summary"
    echo "=========================================="
    printf "%-28s %-10s %8s\n" "Step" "Status" "Duration"
    echo "------------------------------------------------------------"

    for i in "${!STEP_NAMES[@]}"; do
        local name="${STEP_NAMES[$i]}"
        local result="${STEP_RESULTS[$i]}"
        local duration="${STEP_DURATIONS[$i]}"

        if [ "$result" = "PASS" ]; then
            local status="${GREEN}✓ PASS${NC}"
        else
            local status="${RED}✗ FAIL${NC}"
        fi

        printf "%-28s " "$name"
        echo -e "$status      ${duration}s"
    done

    echo "------------------------------------------------------------"
    printf "%-28s %-10s %8s\n" "Total" "" "${total_duration}s"
    echo ""
}

# Print summary table
print_summary_table

# Final result
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All quality checks passed!${NC}"
    echo ""
    exit 0
else
    echo -e "${RED}✗ Some quality checks failed!${NC}"
    echo "Please review the output above and fix any issues."
    echo ""
    exit 1
fi
