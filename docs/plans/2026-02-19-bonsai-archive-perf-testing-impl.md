# Bonsai Archive Performance Testing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create bash scripts to measure and compare Bonsai Archive vs regular Bonsai performance on Hoodi testnet.

**Architecture:** Two scripts - `perf-test.sh` collects metrics/RPC latency on a single node, `compare-results.sh` merges CSV outputs from both nodes. Uses curl for HTTP, jq for JSON parsing.

**Tech Stack:** Bash, curl, jq, awk

---

### Task 1: Create perf-test.sh Skeleton with Argument Parsing

**Files:**
- Create: `scripts/perf-test.sh`

**Step 1: Create the script with argument parsing**

```bash
#!/bin/bash
set -euo pipefail

# Default values
LABEL="bonsai"
RPC_PORT=8545
METRICS_PORT=9545
DURATION=300
INTERVAL=10
HISTORICAL_BLOCK=""
TEST_ADDRESS="0x00000000219ab540356cBB839Cbe05303d7705Fa"  # Deposit contract
TEST_SLOT="0x0"
OUTPUT_DIR="./results"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Collect performance metrics from a Besu node.

Options:
  --label NAME          Node identifier (default: bonsai)
  --rpc-port PORT       JSON-RPC port (default: 8545)
  --metrics-port PORT   Prometheus metrics port (default: 9545)
  --duration SECONDS    Collection duration (default: 300)
  --interval SECONDS    Polling interval (default: 10)
  --historical-block N  Block number for historical queries (default: current - 100000)
  --test-address ADDR   Address for balance/storage queries
  --test-slot SLOT      Storage slot for eth_getStorageAt (default: 0x0)
  --output-dir DIR      Output directory (default: ./results)
  -h, --help            Show this help message
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --label) LABEL="$2"; shift 2 ;;
        --rpc-port) RPC_PORT="$2"; shift 2 ;;
        --metrics-port) METRICS_PORT="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --interval) INTERVAL="$2"; shift 2 ;;
        --historical-block) HISTORICAL_BLOCK="$2"; shift 2 ;;
        --test-address) TEST_ADDRESS="$2"; shift 2 ;;
        --test-slot) TEST_SLOT="$2"; shift 2 ;;
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

RPC_URL="http://localhost:${RPC_PORT}"
METRICS_URL="http://localhost:${METRICS_PORT}/metrics"

echo "Performance Test Configuration:"
echo "  Label: $LABEL"
echo "  RPC URL: $RPC_URL"
echo "  Metrics URL: $METRICS_URL"
echo "  Duration: ${DURATION}s"
echo "  Interval: ${INTERVAL}s"
echo "  Output: $OUTPUT_DIR"
```

**Step 2: Make script executable and test help**

Run: `chmod +x scripts/perf-test.sh && ./scripts/perf-test.sh --help`

Expected: Shows usage message with all options

**Step 3: Commit**

```bash
git add scripts/perf-test.sh
git commit -m "feat: add perf-test.sh skeleton with argument parsing"
```

---

### Task 2: Add JSON-RPC Helper Functions

**Files:**
- Modify: `scripts/perf-test.sh`

**Step 1: Add RPC call function with timing**

Add after the configuration echo block:

```bash
# JSON-RPC helper - returns "latency_ms,success,error_code,result"
rpc_call() {
    local method="$1"
    local params="$2"
    local start_ms end_ms latency_ms response error_code result success

    start_ms=$(date +%s%3N)
    response=$(curl -s -X POST "$RPC_URL" \
        -H "Content-Type: application/json" \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"$method\",\"params\":$params,\"id\":1}" \
        2>/dev/null) || response=""
    end_ms=$(date +%s%3N)
    latency_ms=$((end_ms - start_ms))

    if [[ -z "$response" ]]; then
        echo "${latency_ms},false,connection_error,"
        return
    fi

    error_code=$(echo "$response" | jq -r '.error.code // empty')
    if [[ -n "$error_code" ]]; then
        echo "${latency_ms},false,${error_code},"
        return
    fi

    result=$(echo "$response" | jq -r '.result // empty')
    echo "${latency_ms},true,,${result}"
}

# Get current block number
get_block_number() {
    local response
    response=$(rpc_call "eth_blockNumber" "[]")
    local result
    result=$(echo "$response" | cut -d',' -f4)
    printf "%d" "$result"
}
```

**Step 2: Test RPC function works**

Run: `./scripts/perf-test.sh --label test 2>&1 | head -20`

Expected: Shows configuration (no errors about undefined functions)

**Step 3: Commit**

```bash
git add scripts/perf-test.sh
git commit -m "feat: add JSON-RPC helper functions with timing"
```

---

### Task 3: Add Setup and Verification

**Files:**
- Modify: `scripts/perf-test.sh`

**Step 1: Add setup function**

Add after the helper functions:

```bash
setup() {
    echo ""
    echo "=== Setup ==="

    # Check dependencies
    for cmd in curl jq awk bc; do
        if ! command -v "$cmd" &>/dev/null; then
            echo "ERROR: Required command '$cmd' not found"
            exit 1
        fi
    done
    echo "Dependencies OK"

    # Verify node is responding
    echo -n "Checking node connectivity... "
    local response
    response=$(rpc_call "eth_blockNumber" "[]")
    local success
    success=$(echo "$response" | cut -d',' -f2)
    if [[ "$success" != "true" ]]; then
        echo "FAILED"
        echo "ERROR: Cannot connect to node at $RPC_URL"
        exit 1
    fi
    echo "OK"

    # Get current block number
    CURRENT_BLOCK=$(get_block_number)
    echo "Current block: $CURRENT_BLOCK"

    # Set historical block if not specified
    if [[ -z "$HISTORICAL_BLOCK" ]]; then
        HISTORICAL_BLOCK=$((CURRENT_BLOCK - 100000))
        if [[ $HISTORICAL_BLOCK -lt 1 ]]; then
            HISTORICAL_BLOCK=1
        fi
    fi
    echo "Historical block for queries: $HISTORICAL_BLOCK"

    # Create output directory
    mkdir -p "$OUTPUT_DIR"
    METRICS_CSV="${OUTPUT_DIR}/${LABEL}_metrics.csv"
    RPC_CSV="${OUTPUT_DIR}/${LABEL}_rpc_latency.csv"
    SUMMARY_CSV="${OUTPUT_DIR}/${LABEL}_summary.csv"

    # Initialize CSV headers
    echo "timestamp,block_number,execution_time_ms,cpu_seconds,memory_bytes,gc_count,gc_time_seconds" > "$METRICS_CSV"
    echo "timestamp,method,block_param,latency_ms,success,error_code" > "$RPC_CSV"

    echo "Output files:"
    echo "  Metrics: $METRICS_CSV"
    echo "  RPC Latency: $RPC_CSV"
    echo "  Summary: $SUMMARY_CSV"
}
```

**Step 2: Call setup at the end of the script**

Add at the end:

```bash
setup
echo ""
echo "Setup complete. Ready to collect metrics."
```

**Step 3: Test setup runs**

Run: `./scripts/perf-test.sh --label test --output-dir /tmp/perf-test 2>&1`

Expected: Shows setup steps (will fail on node connectivity if no node running, that's OK)

**Step 4: Commit**

```bash
git add scripts/perf-test.sh
git commit -m "feat: add setup and verification for perf-test.sh"
```

---

### Task 4: Add Prometheus Metrics Collection

**Files:**
- Modify: `scripts/perf-test.sh`

**Step 1: Add metrics collection function**

Add after the setup function:

```bash
collect_metrics() {
    local timestamp block_number execution_time cpu_seconds memory_bytes gc_count gc_time
    local metrics_raw

    timestamp=$(date +%s)
    block_number=$(get_block_number)

    # Fetch Prometheus metrics
    metrics_raw=$(curl -s "$METRICS_URL" 2>/dev/null) || metrics_raw=""

    if [[ -z "$metrics_raw" ]]; then
        echo "WARN: Could not fetch metrics from $METRICS_URL"
        return
    fi

    # Parse metrics (use 0 if not found)
    execution_time=$(echo "$metrics_raw" | grep -E '^besu_block_processing_execution_time_head ' | awk '{print $2}' || echo "0")
    cpu_seconds=$(echo "$metrics_raw" | grep -E '^process_cpu_seconds_total ' | awk '{print $2}' || echo "0")
    memory_bytes=$(echo "$metrics_raw" | grep -E '^jvm_memory_used_bytes\{area="heap"' | awk '{print $2}' || echo "0")
    gc_count=$(echo "$metrics_raw" | grep -E '^jvm_gc_pause_seconds_count' | awk '{sum+=$2} END {print sum}' || echo "0")
    gc_time=$(echo "$metrics_raw" | grep -E '^jvm_gc_pause_seconds_sum' | awk '{sum+=$2} END {print sum}' || echo "0")

    # Default to 0 if empty
    execution_time=${execution_time:-0}
    cpu_seconds=${cpu_seconds:-0}
    memory_bytes=${memory_bytes:-0}
    gc_count=${gc_count:-0}
    gc_time=${gc_time:-0}

    echo "${timestamp},${block_number},${execution_time},${cpu_seconds},${memory_bytes},${gc_count},${gc_time}" >> "$METRICS_CSV"
}
```

**Step 2: Commit**

```bash
git add scripts/perf-test.sh
git commit -m "feat: add Prometheus metrics collection"
```

---

### Task 5: Add RPC Latency Collection

**Files:**
- Modify: `scripts/perf-test.sh`

**Step 1: Add RPC latency test function**

Add after collect_metrics:

```bash
collect_rpc_latency() {
    local timestamp method block_param response latency success error_code
    timestamp=$(date +%s)

    # Helper to record result
    record() {
        method="$1"
        block_param="$2"
        response="$3"
        latency=$(echo "$response" | cut -d',' -f1)
        success=$(echo "$response" | cut -d',' -f2)
        error_code=$(echo "$response" | cut -d',' -f3)
        echo "${timestamp},${method},${block_param},${latency},${success},${error_code}" >> "$RPC_CSV"
    }

    # Baseline
    record "eth_blockNumber" "n/a" "$(rpc_call "eth_blockNumber" "[]")"

    # Block retrieval
    record "eth_getBlockByNumber" "latest" "$(rpc_call "eth_getBlockByNumber" "[\"latest\", false]")"

    # Account balance - latest
    record "eth_getBalance" "latest" "$(rpc_call "eth_getBalance" "[\"$TEST_ADDRESS\", \"latest\"]")"

    # Account balance - historical
    local hist_hex
    hist_hex=$(printf "0x%x" "$HISTORICAL_BLOCK")
    record "eth_getBalance" "$HISTORICAL_BLOCK" "$(rpc_call "eth_getBalance" "[\"$TEST_ADDRESS\", \"$hist_hex\"]")"

    # Storage - latest
    record "eth_getStorageAt" "latest" "$(rpc_call "eth_getStorageAt" "[\"$TEST_ADDRESS\", \"$TEST_SLOT\", \"latest\"]")"

    # Storage - historical
    record "eth_getStorageAt" "$HISTORICAL_BLOCK" "$(rpc_call "eth_getStorageAt" "[\"$TEST_ADDRESS\", \"$TEST_SLOT\", \"$hist_hex\"]")"

    # eth_call - latest (simple call to get balance, no data needed for deposit contract)
    record "eth_call" "latest" "$(rpc_call "eth_call" "[{\"to\": \"$TEST_ADDRESS\"}, \"latest\"]")"

    # eth_call - historical
    record "eth_call" "$HISTORICAL_BLOCK" "$(rpc_call "eth_call" "[{\"to\": \"$TEST_ADDRESS\"}, \"$hist_hex\"]")"

    # Transaction receipt (use a known tx if available, otherwise skip)
    # This is optional - we just test the baseline latency
    record "eth_getTransactionReceipt" "n/a" "$(rpc_call "eth_getTransactionReceipt" "[\"0x0000000000000000000000000000000000000000000000000000000000000000\"]")"
}
```

**Step 2: Commit**

```bash
git add scripts/perf-test.sh
git commit -m "feat: add RPC latency collection"
```

---

### Task 6: Add Main Collection Loop

**Files:**
- Modify: `scripts/perf-test.sh`

**Step 1: Add main loop**

Add after collect_rpc_latency:

```bash
run_collection() {
    local end_time iterations
    end_time=$(($(date +%s) + DURATION))
    iterations=0

    echo ""
    echo "=== Starting Collection ==="
    echo "Will run for ${DURATION} seconds (until $(date -d @$end_time 2>/dev/null || date -r $end_time))"
    echo ""

    while [[ $(date +%s) -lt $end_time ]]; do
        iterations=$((iterations + 1))
        echo -n "Iteration $iterations at $(date '+%H:%M:%S')... "

        collect_metrics
        collect_rpc_latency

        echo "done"

        # Sleep until next interval
        local next_run=$(($(date +%s) + INTERVAL))
        while [[ $(date +%s) -lt $next_run ]] && [[ $(date +%s) -lt $end_time ]]; do
            sleep 1
        done
    done

    echo ""
    echo "Collection complete. $iterations iterations recorded."
}
```

**Step 2: Commit**

```bash
git add scripts/perf-test.sh
git commit -m "feat: add main collection loop"
```

---

### Task 7: Add Summary Generation

**Files:**
- Modify: `scripts/perf-test.sh`

**Step 1: Add summary function**

Add after run_collection:

```bash
generate_summary() {
    echo ""
    echo "=== Generating Summary ==="

    # Summary header
    echo "metric,count,min,max,avg,p50,p95,p99" > "$SUMMARY_CSV"

    # Summarize RPC latencies by method
    local methods
    methods=$(tail -n +2 "$RPC_CSV" | cut -d',' -f1 | sort -u)

    for method in $methods; do
        # Get latencies for successful calls only
        local latencies
        latencies=$(grep "^[0-9]*,${method}," "$RPC_CSV" | grep ",true," | cut -d',' -f4 | sort -n)

        if [[ -z "$latencies" ]]; then
            echo "${method},0,,,,,," >> "$SUMMARY_CSV"
            continue
        fi

        local count min max sum avg p50 p95 p99
        count=$(echo "$latencies" | wc -l | tr -d ' ')
        min=$(echo "$latencies" | head -1)
        max=$(echo "$latencies" | tail -1)
        sum=$(echo "$latencies" | awk '{sum+=$1} END {print sum}')
        avg=$(echo "scale=2; $sum / $count" | bc)

        # Percentiles
        p50_idx=$(echo "scale=0; $count * 0.50 / 1" | bc)
        p95_idx=$(echo "scale=0; $count * 0.95 / 1" | bc)
        p99_idx=$(echo "scale=0; $count * 0.99 / 1" | bc)

        p50=$(echo "$latencies" | sed -n "${p50_idx}p")
        p95=$(echo "$latencies" | sed -n "${p95_idx}p")
        p99=$(echo "$latencies" | sed -n "${p99_idx}p")

        # Handle edge cases
        [[ -z "$p50" ]] && p50=$max
        [[ -z "$p95" ]] && p95=$max
        [[ -z "$p99" ]] && p99=$max

        echo "${method},${count},${min},${max},${avg},${p50},${p95},${p99}" >> "$SUMMARY_CSV"
    done

    # Add block execution time summary from metrics
    local exec_times
    exec_times=$(tail -n +2 "$METRICS_CSV" | cut -d',' -f3 | grep -v "^0$" | sort -n)

    if [[ -n "$exec_times" ]]; then
        local count min max sum avg
        count=$(echo "$exec_times" | wc -l | tr -d ' ')
        min=$(echo "$exec_times" | head -1)
        max=$(echo "$exec_times" | tail -1)
        sum=$(echo "$exec_times" | awk '{sum+=$1} END {print sum}')
        avg=$(echo "scale=2; $sum / $count" | bc)
        echo "block_execution_time,${count},${min},${max},${avg},,," >> "$SUMMARY_CSV"
    fi

    echo "Summary written to $SUMMARY_CSV"
    echo ""
    echo "=== Results ==="
    column -t -s',' "$SUMMARY_CSV"
}
```

**Step 2: Update script end to run everything**

Replace the end of the script (after `setup` call) with:

```bash
setup
run_collection
generate_summary

echo ""
echo "Performance test complete!"
echo "Results saved to: $OUTPUT_DIR"
```

**Step 3: Commit**

```bash
git add scripts/perf-test.sh
git commit -m "feat: add summary generation for perf-test.sh"
```

---

### Task 8: Create compare-results.sh

**Files:**
- Create: `scripts/compare-results.sh`

**Step 1: Create the comparison script**

```bash
#!/bin/bash
set -euo pipefail

usage() {
    cat <<EOF
Usage: $(basename "$0") <bonsai_dir> <archive_dir>

Compare performance results from Bonsai vs Bonsai Archive tests.

Arguments:
  bonsai_dir   Directory containing bonsai_*.csv files
  archive_dir  Directory containing bonsai-archive_*.csv files

Output:
  comparison.csv in current directory
EOF
    exit 1
}

if [[ $# -ne 2 ]]; then
    usage
fi

BONSAI_DIR="$1"
ARCHIVE_DIR="$2"

# Find summary files
BONSAI_SUMMARY=$(find "$BONSAI_DIR" -name "*_summary.csv" | head -1)
ARCHIVE_SUMMARY=$(find "$ARCHIVE_DIR" -name "*_summary.csv" | head -1)

if [[ -z "$BONSAI_SUMMARY" ]]; then
    echo "ERROR: No summary CSV found in $BONSAI_DIR"
    exit 1
fi

if [[ -z "$ARCHIVE_SUMMARY" ]]; then
    echo "ERROR: No summary CSV found in $ARCHIVE_DIR"
    exit 1
fi

echo "Comparing:"
echo "  Bonsai: $BONSAI_SUMMARY"
echo "  Archive: $ARCHIVE_SUMMARY"
echo ""

# Create comparison
OUTPUT="comparison.csv"
echo "metric,bonsai_avg,archive_avg,delta_ms,delta_percent,bonsai_p95,archive_p95" > "$OUTPUT"

# Read bonsai metrics into associative array
declare -A BONSAI_AVG
declare -A BONSAI_P95
while IFS=',' read -r metric count min max avg p50 p95 p99; do
    [[ "$metric" == "metric" ]] && continue  # Skip header
    BONSAI_AVG["$metric"]="$avg"
    BONSAI_P95["$metric"]="$p95"
done < "$BONSAI_SUMMARY"

# Read archive and compare
while IFS=',' read -r metric count min max avg p50 p95 p99; do
    [[ "$metric" == "metric" ]] && continue  # Skip header

    bonsai_avg="${BONSAI_AVG[$metric]:-}"
    bonsai_p95="${BONSAI_P95[$metric]:-}"
    archive_avg="$avg"
    archive_p95="$p95"

    if [[ -z "$bonsai_avg" ]] || [[ -z "$archive_avg" ]]; then
        echo "${metric},${bonsai_avg:-n/a},${archive_avg},n/a,n/a,${bonsai_p95:-n/a},${archive_p95}" >> "$OUTPUT"
        continue
    fi

    # Calculate delta
    delta=$(echo "scale=2; $archive_avg - $bonsai_avg" | bc)

    # Calculate percentage (avoid divide by zero)
    if [[ "$bonsai_avg" != "0" ]] && [[ -n "$bonsai_avg" ]]; then
        percent=$(echo "scale=1; ($delta / $bonsai_avg) * 100" | bc)
    else
        percent="n/a"
    fi

    echo "${metric},${bonsai_avg},${archive_avg},${delta},${percent}%,${bonsai_p95},${archive_p95}" >> "$OUTPUT"
done < "$ARCHIVE_SUMMARY"

echo "=== Comparison Results ==="
column -t -s',' "$OUTPUT"
echo ""
echo "Results saved to: $OUTPUT"

# Show historical query capability difference
echo ""
echo "=== Historical Query Capability ==="
BONSAI_RPC=$(find "$BONSAI_DIR" -name "*_rpc_latency.csv" | head -1)
ARCHIVE_RPC=$(find "$ARCHIVE_DIR" -name "*_rpc_latency.csv" | head -1)

if [[ -n "$BONSAI_RPC" ]] && [[ -n "$ARCHIVE_RPC" ]]; then
    bonsai_hist_success=$(grep -E "eth_getBalance,[0-9]+," "$BONSAI_RPC" | grep ",true," | wc -l | tr -d ' ')
    bonsai_hist_fail=$(grep -E "eth_getBalance,[0-9]+," "$BONSAI_RPC" | grep ",false," | wc -l | tr -d ' ')
    archive_hist_success=$(grep -E "eth_getBalance,[0-9]+," "$ARCHIVE_RPC" | grep ",true," | wc -l | tr -d ' ')
    archive_hist_fail=$(grep -E "eth_getBalance,[0-9]+," "$ARCHIVE_RPC" | grep ",false," | wc -l | tr -d ' ')

    echo "Historical eth_getBalance queries:"
    echo "  Bonsai:  $bonsai_hist_success succeeded, $bonsai_hist_fail failed"
    echo "  Archive: $archive_hist_success succeeded, $archive_hist_fail failed"
fi
```

**Step 2: Make executable**

Run: `chmod +x scripts/compare-results.sh`

**Step 3: Commit**

```bash
git add scripts/compare-results.sh
git commit -m "feat: add compare-results.sh for Bonsai vs Archive comparison"
```

---

### Task 9: Test End-to-End (Manual)

**Files:**
- None (manual testing)

**Step 1: Run perf-test.sh with mock or real node**

If you have a node running:
```bash
./scripts/perf-test.sh --label test --duration 30 --interval 5 --output-dir /tmp/perf-test
```

If no node, verify scripts parse correctly:
```bash
bash -n scripts/perf-test.sh
bash -n scripts/compare-results.sh
echo "Syntax OK"
```

**Step 2: Verify CSV output format**

Check that CSV files have correct headers and format.

**Step 3: Final commit with any fixes**

```bash
git add scripts/
git commit -m "test: verify perf-test scripts work end-to-end"
```

---

## Usage Instructions

### On Bonsai Node

```bash
./scripts/perf-test.sh \
    --label bonsai \
    --duration 600 \
    --interval 10 \
    --historical-block 1000000 \
    --output-dir ./bonsai-results
```

### On Bonsai Archive Node

```bash
./scripts/perf-test.sh \
    --label bonsai-archive \
    --duration 600 \
    --interval 10 \
    --historical-block 1000000 \
    --output-dir ./archive-results
```

### Compare Results (locally after copying CSVs)

```bash
./scripts/compare-results.sh ./bonsai-results ./archive-results
```
