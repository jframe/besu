#!/usr/bin/env bash
set -euo pipefail

# Default values
LABEL="bonsai"
RPC_PORT="8545"
METRICS_PORT="9545"
DURATION="300"
INTERVAL="10"
HISTORICAL_BLOCK=""
TEST_ADDRESS="0x00000000219ab540356cBB839Cbe05303d7705Fa"
TEST_SLOT="0x0"
OUTPUT_DIR="./results"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Performance testing script for Besu Bonsai Archive vs regular Bonsai comparison.

Options:
    --label NAME          Label for this test run (default: bonsai)
    --rpc-port PORT       JSON-RPC port (default: 8545)
    --metrics-port PORT   Metrics port (default: 9545)
    --duration SECONDS    Test duration in seconds (default: 300)
    --interval SECONDS    Sampling interval in seconds (default: 10)
    --historical-block N  Historical block number for queries (default: empty)
    --test-address ADDR   Contract address to test (default: 0x00000000219ab540356cBB839Cbe05303d7705Fa)
    --test-slot SLOT      Storage slot to test (default: 0x0)
    --output-dir DIR      Output directory for results (default: ./results)
    -h, --help            Show this help message

Examples:
    $(basename "$0") --label bonsai-archive --duration 600
    $(basename "$0") --rpc-port 8546 --historical-block 19000000
EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --label)
            LABEL="$2"
            shift 2
            ;;
        --rpc-port)
            RPC_PORT="$2"
            shift 2
            ;;
        --metrics-port)
            METRICS_PORT="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --interval)
            INTERVAL="$2"
            shift 2
            ;;
        --historical-block)
            HISTORICAL_BLOCK="$2"
            shift 2
            ;;
        --test-address)
            TEST_ADDRESS="$2"
            shift 2
            ;;
        --test-slot)
            TEST_SLOT="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Error: Unknown option: $1" >&2
            echo "Use --help for usage information." >&2
            exit 1
            ;;
    esac
done

# Set derived URLs from ports
RPC_URL="http://localhost:${RPC_PORT}"
METRICS_URL="http://localhost:${METRICS_PORT}/metrics"

# Echo configuration
echo "=== Performance Test Configuration ==="
echo "Label:            ${LABEL}"
echo "RPC URL:          ${RPC_URL}"
echo "Metrics URL:      ${METRICS_URL}"
echo "Duration:         ${DURATION} seconds"
echo "Interval:         ${INTERVAL} seconds"
echo "Historical Block: ${HISTORICAL_BLOCK:-<latest>}"
echo "Test Address:     ${TEST_ADDRESS}"
echo "Test Slot:        ${TEST_SLOT}"
echo "Output Directory: ${OUTPUT_DIR}"
echo "======================================"

# JSON-RPC helper - returns "latency_ms,success,error_code,result"
rpc_call() {
    local method="$1"
    local params="$2"
    local start_ms end_ms latency_ms response error_code result success

    start_ms=$(date +%s%3N)
    response=$(curl -s --max-time 30 -X POST "$RPC_URL" \
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

    # eth_call - latest
    record "eth_call" "latest" "$(rpc_call "eth_call" "[{\"to\": \"$TEST_ADDRESS\"}, \"latest\"]")"

    # eth_call - historical
    record "eth_call" "$HISTORICAL_BLOCK" "$(rpc_call "eth_call" "[{\"to\": \"$TEST_ADDRESS\"}, \"$hist_hex\"]")"

    # Transaction receipt
    record "eth_getTransactionReceipt" "n/a" "$(rpc_call "eth_getTransactionReceipt" "[\"0x0000000000000000000000000000000000000000000000000000000000000000\"]")"
}

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

setup
run_collection
generate_summary

echo ""
echo "Performance test complete!"
echo "Results saved to: $OUTPUT_DIR"