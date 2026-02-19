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
