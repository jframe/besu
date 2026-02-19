# Bonsai Archive Performance Testing Design

## Overview

Performance testing strategy to measure the overhead Bonsai Archive adds compared to regular Bonsai, using nodes running on the Hoodi (Holesky) testnet.

## Goals

1. Measure runtime overhead of Bonsai Archive vs regular Bonsai
2. Compare block execution times, resource usage, and RPC latency
3. Document capability differences (historical query support)
4. Output CSV files for analysis

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Node A (Bonsai)                          │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              perf-test.sh --label bonsai                │    │
│  │                          │                               │    │
│  │                          ▼                               │    │
│  │            localhost:8545 (RPC)                          │    │
│  │            localhost:9545 (Metrics)                      │    │
│  │                          │                               │    │
│  │                          ▼                               │    │
│  │              bonsai_metrics.csv                          │    │
│  │              bonsai_rpc_latency.csv                      │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      Node B (Bonsai Archive)                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           perf-test.sh --label bonsai-archive           │    │
│  │                          │                               │    │
│  │                          ▼                               │    │
│  │            localhost:8545 (RPC)                          │    │
│  │            localhost:9545 (Metrics)                      │    │
│  │                          │                               │    │
│  │                          ▼                               │    │
│  │           bonsai-archive_metrics.csv                     │    │
│  │           bonsai-archive_rpc_latency.csv                 │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘

              ┌───────────────────────────┐
              │  compare.sh (run locally) │
              │  Merges CSVs, computes    │
              │  deltas and percentages   │
              └───────────────────────────┘
```

The script runs independently on each node, outputting CSV files that can later be compared.

## Metrics to Collect

### Prometheus Metrics (polled periodically)

| Metric | Category | Description |
|--------|----------|-------------|
| `besu_blockchain_chain_head_gas_used` | BLOCKCHAIN | Gas per block |
| `besu_blockchain_chain_head_transaction_count` | BLOCKCHAIN | Transactions per block |
| `besu_block_processing_execution_time_head` | BLOCK_PROCESSING | Block execution time (ms) |
| `process_cpu_seconds_total` | JVM | CPU usage |
| `jvm_memory_used_bytes` | JVM | Heap memory |
| `jvm_gc_pause_seconds_count` | JVM | GC frequency |
| `jvm_gc_pause_seconds_sum` | JVM | Total GC time |
| `rocksdb_compaction_time_total` | KVSTORE | DB compaction overhead |
| `rocksdb_write_delay_count` | KVSTORE | Write stalls |

### JSON-RPC Latency Tests

| Method | Description |
|--------|-------------|
| `eth_blockNumber` | Baseline latency (simple) |
| `eth_getBlockByNumber` | Block retrieval |
| `eth_getBalance` (latest) | Account state lookup (current) |
| `eth_getBalance` (historical) | Account state at block N - tests archive capability |
| `eth_getStorageAt` (latest) | Storage slot lookup (current) |
| `eth_getStorageAt` (historical) | Storage at block N - tests archive capability |
| `eth_call` (latest) | Contract execution (current) |
| `eth_call` (historical) | Contract execution at block N |
| `eth_getTransactionReceipt` | Receipt lookup |

**Note:** Historical queries will fail on regular Bonsai (beyond ~512 block trie log limit) but succeed on Bonsai Archive. The script records success/failure to document this capability difference.

## Script Interface

### Main script: `perf-test.sh`

```bash
perf-test.sh [OPTIONS]

Options:
  --label NAME          Node identifier (e.g., bonsai, bonsai-archive)
  --rpc-port PORT       JSON-RPC port (default: 8545)
  --metrics-port PORT   Prometheus metrics port (default: 9545)
  --duration SECONDS    Collection duration (default: 300)
  --interval SECONDS    Polling interval (default: 10)
  --historical-block N  Block number for historical queries
  --test-address ADDR   Address for balance/storage queries
  --test-slot SLOT      Storage slot for eth_getStorageAt
  --output-dir DIR      Output directory (default: ./results)
```

### Comparison script: `compare-results.sh`

```bash
compare-results.sh <bonsai_dir> <archive_dir>
```

## Output Files

### Per-node outputs

```
results/
├── {label}_metrics.csv
│   timestamp, block_number, execution_time_ms, cpu_seconds, memory_bytes, gc_count, gc_time_ms, ...
│
├── {label}_rpc_latency.csv
│   timestamp, method, block_param, latency_ms, success, error_code
│
└── {label}_summary.csv
    metric, min, max, avg, p50, p95, p99
```

### RPC latency CSV format

```csv
timestamp,method,block_param,latency_ms,success,error_code
1708300000,eth_getBalance,latest,12,true,
1708300000,eth_getBalance,1000000,45,true,
1708300000,eth_getBalance,1000000,8,false,-32000
```

### Comparison output

```csv
metric,bonsai_avg,archive_avg,delta_ms,delta_percent
eth_blockNumber,5.2,5.4,0.2,3.8%
eth_getBalance_latest,12.1,14.3,2.2,18.2%
block_execution_time,145.2,152.8,7.6,5.2%
```

## Test Execution Flow

```
1. SETUP
   ├── Verify node is responding (eth_blockNumber)
   ├── Get current block number for baseline
   ├── Create output directory
   └── Select historical block (configurable, default: current - 100000)

2. METRICS COLLECTION (runs in background)
   └── Loop every --interval seconds:
       ├── Fetch /metrics endpoint
       ├── Parse relevant Prometheus metrics
       └── Append to {label}_metrics.csv

3. RPC LATENCY TESTS (runs in parallel)
   └── Loop every --interval seconds:
       ├── eth_blockNumber (baseline)
       ├── eth_getBlockByNumber(latest)
       ├── eth_getBalance(address, latest)
       ├── eth_getBalance(address, historical)
       ├── eth_getStorageAt(address, slot, latest)
       ├── eth_getStorageAt(address, slot, historical)
       ├── eth_call(contract, latest)
       ├── eth_call(contract, historical)
       └── Append all results to {label}_rpc_latency.csv

4. COMPLETION
   ├── Stop background collection
   ├── Generate {label}_summary.csv with min/max/avg/p50/p95/p99
   └── Print summary to console
```

## Dependencies

- `curl` - HTTP requests
- `jq` - JSON parsing
- Standard bash utilities (awk, bc)

## Usage Example

```bash
# On Bonsai node
./perf-test.sh --label bonsai --duration 600 --historical-block 1000000

# On Bonsai Archive node
./perf-test.sh --label bonsai-archive --duration 600 --historical-block 1000000

# Compare results (locally)
./compare-results.sh ./bonsai-results ./archive-results
```

## Success Criteria

1. Scripts run without errors on both node types
2. CSV files contain valid, parseable data
3. Comparison clearly shows:
   - Performance delta for shared operations
   - Capability delta for historical queries
