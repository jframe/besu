#!/bin/bash
##
## Copyright contributors to Besu.
##
## Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
## the License. You may obtain a copy of the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
## an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
## specific language governing permissions and limitations under the License.
##
## SPDX-License-Identifier: Apache-2.0
##

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
