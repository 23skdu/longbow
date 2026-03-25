#!/usr/bin/env bash
set -euo pipefail

# Full Performance Matrix — Go Benchmark Tool
# Usage: ./scripts/run_perf_matrix.sh [--dims 128,384] [--counts 1000,5000,10000,25000] [--memory 12884901888]
# Each test gets a fresh server to avoid OOM/GC interference.

DIMS="${1:-128,384}"
COUNTS="${2:-1000,5000,10000,25000}"
MEMORY="${3:-12884901888}"  # 12GB default
DTYPES="float32,float64,int8,int16,int32,uint32,complex64,complex128"
QUERIES=200
URI="127.0.0.1:3000"
BIN="$(dirname "$0")/../bin"
DATA_DIR="$(dirname "$0")/../data/bench"
LOG_DIR="$(dirname "$0")/../data/perf_logs"
TIMEOUT=300  # 5 min per test

# Detect platform
if [[ "$(uname)" == "Darwin" ]]; then
    PLATFORM="macos"
    LONGBOW_BIN="$BIN/longbow"
    BENCH_TOOL="$BIN/benchmark-tool"
elif [[ "$(uname)" == "Linux" ]]; then
    PLATFORM="linux"
    LONGBOW_BIN="$BIN/longbow"
    BENCH_TOOL="$BIN/benchmark-tool"
else
    echo "Unsupported platform: $(uname)"
    exit 1
fi

mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTFILE="$LOG_DIR/perf_matrix_${PLATFORM}_${TIMESTAMP}.json"
SUMMARY="$LOG_DIR/perf_matrix_${PLATFORM}_${TIMESTAMP}.md"

RESULTS=()
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -9 "$SERVER_PID" 2>/dev/null || true
        sleep 1
    fi
}
trap cleanup EXIT

start_server() {
    rm -rf "$DATA_DIR/node1" "$DATA_DIR/node2" 2>/dev/null || true
    mkdir -p "$DATA_DIR"
    LONGBOW_MAX_MEMORY="$MEMORY" \
    ARROW_DISABLE_LOCKING=1 \
    "$LONGBOW_BIN" \
        --listen-addr "$URI" \
        --data-path "$DATA_DIR" \
        --node-id bench1 \
        > "$LOG_DIR/longbow_${1}.log" 2>&1 &
    SERVER_PID=$!
    # Wait for server to be ready
    for i in $(seq 1 30); do
        if ss -tlnp 2>/dev/null | grep -q ":3000 " || \
           lsof -i :3000 2>/dev/null | grep -q LISTEN; then
            sleep 1  # extra settle time
            return 0
        fi
        sleep 1
    done
    echo "WARNING: Server may not be ready on port 3000"
    return 0
}

stop_server() {
    if [[ -n "$SERVER_PID" ]]; then
        kill -9 "$SERVER_PID" 2>/dev/null || true
        sleep 2
        SERVER_PID=""
    fi
}

run_benchmark() {
    local dim=$1 dtype=$2 count=$3
    local dataset="bench_${dtype}_${dim}_${count}"
    local start_s=$(date +%s%N)

    local output
    output=$(
        timeout "$TIMEOUT" "$BENCH_TOOL" \
            --uri="$URI" \
            --dim="$dim" \
            --dtype="$dtype" \
            --scale="$count" \
            --queries="$QUERIES" \
            --dataset="$dataset" \
            --json="$LOG_DIR/result_${dtype}_${dim}_${count}.json" \
            2>&1
    ) || {
        echo "FAILED: $dtype dim=$dim count=$count"
        return 1
    }

    local end_s=$(date +%s%N)
    local elapsed_s=$(( (end_s - start_s) / 1000000000 ))

    # Parse JSON file if it exists
    local json_file="$LOG_DIR/result_${dtype}_${dim}_${count}.json"
    if [[ -f "$json_file" ]]; then
        RESULTS+=("$(cat "$json_file")")
    else
        # Fallback: parse stdout
        local doput_mbs doput_vecs doget_mbs doget_vecs dense_qps hybrid_qps filtered_qps idx_s
        doput_vecs=$(echo "$output" | grep "DoPut" | head -1 | awk -F'|' '{gsub(/,/,"",$2); print $2}' || echo "0")
        doput_mbs=$(echo "$output" | grep "DoPut" | head -1 | awk -F'|' '{gsub(/,/,"",$3); print $3}' || echo "0")
        doget_vecs=$(echo "$output" | grep "DoGet" | head -1 | awk -F'|' '{gsub(/,/,"",$2); print $2}' || echo "0")
        doget_mbs=$(echo "$output" | grep "DoGet" | head -1 | awk -F'|' '{gsub(/,/,"",$3); print $3}' || echo "0")
        dense_qps=$(echo "$output" | grep "Dense" | head -1 | awk -F'|' '{gsub(/,/,"",$2); print $2}' || echo "0")
        hybrid_qps=$(echo "$output" | grep "Hybrid" | head -1 | awk -F'|' '{gsub(/,/,"",$2); print $2}' || echo "0")
        filtered_qps=$(echo "$output" | grep "Filtered" | head -1 | awk -F'|' '{gsub(/,/,"",$2); print $2}' || echo "0")
        idx_s=$(echo "$output" | grep "Index" | head -1 | awk -F'|' '{gsub(/,/,"",$2); print $2}' || echo "0")

        RESULTS+=("{\"dtype\":\"$dtype\",\"dim\":$dim,\"count\":$count,\"doput_vecs\":${doput_vecs:-0},\"doput_mbs\":${doput_mbs:-0},\"doget_vecs\":${doget_vecs:-0},\"doget_mbs\":${doget_mbs:-0},\"dense_qps\":${dense_qps:-0},\"hybrid_qps\":${hybrid_qps:-0},\"filtered_qps\":${filtered_qps:-0},\"index_s\":${idx_s:-0},\"elapsed_s\":$elapsed_s}")
    fi
}

# Main
echo "======================================================================"
echo "FULL PERFORMANCE MATRIX ($PLATFORM, $(uname -m), ${MEMORY} bytes memory)"
echo "Started: $(date)"
echo "DIMS=$DIMS COUNTS=$COUNTS DTYPES=$DTYPES"
echo "======================================================================"

IFS=',' read -ra DIM_ARR <<< "$DIMS"
IFS=',' read -ra COUNT_ARR <<< "$COUNTS"
IFS=',' read -ra DTYPE_ARR <<< "$DTYPES"

total=${#DIM_ARR[@]}
total=$((total * ${#COUNT_ARR[@]} * ${#DTYPE_ARR[@]}))
run_num=0

for dim in "${DIM_ARR[@]}"; do
    for dtype in "${DTYPE_ARR[@]}"; do
        for count in "${COUNT_ARR[@]}"; do
            run_num=$((run_num + 1))
            label="$dtype dim=$dim count=$count"
            echo ""
            echo "[$run_num/$total] $label"
            echo "  Starting fresh server..."
            stop_server
            sleep 1
            start_server "${dtype}_${dim}_${count}"
            sleep 2

            echo "  Running benchmark..."
            if run_benchmark "$dim" "$dtype" "$count"; then
                echo "  DONE ($(( $(date +%s%N) / 1000000000 ))s total)"
            else
                echo "  FAILED — continuing"
            fi
        done
    done
done

stop_server

# Save JSON
echo "[" > "$OUTFILE"
for i in "${!RESULTS[@]}"; do
    if [[ $i -gt 0 ]]; then echo "," >> "$OUTFILE"; fi
    echo "${RESULTS[$i]}" >> "$OUTFILE"
done
echo "]" >> "$OUTFILE"

echo ""
echo "Results saved to: $OUTFILE"
echo "Completed: $(date)"
echo "======================================================================"
