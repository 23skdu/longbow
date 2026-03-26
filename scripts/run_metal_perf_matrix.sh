#!/usr/bin/env bash
set -euo pipefail

DIMS="${1:-128,384}"
COUNTS="${2:-1000,5000,10000,25000}"
MEMORY="${3:-12884901888}"
DTYPES="float32,float64,int8,int16,int32,uint32,complex64,complex128,turboquant"
QUERIES=200
URI="127.0.0.1:3000"
BIN="$(dirname "$0")/../bin"
DATA_DIR="$(dirname "$0")/../data/bench"
LOG_DIR="$(dirname "$0")/../data/perf_logs"
TIMEOUT=300

PLATFORM="macos"
LONGBOW_BIN="$BIN/longbow-metal"
BENCH_TOOL="$BIN/benchmark-tool"

mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTFILE="$LOG_DIR/perf_matrix_metal_${TIMESTAMP}.json"

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
    local data_root="$(dirname "$0")/../data"
    rm -rf "$data_root/wal.log" "$data_root/snapshots" "$data_root/bench" "$data_root/wal" 2>/dev/null || true
    mkdir -p "$DATA_DIR"
    LONGBOW_MAX_MEMORY="$MEMORY" \
    ARROW_DISABLE_LOCKING=1 \
    "$LONGBOW_BIN" \
        --listen-addr "$URI" \
        --data-path "$data_root" \
        --node-id bench1 \
        > "$LOG_DIR/longbow_metal_${1}.log" 2>&1 &
    SERVER_PID=$!
    for i in $(seq 1 60); do
        if lsof -i :3000 2>/dev/null | grep -q LISTEN; then
            sleep 2
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
        SERVER_PID=""
    fi
    pkill -9 -f "longbow-metal.*bench1" 2>/dev/null || true
    sleep 3
    while lsof -i :3000 2>/dev/null | grep -q LISTEN; do
        pkill -9 -f "longbow-metal" 2>/dev/null || true
        sleep 1
    done
}

run_with_timeout() {
    local timeout_secs=$1
    shift
    local cmd=("$@")
    local output_file=$(mktemp)
    
    "${cmd[@]}" > "$output_file" 2>&1 &
    local pid=$!
    
    local elapsed=0
    while kill -0 "$pid" 2>/dev/null; do
        if [[ $elapsed -ge $timeout_secs ]]; then
            kill -9 "$pid" 2>/dev/null || true
            cat "$output_file"
            rm -f "$output_file"
            return 124
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    
    wait "$pid"
    local exit_code=$?
    cat "$output_file"
    rm -f "$output_file"
    return $exit_code
}

run_benchmark() {
    local dim=$1 dtype=$2 count=$3
    local dataset="bench_metal_${dtype}_${dim}_${count}"
    local start_s=$(date +%s%N)

    local output
    output=$(
        run_with_timeout "$TIMEOUT" "$BENCH_TOOL" \
            --uri="$URI" \
            --dim="$dim" \
            --dtype="$dtype" \
            --scale="$count" \
            --queries="$QUERIES" \
            --dataset="$dataset" \
            --json="$LOG_DIR/result_metal_${dtype}_${dim}_${count}.json" \
            2>&1
    ) || {
        echo "FAILED: $dtype dim=$dim count=$count"
        echo "Output was: $output"
        return 1
    }

    local end_s=$(date +%s%N)
    local elapsed_s=$(( (end_s - start_s) / 1000000000 ))

    local json_file="$LOG_DIR/result_metal_${dtype}_${dim}_${count}.json"
    if [[ -f "$json_file" ]]; then
        RESULTS+=("$(cat "$json_file")")
    fi

    echo "  Completed in ${elapsed_s}s"
}

echo "======================================================================"
echo "FULL METAL PERFORMANCE MATRIX ($PLATFORM, $(uname -m))"
echo "Started: $(date)"
echo "DIMS=$DIMS COUNTS=$COUNTS DTYPES=$DTYPES MEMORY=$MEMORY"
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
            sleep 3
            start_server "${dtype}_${dim}_${count}"
            sleep 3

            echo "  Running benchmark..."
            if run_benchmark "$dim" "$dtype" "$count"; then
                echo "  DONE"
            else
                echo "  FAILED — INVESTIGATING"
                echo "  Server log: $LOG_DIR/longbow_metal_${dtype}_${dim}_${count}.log"
                echo "  Last 30 lines of server log:"
                tail -30 "$LOG_DIR/longbow_metal_${dtype}_${dim}_${count}.log" 2>/dev/null || true
                stop_server
                exit 1
            fi
        done
    done
done

stop_server

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
