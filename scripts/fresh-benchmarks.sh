#!/bin/bash
set -e

# =============================================================================
# Fresh-Benchmark Orchestration Script
# Runs Longbow benchmarks with fresh server per test group.
# - Cleans data directory between each run
# - Starts server with 20GB RAM
# - Pre-warms before each test
# - Records results to a JSON file for later processing
# =============================================================================

OUTPUT_FILE="${1:-fresh_bench_results.json}"
PYTHON="${2:-python3}"

MEMORY_LIMIT=21474836480  # 20GB
BINARY="./bin/longbow"
DATA_DIR="data/node1"
REPORTS_DIR="reports"
mkdir -p "$REPORTS_DIR"

# Cleanup function
cleanup() {
    echo "[CLEANUP] Stopping server..."
    pkill -f "./bin/longbow" 2>/dev/null || true
    sleep 2
}
trap cleanup EXIT

# Start server
start_server() {
    echo "[SERVER] Starting Longbow (20GB RAM)..."
    LONGBOW_LISTEN_ADDR=0.0.0.0:3000 \
    LONGBOW_NODE_ID=node1 \
    LONGBOW_META_ADDR=0.0.0.0:3001 \
    LONGBOW_METRICS_ADDR=0.0.0.0:9090 \
    LONGBOW_GOSSIP_PORT=7946 \
    LONGBOW_GOSSIP_ENABLED=true \
    LONGBOW_GOSSIP_DISCOVERY_PROVIDER=static \
    LONGBOW_GOSSIP_STATIC_PEERS="" \
    LONGBOW_DATA_PATH="$DATA_DIR" \
    LONGBOW_MAX_MEMORY=$MEMORY_LIMIT \
    LONGBOW_HYBRID_SEARCH_ENABLED=true \
    LONGBOW_HYBRID_TEXT_COLUMNS=text \
    GOGC=75 \
    "$BINARY" > "$REPORTS_DIR/server.log" 2>&1 &
    SERVER_PID=$!
    echo "[SERVER] Started (PID $SERVER_PID)"
    
    echo "[SERVER] Waiting for readiness (15s)..."
    sleep 15
    
    # Verify server is up
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "[ERROR] Server died. Check $REPORTS_DIR/server.log"
        cat "$REPORTS_DIR/server.log"
        exit 1
    fi
    echo "[SERVER] Ready."
}

# Stop server
stop_server() {
    echo "[SERVER] Stopping..."
    pkill -f "./bin/longbow" 2>/dev/null || true
    sleep 3
    echo "[SERVER] Stopped."
}

# Clean data directory
clean_data() {
    echo "[CLEAN] Removing data directory..."
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    echo "[CLEAN] Done."
}

# Run a single benchmark test
run_test() {
    local dtype="$1"
    local dim="$2"
    local rows="$3"
    local dataset_name="$4"
    local extra_flags="$5"
    
    echo ""
    echo "=================================================="
    echo "  Running: dtype=$dtype dim=$dim rows=$rows"
    echo "  Dataset: $dataset_name"
    echo "=================================================="
    
    local report_file="$REPORTS_DIR/${dataset_name}.json"
    
    "$PYTHON" scripts/perf_test.py \
        --data-uri grpc://127.0.0.1:3000 \
        --meta-uri grpc://127.0.0.1:3001 \
        --dataset "$dataset_name" \
        --rows "$rows" \
        --dim "$dim" \
        --dtype "$dtype" \
        --search \
        --k 10 \
        --queries 1000 \
        --json "$report_file" \
        $extra_flags
    
    echo "[TEST] Done: $dataset_name"
}

# =============================================================================
# MAIN: Run all benchmarks
# =============================================================================

echo "=================================================="
echo "LONGBOW FRESH BENCHMARK SUITE"
echo "Memory: 20GB | Python: $PYTHON"
echo "=================================================="

# Initialize results accumulator
ALL_RESULTS='[]'

# Previous session results: key=expected_put,expected_get,expected_qps,expected_p50,expected_p99
# Used by the Python aggregation step only.

# =============================================================================
# TEST GROUP 1: Data type comparison at dim=128, 10k vectors
# =============================================================================

clean_data
start_server

run_test "float32" 128 10000 "bench_float32_d128_10k"
run_test "float64" 128 10000 "bench_float64_d128_10k"
run_test "int8"    128 10000 "bench_int8_d128_10k"
run_test "complex64" 128 10000 "bench_complex64_d128_10k"

stop_server

# =============================================================================
# TEST GROUP 2: dim=384 scales (fresh server per scale)
# =============================================================================

# 5k dim=384
clean_data
start_server
run_test "float32" 384 5000 "bench_float32_d384_5k"
stop_server

# 10k dim=384
clean_data
start_server
run_test "float32" 384 10000 "bench_float32_d384_10k"
stop_server

# 25k dim=384
clean_data
start_server
run_test "float32" 384 25000 "bench_float32_d384_25k"
stop_server

# =============================================================================
# TEST GROUP 3: Larger scales at dim=128
# =============================================================================

# 25k dim=128
clean_data
start_server
run_test "float32" 128 25000 "bench_float32_d128_25k"
stop_server

# 50k dim=128
clean_data
start_server
run_test "float32" 128 50000 "bench_float32_d128_50k"
stop_server

# =============================================================================
# Aggregate results
# =============================================================================

echo ""
echo "=================================================="
echo "BENCHMARK COMPLETE"
echo "=================================================="

# Build comparison table from JSON reports
"$PYTHON" - << 'PYEOF'
import json
import os
import sys

reports_dir = "reports"
results = []

# Known previous results: "put_mb_s,doget_mb_s,search_qps,p50_ms,p99_ms"
prev = {
    "bench_float32_d128_10k": (625, 1094, 2054, 0.48, 0.55),
    "bench_float64_d128_10k": (958, 1483, 3555, 0.27, 0.39),
    "bench_int8_d128_10k":    (236, 626, 3637, 0.26, 0.42),
    "bench_complex64_d128_10k": (854, 1174, 1273, 0.77, 0.99),
    "bench_float32_d384_5k":  (850, 1703, 1124, 0.87, 1.24),
}

files = sorted([f for f in os.listdir(reports_dir) if f.endswith('.json')])

print(f"\n{'Config':<35} | {'Put MB/s':>10} | {'Get MB/s':>10} | {'Search QPS':>12} | {'p50 ms':>8} | {'p99 ms':>8} | {'vs Prev Put':>10} | {'vs Prev QPS':>10}")
print("-" * 130)

for fname in files:
    path = os.path.join(reports_dir, fname)
    name = fname.replace('.json', '')
    try:
        with open(path) as f:
            data = json.load(f)
        
        # Find relevant results
        put_r = next((r for r in data if 'DoPut' in r['name']), None)
        get_r = next((r for r in data if 'DoGet' in r['name']), None)
        search_r = next((r for r in data if 'VectorSearch' in r['name']), None)
        
        put = put_r['throughput'] if put_r else 0
        get = get_r['throughput'] if get_r else 0
        qps = search_r['throughput'] if search_r else 0
        p50 = search_r['p50_ms'] if search_r else 0
        p99 = search_r['p99_ms'] if search_r else 0
        
        put_delta = ""
        qps_delta = ""
        if name in prev:
            prev_put, prev_get, prev_qps, prev_p50, prev_p99 = prev[name]
            put_ch = ((put - prev_put) / prev_put * 100) if prev_put else 0
            qps_ch = ((qps - prev_qps) / prev_qps * 100) if prev_qps else 0
            put_delta = f"{put_ch:+.1f}%"
            qps_delta = f"{qps_ch:+.1f}%"
        
        print(f"{name:<35} | {put:>10.0f} | {get:>10.0f} | {qps:>12.0f} | {p50:>8.2f} | {p99:>8.2f} | {put_delta:>10} | {qps_delta:>10}")
        
        results.append({
            "name": name,
            "put_mb_s": round(put, 2),
            "get_mb_s": round(get, 2),
            "search_qps": round(qps, 2),
            "p50_ms": round(p50, 2),
            "p99_ms": round(p99, 2),
        })
    except Exception as e:
        print(f"Error reading {fname}: {e}")

# Save aggregated results
with open("fresh_bench_results.json", "w") as f:
    json.dump(results, f, indent=2)
print(f"\nResults saved to fresh_bench_results.json")
PYEOF

echo ""
echo "All done! See reports/ for individual test results."
echo "Run: python3 scripts/format_perf_tables.py or update docs/performance.md manually."
