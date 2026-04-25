#!/bin/bash
# Benchmark runner for local CPU and Metal
cd /Users/rsd/REPOS/longbow

export LONGBOW_MAX_MEMORY=19327352832

# Function to run benchmark group
run_bench() {
    local mode=$1
    local dims=$2
    local counts=$3
    local dtypes=$4
    local label=$5
    
    echo "Running $mode benchmarks for dims=$dims counts=$counts..."
    source venv/bin/activate
    python3 -u scripts/unified_benchmark.py \
        --mode $mode \
        --dims "$dims" \
        --counts "$counts" \
        --dtypes "$dtypes" \
        --memory 19327352832 \
        --duration 30 \
        --queries 1000 \
        --label "$label" \
        2>&1 | tee "data/perf_logs/local_${mode}_${label}.log"
}

# DTs requested by user
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"

# Run CPU and Metal sequentially on local host (to avoid resource contention on same machine)
echo "Starting LOCAL benchmarks (CPU + Metal)..."

# Group 1: 128, 384
run_bench "cpu" "128,384" "500,1000,5000,15000,50000,100000" "$DTYPES" "low_dim"
run_bench "metal" "128,384" "500,1000,5000,15000,50000,100000" "$DTYPES" "low_dim"

# Group 2: 768, 1024, 3072
run_bench "cpu" "768,1024,3072" "500,1000,5000,10000,20000" "$DTYPES" "high_dim"
run_bench "metal" "768,1024,3072" "500,1000,5000,10000,20000" "$DTYPES" "high_dim"

echo "All LOCAL benchmarks complete!"
date