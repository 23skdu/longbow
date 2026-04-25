#!/bin/bash
# Benchmark runner for ancalagon CPU and CUDA
cd ~/longbow

export LONGBOW_MAX_MEMORY=19327352832

# DTs requested by user
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"

# Function to run benchmark group
run_bench() {
    local mode=$1
    local dims=$2
    local counts=$3
    local label=$4
    
    echo "Running $mode benchmarks for dims=$dims counts=$counts..."
    # Use system python with pre-installed packages
    python3 -u scripts/unified_benchmark.py \
        --mode $mode \
        --dims "$dims" \
        --counts "$counts" \
        --dtypes "$DTYPES" \
        --memory 19327352832 \
        --duration 30 \
        --queries 1000 \
        --label "ancalagon_${mode}_${label}" \
        2>&1 | tee "data/perf_logs/ancalagon_${mode}_${label}.log"
}

echo "Starting ANCALAGON benchmarks (CPU + CUDA)..."

# Group 1: 128, 384
run_bench "cpu" "128,384" "500,1000,5000,15000,50000,100000" "low_dim"
run_bench "cuda" "128,384" "500,1000,5000,15000,50000,100000" "low_dim"

# Group 2: 768, 1024, 3072
run_bench "cpu" "768,1024,3072" "500,1000,5000,10000,20000" "high_dim"
run_bench "cuda" "768,1024,3072" "500,1000,5000,10000,20000" "high_dim"

echo "All ANCALAGON benchmarks complete!"
date