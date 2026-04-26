#!/bin/bash
# FULL Benchmark runner for ancalagon CPU and CUDA
cd ~/longbow

# Cleanup old data
rm -rf data/bench data/perf_logs
mkdir -p data/perf_logs

export LONGBOW_MAX_MEMORY=19327352832 # 18GB
export LONGBOW_DATA_PATH="./data/bench"

# ALL Dtypes requested
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"

# ALL Dimensions and Counts requested
DIMS="128,384,768,1024,3072"
COUNTS="500,1000,5000,15000,50000,100000"

# Function to run benchmark group
run_bench() {
    local mode=$1
    local dims=$2
    local counts=$3
    local label=$4
    
    echo "Running $mode benchmarks for dims=$dims counts=$counts..."
    # Ensure venv is used if available, otherwise system python
    if [ -d "venv" ]; then
        source venv/bin/activate
    fi
    
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

run_bench "cpu" "$DIMS" "$COUNTS" "full"
run_bench "cuda" "$DIMS" "$COUNTS" "full"

echo "All ANCALAGON benchmarks complete!"
date