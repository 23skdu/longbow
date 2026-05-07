#!/bin/bash
set -e

DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768,1024,3072"
COUNTS="5000,25000,125000"
MEMORY=19327352832
QUERIES=1000

MODES=("cpu" "cuda" "temporal" "geo" "graphrag" "learned_index")

# Ensure output directory exists
mkdir -p scripts/bench_results/remote

for MODE in "${MODES[@]}"; do
    echo "[$(date)] Starting Remote Benchmark (ancalagon): Mode=$MODE"
    python3 -u scripts/unified_benchmark.py \
        --mode $MODE \
        --dtypes "$DTYPES" \
        --dims "$DIMS" \
        --counts "$COUNTS" \
        --memory $MEMORY \
        --pprof \
        --workers 16 \
        --queries $QUERIES \
        --label "remote_$MODE" \
        --output-dir "scripts/bench_results/remote"
done

echo "[$(date)] Remote Benchmark suite completed."
