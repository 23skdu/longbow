#!/bin/bash
set -e

DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768,1024,3072"
COUNTS="5000,25000,125000"
MEMORY=19327352832
QUERIES=1000

MODES=("cpu" "metal" "temporal" "geo" "graphrag" "learned_index")

# Ensure output directory exists
mkdir -p scripts/bench_results/local

for MODE in "${MODES[@]}"; do
    echo "[$(date)] Starting Local Benchmark: Mode=$MODE"
    python3 -u scripts/unified_benchmark.py \
        --mode $MODE \
        --dtypes "$DTYPES" \
        --dims "$DIMS" \
        --counts "$COUNTS" \
        --memory $MEMORY \
        --pprof \
        --workers 8 \
        --queries $QUERIES \
        --label "local_$MODE" \
        --output-dir "scripts/bench_results/local"
done

echo "[$(date)] Local Benchmark suite completed."
