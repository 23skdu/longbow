#!/bin/bash
set -e

DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768"
COUNTS="1000,5000,10000,25000,50000,150000,300000,500000"
MEMORY=27917287424
WORKERS=8
QUERIES=100

MODES=("cpu" "cuda" "temporal" "geo" "graphrag" "learned_index")

for MODE in "${MODES[@]}"; do
    echo "Starting Remote Benchmark (ancalagon): Mode=$MODE"
    python3 -u scripts/unified_benchmark.py --mode $MODE --dtypes "$DTYPES" --dims "$DIMS" --counts "$COUNTS" --memory $MEMORY --pprof --workers $WORKERS --queries $QUERIES --label "remote_$MODE"
done
