#!/bin/bash
set -e

TYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768,1024,3072"
COUNTS="5000,10000,25000,100000,250000"
MODES="all"
MEMORY="19327352832" # 18GB

echo "Starting MASTER LOCAL BENCHMARK (CPU -> Metal)"
export LONGBOW_MAX_MEMORY=$MEMORY

echo "--- RUNNING CPU MODE ---"
python3 scripts/unified_benchmark.py --mode cpu --dtypes $TYPES --dims $DIMS --counts $COUNTS --search-modes $MODES --queries 100 --pprof --label final_local_cpu

echo "--- RUNNING METAL MODE ---"
python3 scripts/unified_benchmark.py --mode metal --dtypes $TYPES --dims $DIMS --counts $COUNTS --search-modes $MODES --queries 100 --pprof --label final_local_metal

echo "MASTER LOCAL BENCHMARK COMPLETE"
