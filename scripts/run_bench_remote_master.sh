#!/bin/bash
set -e

TYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768,1024,3072"
COUNTS="5000,10000,25000,100000,250000"
MODES="all"
MEMORY="19327352832" # 18GB

echo "Starting MASTER REMOTE BENCHMARK on ancalagon (CPU -> CUDA)"

ssh ancalagon "cd REPOS/longbow && \
export LONGBOW_MAX_MEMORY=$MEMORY && \
echo '--- RUNNING CPU MODE ---' && \
python3 scripts/unified_benchmark.py --mode cpu --dtypes $TYPES --dims $DIMS --counts $COUNTS --search-modes $MODES --queries 100 --pprof --label final_remote_cpu && \
echo '--- RUNNING CUDA MODE ---' && \
python3 scripts/unified_benchmark.py --mode cuda --dtypes $TYPES --dims $DIMS --counts $COUNTS --search-modes $MODES --queries 100 --pprof --label final_remote_cuda"

echo "MASTER REMOTE BENCHMARK COMPLETE"
