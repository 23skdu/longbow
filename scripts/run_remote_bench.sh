#!/bin/bash
# Remote Benchmark Runner (to be run on ancalagon)
set -e

# Setup environment
export LONGBOW_MAX_MEMORY=19327352832
export PATH=$PATH:$(pwd)/bin

DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="384"
COUNTS="5000,25000,100000"

echo "Starting Remote CPU Benchmarks..."
python3 scripts/unified_benchmark.py --mode cpu --dtypes $DTYPES --dims $DIMS --counts $COUNTS --label remote_cpu --output-dir scripts/bench_results/remote/cpu --pprof

echo "Starting Remote CUDA Benchmarks..."
python3 scripts/unified_benchmark.py --mode cuda --dtypes $DTYPES --dims $DIMS --counts $COUNTS --label remote_cuda --output-dir scripts/bench_results/remote/cuda --pprof

echo "Remote Benchmarks Completed."
