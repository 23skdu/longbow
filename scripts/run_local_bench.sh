#!/bin/bash
# Local Benchmark Runner
set -e

# Setup environment
export LONGBOW_MAX_MEMORY=19327352832
export PATH=$PATH:$(pwd)/bin

DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="384"
COUNTS="5000,25000,100000"

echo "Starting Local CPU Benchmarks..."
python3 scripts/unified_benchmark.py --mode cpu --dtypes $DTYPES --dims $DIMS --counts $COUNTS --label local_cpu --output-dir scripts/bench_results/local/cpu --pprof

echo "Starting Local Metal Benchmarks..."
python3 scripts/unified_benchmark.py --mode metal --dtypes $DTYPES --dims $DIMS --counts $COUNTS --label local_metal --output-dir scripts/bench_results/local/metal --pprof

echo "Local Benchmarks Completed."
