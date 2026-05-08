#!/bin/bash
# Master Remote Benchmark Script (to be run on ancalagon)
set -e

DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768,1024,3072"
COUNTS="25000,100000,250000,500000"
QUERIES=500

echo "Starting Remote Sequential Benchmarks (CPU then CUDA) on ancalagon..."

# Run CPU
echo "--- Starting Remote CPU Benchmarks ---"
./scripts/bench_tool_runner.sh \
    --mode cpu \
    --types "$DTYPES" \
    --dims "$DIMS" \
    --counts "$COUNTS" \
    --queries $QUERIES \
    --output "benchmark_results/remote"

# Run CUDA
echo "--- Starting Remote CUDA Benchmarks ---"
./scripts/bench_tool_runner.sh \
    --mode cuda \
    --types "$DTYPES" \
    --dims "$DIMS" \
    --counts "$COUNTS" \
    --queries $QUERIES \
    --output "benchmark_results/remote"

echo "Remote Benchmarks Completed."
