#!/bin/bash
set -e
trap "pkill -P $$; exit" SIGINT SIGTERM EXIT
DIMS="128,384,768,1024,3072"
COUNTS="5000,10000,25000,100000,250000"
TYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"

echo "Starting Remote CPU Benchmark..."
./scripts/bench_tool_runner.sh --mode cpu --dims "$DIMS" --counts "$COUNTS" --types "$TYPES" --output benchmark_results

echo "Starting Remote CUDA Benchmark..."
./scripts/bench_tool_runner.sh --mode cuda --dims "$DIMS" --counts "$COUNTS" --types "$TYPES" --output benchmark_results
