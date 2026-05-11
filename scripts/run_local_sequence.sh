#!/bin/bash
set -e
TYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768,1024,3072"
COUNTS="5000,10000,25000"

echo "Starting Local CPU Benchmark..."
./scripts/bench_tool_runner.sh --mode cpu --dims "$DIMS" --counts "$COUNTS" --types "$TYPES" --output bench_results/local_cpu

echo "Starting Local Metal Benchmark..."
./scripts/bench_tool_runner.sh --mode metal --dims "$DIMS" --counts "$COUNTS" --types "$TYPES" --output bench_results/local_metal
