#!/bin/bash
# scripts/execute_matrix_remote.sh

DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768,1024,3072"
COUNTS="25000,50000,100000,150000"

# Remote ancalagon (CPU then CUDA)
./scripts/bench_tool_runner.sh --mode cpu --dims "$DIMS" --counts "$COUNTS" --types "$DTYPES" --output bench_results/ancalagon_cpu
./scripts/bench_tool_runner.sh --mode cuda --dims "$DIMS" --counts "$COUNTS" --types "$DTYPES" --output bench_results/ancalagon_cuda
