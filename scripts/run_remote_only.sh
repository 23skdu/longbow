#!/bin/bash
# scripts/run_remote_only.sh
set -e

DIMS="128,384,768,1024,3072"
COUNTS="10000,25000,50000,100000"
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
MEMORY=19327352832 # 18GB

echo "[REMOTE] Syncing code to ancalagon with --delete..."
rsync -avz --delete --exclude '.git' --exclude 'bench_results' --exclude 'bin' --exclude 'data' --exclude 'profiles' . ancalagon:~/REPOS/longbow/

echo "[REMOTE] Building on ancalagon..."
ssh ancalagon "cd REPOS/longbow && mkdir -p bin && go build -o bin/longbow-cpu ./cmd/longbow && go build -tags cuda -o bin/longbow-cuda ./cmd/longbow && go build -o bin/bench-tool ./cmd/bench-tool"

echo "[REMOTE] Starting CPU benchmarks..."
ssh ancalagon "cd REPOS/longbow && ./scripts/bench_tool_runner.sh --mode cpu --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$DTYPES\" --queries 1000"

echo "[REMOTE] Starting CUDA benchmarks..."
ssh ancalagon "cd REPOS/longbow && ./scripts/bench_tool_runner.sh --mode cuda --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$DTYPES\" --queries 1000"
