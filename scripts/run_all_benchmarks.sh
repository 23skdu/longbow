#!/bin/bash
# Master script to run benchmarks local and remote in parallel
set -e

DIMS="128,384,768,1024,3072"
COUNTS="1000,5000,10000,25000,100000,250000"
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
QUERIES=100

run_local() {
    echo "[LOCAL] Running CPU benchmarks..."
    ./scripts/bench_tool_runner.sh --mode cpu --dims "$DIMS" --counts "$COUNTS" --types "$DTYPES" --queries "$QUERIES"
    
    echo "[LOCAL] Running Metal benchmarks..."
    ./scripts/bench_tool_runner.sh --mode metal --dims "$DIMS" --counts "$COUNTS" --types "$DTYPES" --queries "$QUERIES"
}

run_remote() {
    echo "[REMOTE] Syncing code to ancalagon..."
    rsync -avz --exclude '.git' --exclude 'bench_results' --exclude 'data' --exclude 'bin' . ancalagon:~/REPOS/longbow/
    
    echo "[REMOTE] Building on ancalagon..."
    ssh ancalagon "cd REPOS/longbow && mkdir -p bin && go build -o bin/longbow-cpu ./cmd/longbow && go build -tags cuda -o bin/longbow-cuda ./cmd/longbow && go build -o bin/bench-tool ./cmd/bench-tool"
    
    echo "[REMOTE] Running CPU benchmarks..."
    ssh ancalagon "cd REPOS/longbow && ./scripts/bench_tool_runner.sh --mode cpu --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$DTYPES\" --queries \"$QUERIES\""
    
    echo "[REMOTE] Running CUDA benchmarks..."
    ssh ancalagon "cd REPOS/longbow && ./scripts/bench_tool_runner.sh --mode cuda --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$DTYPES\" --queries \"$QUERIES\""
}

# Run local and remote in parallel
run_local &
LOCAL_PID=$!

run_remote &
REMOTE_PID=$!

wait $LOCAL_PID
wait $REMOTE_PID

echo "All benchmarks completed."
