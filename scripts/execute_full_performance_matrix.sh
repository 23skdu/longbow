#!/bin/bash
# Comprehensive performance benchmark matrix as requested by user
set -e

DIMS="128,384,768"
COUNTS="5000,10000,25000,100000,250000"
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
MEMORY=19327352832 # 18GB

# Search modes to test: ingestion, hybrid, dense, sparse, filtered, byid, learned index, geo-spatial, graphrag, temporal
# Note: bench-tool maps these to its internal mode names
SEARCH_MODES="dense,hybrid,sparse,filtered,byid,learnedindex,geo,graphrag,temporal"

export LONGBOW_MAX_MEMORY=$MEMORY
export GODEBUG=madvdontneed=1

run_local() {
    echo "[LOCAL] Building binaries..."
    go build -o bin/longbow-cpu ./cmd/longbow
    go build -o bin/longbow-metal ./cmd/longbow
    go build -o bin/bench-tool ./cmd/bench-tool
    
    echo "[LOCAL] Starting CPU benchmarks..."
    ./scripts/bench_tool_runner.sh --mode cpu --dims "$DIMS" --counts "$COUNTS" --types "$DTYPES" --queries 1000 > local_cpu_matrix.log 2>&1
    
    echo "[LOCAL] Cleaning up after CPU run..."
    killall longbow-cpu bench-tool 2>/dev/null || true
    sleep 5
    
    echo "[LOCAL] Starting Metal benchmarks..."
    ./scripts/bench_tool_runner.sh --mode metal --dims "$DIMS" --counts "$COUNTS" --types "$DTYPES" --queries 1000 > local_metal_matrix.log 2>&1
}

run_remote() {
    echo "[REMOTE] Syncing code to ancalagon..."
    rsync -avz --exclude '.git' --exclude 'bench_results' --exclude 'bin' --exclude 'data' --exclude 'profiles' . ancalagon:~/REPOS/longbow/
    
    echo "[REMOTE] Building on ancalagon..."
    ssh ancalagon "cd REPOS/longbow && mkdir -p bin && go build -o bin/longbow-cpu ./cmd/longbow && go build -tags cuda -o bin/longbow-cuda ./cmd/longbow && go build -o bin/bench-tool ./cmd/bench-tool"
    
    echo "[REMOTE] Starting CPU benchmarks..."
    ssh ancalagon "cd REPOS/longbow && export LONGBOW_MAX_MEMORY=$MEMORY && ./scripts/bench_tool_runner.sh --mode cpu --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$DTYPES\" --queries 1000" > remote_cpu_matrix.log 2>&1
    
    echo "[REMOTE] Cleaning up after CPU run..."
    ssh ancalagon "killall longbow-cpu bench-tool 2>/dev/null || true" 2>/dev/null || true
    sleep 5
    
    echo "[REMOTE] Starting CUDA benchmarks..."
    ssh ancalagon "cd REPOS/longbow && export LONGBOW_MAX_MEMORY=$MEMORY && ./scripts/bench_tool_runner.sh --mode cuda --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$DTYPES\" --queries 1000" > remote_cuda_matrix.log 2>&1
}

echo "Starting parallel matrix execution..."
run_local &
LOCAL_PID=$!

run_remote &
REMOTE_PID=$!

echo "Benchmarks running in background..."
echo "Local PID: $LOCAL_PID"
echo "Remote PID: $REMOTE_PID"

wait $LOCAL_PID
wait $REMOTE_PID

echo "All benchmarks completed."
