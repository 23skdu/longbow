#!/bin/bash

# Configuration
MAX_MEM=19327352832
LOCAL_REPO="/Users/rsd/REPOS/longbow"
REMOTE_HOST="ancalagon"
REMOTE_REPO="~/longbow_bench"

run_local() {
    local mode=$1
    local gpu_enabled="false"
    if [ "$mode" == "metal" ]; then gpu_enabled="true"; fi

    echo "[LOCAL] Starting Longbow in $mode mode..."
    LONGBOW_GPU_ENABLED=$gpu_enabled LONGBOW_MAX_MEMORY=$MAX_MEM \
    go run ./cmd/longbow > local_${mode}.log 2>&1 &
    SERVER_PID=$!
    
    # Wait for server readiness
    sleep 10
    
    echo "[LOCAL] Running benchmark matrix for $mode..."
    ./scripts/bench_matrix.sh "127.0.0.1:3000" "127.0.0.1:9090" "$mode"
    
    echo "[LOCAL] Stopping server..."
    kill $SERVER_PID
    wait $SERVER_PID 2>/dev/null
}

run_remote() {
    local mode=$1
    local gpu_enabled="false"
    if [ "$mode" == "cuda" ]; then gpu_enabled="true"; fi

    echo "[REMOTE] Syncing code to $REMOTE_HOST..."
    tar --exclude='.git' --exclude='bench_results' --exclude='data/' --exclude='*.log' --exclude='bin' --exclude='debug' -cf - . | ssh "$REMOTE_HOST" "rm -rf $REMOTE_REPO && mkdir -p $REMOTE_REPO && cd $REMOTE_REPO && tar -xf -"
    
    echo "[REMOTE] Starting Longbow in $mode mode..."
    ssh "$REMOTE_HOST" "cd $REMOTE_REPO && LONGBOW_GPU_ENABLED=$gpu_enabled LONGBOW_MAX_MEMORY=$MAX_MEM go run ./cmd/longbow" > remote_${mode}.log 2>&1 &
    REMOTE_SERVER_PID=$!
    
    # Wait for server readiness
    sleep 15
    
    echo "[REMOTE] Running benchmark matrix for $mode..."
    ssh "$REMOTE_HOST" "cd $REMOTE_REPO && ./scripts/bench_matrix.sh '127.0.0.1:3000' '127.0.0.1:9090' '$mode'"
    
    echo "[REMOTE] Stopping server..."
    ssh "$REMOTE_HOST" "pkill -f 'cmd/longbow/main.go' || pkill longbow"
}

# Main Execution
echo "Starting Parallel Benchmarks: Local (M3) and Remote (ancalagon)..."

# Local Thread
(
    run_local "cpu"
    run_local "metal"
) &
LOCAL_JOB=$!

# Remote Thread
(
    run_remote "cpu"
    run_remote "cuda"
) &
REMOTE_JOB=$!

echo "Jobs started. Waiting for completion..."
wait $LOCAL_JOB
wait $REMOTE_JOB

echo "All benchmarks completed."
