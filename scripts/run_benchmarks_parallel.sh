#!/bin/bash

# Configuration
MAX_MEM=19327352832
LOCAL_REPO="/Users/rsd/REPOS/longbow"
REMOTE_HOST="ancalagon"
REMOTE_REPO="~/longbow"

cleanup_local() {
    echo "[LOCAL] Cleaning up old data and binaries..."
    rm -rf data/ bench_results/ *.log *.prof
    go clean
}

cleanup_remote() {
    echo "[REMOTE] Cleaning up old data and binaries..."
    ssh "$REMOTE_HOST" "cd $REMOTE_REPO && rm -rf data/ bench_results/ *.log *.prof && go clean"
}

run_local() {
    local mode=$1
    local gpu_enabled="false"
    if [ "$mode" == "metal" ]; then gpu_enabled="true"; fi

    echo "[LOCAL] Starting Longbow in $mode mode..."
    # Allocate 18GB
    LONGBOW_GPU_ENABLED=$gpu_enabled LONGBOW_MAX_MEMORY=$MAX_MEM \
    ./bin/longbow > local_${mode}.log 2>&1 &
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

    echo "[REMOTE] Pulling fresh changes on $REMOTE_HOST..."
    ssh "$REMOTE_HOST" "cd $REMOTE_REPO && git fetch origin && git reset --hard origin/main"
    
    echo "[REMOTE] Starting Longbow in $mode mode..."
    ssh "$REMOTE_HOST" "cd $REMOTE_REPO && LONGBOW_GPU_ENABLED=$gpu_enabled LONGBOW_MAX_MEMORY=$MAX_MEM ./bin/longbow" > remote_${mode}.log 2>&1 &
    REMOTE_SERVER_PID=$!
    
    # Wait for server readiness
    sleep 15
    
    echo "[REMOTE] Running benchmark matrix for $mode..."
    ssh "$REMOTE_HOST" "cd $REMOTE_REPO && ./scripts/bench_matrix.sh '127.0.0.1:3000' '127.0.0.1:9090' '$mode'"
    
    echo "[REMOTE] Stopping server..."
    ssh "$REMOTE_HOST" "pkill -f './cmd/longbow' || pkill longbow"
}

# Main Execution
echo "Starting Parallel Benchmarks: Local (M3) and Remote (ancalagon)..."

# Initial Cleanup
cleanup_local
cleanup_remote

echo "Compiling binaries..."
mkdir -p bin
go build -o bin/longbow ./cmd/longbow
go build -o bin/bench-tool ./cmd/bench-tool

echo "Updating remote code and compiling..."
ssh "$REMOTE_HOST" "cd $REMOTE_REPO && git fetch origin && git reset --hard origin/main && mkdir -p bin && go build -o bin/longbow ./cmd/longbow && go build -o bin/bench-tool ./cmd/bench-tool"
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
