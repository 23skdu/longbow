#!/bin/bash
# Master script to execute parallel performance benchmarks on Local and Remote hosts
# Sequential CPU -> GPU execution on each host.

set -e

# Configuration
DIMS="128,384,768,1024,3072"
COUNTS="50000,250000,500000,1000000"
DTYPES="float32,complex64,complex128,turboquant2,turboquant4,turboquant8"
MEMORY=19327352832 # 18GB

echo "================================================================"
echo "Starting Parallel Longbow Benchmark Suite"
echo "================================================================"

# Local Run (CPU then Metal)
run_local() {
    echo "[LOCAL] Starting CPU benchmarks..."
    ./scripts/bench_tool_runner.sh --mode cpu --dims "$DIMS" --counts "$COUNTS" --types "$DTYPES" > local_cpu.log 2>&1
    
    echo "[LOCAL] Cleaning up after CPU..."
    killall longbow-cpu bench-tool 2>/dev/null || true
    sleep 5
    
    echo "[LOCAL] Starting Metal benchmarks..."
    ./scripts/bench_tool_runner.sh --mode metal --dims "$DIMS" --counts "$COUNTS" --types "$DTYPES" > local_metal.log 2>&1
    
    echo "[LOCAL] Finished all local benchmarks."
}

# Remote Run (CPU then CUDA)
run_remote() {
    echo "[REMOTE] Starting CPU benchmarks..."
    ssh ancalagon "cd REPOS/longbow && ./scripts/bench_tool_runner.sh --mode cpu --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$DTYPES\"" > remote_cpu.log 2>&1
    
    echo "[REMOTE] Cleaning up after CPU..."
    ssh ancalagon "killall longbow-cpu bench-tool 2>/dev/null || true" 2>/dev/null || true
    sleep 5
    
    echo "[REMOTE] Starting CUDA benchmarks..."
    ssh ancalagon "cd REPOS/longbow && ./scripts/bench_tool_runner.sh --mode cuda --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$DTYPES\"" > remote_cuda.log 2>&1
    
    echo "[REMOTE] Finished all remote benchmarks."
}

# Run in parallel
run_local &
LOCAL_PID=$!

run_remote &
REMOTE_PID=$!

echo "Benchmarks running in background..."
echo "Local PID: $LOCAL_PID"
echo "Remote PID: $REMOTE_PID"

wait $LOCAL_PID
wait $REMOTE_PID

echo "================================================================"
echo "All Parallel Benchmarks Completed"
echo "================================================================"
