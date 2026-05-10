#!/bin/bash
# Local/Remote Parallel Benchmark Orchestrator

DIMS="128,384,768,1024,3072"
COUNTS="10000,25000"
TYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"

# Local Track
(
    echo "[LOCAL] Starting CPU benchmarks..."
    ./scripts/bench_tool_runner.sh --mode cpu --dims "$DIMS" --counts "$COUNTS" --types "$TYPES" > local_cpu_track.log 2>&1
    echo "[LOCAL] CPU benchmarks finished. Cooling down..."
    sleep 30
    echo "[LOCAL] Starting Metal benchmarks..."
    ./scripts/bench_tool_runner.sh --mode metal --dims "$DIMS" --counts "$COUNTS" --types "$TYPES" > local_metal_track.log 2>&1
    echo "[LOCAL] Metal benchmarks finished."
) &
LOCAL_PID=$!

# Remote Track
(
    echo "[REMOTE] Starting CPU benchmarks on ancalagon..."
    ssh ancalagon "cd ~/REPOS/longbow && ./scripts/bench_tool_runner.sh --mode cpu --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$TYPES\"" > remote_cpu_track.log 2>&1
    echo "[REMOTE] CPU benchmarks finished. Cooling down..."
    sleep 30
    echo "[REMOTE] Starting CUDA benchmarks on ancalagon..."
    ssh ancalagon "cd ~/REPOS/longbow && ./scripts/bench_tool_runner.sh --mode cuda --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$TYPES\"" > remote_cuda_track.log 2>&1
    echo "[REMOTE] CUDA benchmarks finished."
) &
REMOTE_PID=$!

echo "Local Parallel PID: $LOCAL_PID"
echo "Remote Parallel PID: $REMOTE_PID"

wait $LOCAL_PID
wait $REMOTE_PID

echo "All benchmarks completed."
