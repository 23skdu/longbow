#!/bin/bash
# Master benchmark script to run local and remote tests in parallel
set -e

DIMS="128,384,768,1024,3072"
COUNTS="10000,25000,100000"
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
QUERIES=1000

run_local() {
    echo "[LOCAL] Starting CPU benchmarks..."
    ./scripts/bench_tool_runner.sh --mode cpu --dims "$DIMS" --counts "$COUNTS" --types "$DTYPES" --queries "$QUERIES" > local_cpu_matrix.log 2>&1
    
    echo "[LOCAL] Starting Metal benchmarks..."
    ./scripts/bench_tool_runner.sh --mode metal --dims "$DIMS" --counts "$COUNTS" --types "$DTYPES" --queries "$QUERIES" > local_metal_matrix.log 2>&1
}

run_remote() {
    echo "[REMOTE] Starting CPU benchmarks on ancalagon..."
    ssh ancalagon "cd REPOS/longbow && ./scripts/bench_tool_runner.sh --mode cpu --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$DTYPES\" --queries \"$QUERIES\"" > remote_cpu_matrix.log 2>&1
    
    echo "[REMOTE] Starting CUDA benchmarks on ancalagon..."
    ssh ancalagon "cd REPOS/longbow && ./scripts/bench_tool_runner.sh --mode cuda --dims \"$DIMS\" --counts \"$COUNTS\" --types \"$DTYPES\" --queries \"$QUERIES\"" > remote_cuda_matrix.log 2>&1
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
echo "Local benchmarks completed."

wait $REMOTE_PID
echo "Remote benchmarks completed. Syncing results back..."
rsync -avz ancalagon:~/REPOS/longbow/bench_results/ bench_results/

echo "All benchmarks completed. Aggregating results..."
python3 scripts/aggregate_results.py --dir bench_results/ --out docs/performance_matrix_new.md
