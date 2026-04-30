#!/bin/bash

# COORDINATOR SCRIPT for Parallel Benchmarking
# Host 1: Local (M3 Darwin) - CPU and Metal
# Host 2: Remote (Ancalagon Linux) - CPU and CUDA

echo "Starting Parallel Benchmarking Suite..."
mkdir -p results_parallel

# 1. Run Local benchmarks (CPU followed by Metal)
echo "Starting LOCAL benchmarks (CPU then Metal)..."
(
    ./scripts/bench_matrix.sh cpu local 3000 9090 > local_cpu_run.log 2>&1
    ./scripts/bench_matrix.sh metal local 3001 9091 > local_metal_run.log 2>&1
) &
LOCAL_PID=$!

# 2. Run Remote benchmarks on Ancalagon (CPU followed by CUDA)
echo "Starting REMOTE benchmarks on ancalagon (CPU then CUDA)..."
(
    ssh ancalagon "cd REPOS/longbow && ./scripts/bench_matrix.sh cpu remote 3000 9090" > remote_cpu_run.log 2>&1
    ssh ancalagon "cd REPOS/longbow && ./scripts/bench_matrix.sh cuda remote 3001 9091" > remote_cuda_run.log 2>&1
) &
REMOTE_PID=$!

echo "Local benchmarks PID: $LOCAL_PID"
echo "Remote benchmarks PID: $REMOTE_PID"
echo "Monitoring execution... This will take a long time."

# Wait for both to finish
wait $LOCAL_PID
wait $REMOTE_PID

# Sync results from ancalagon
echo "Syncing results from ancalagon..."
rsync -avz ancalagon:REPOS/longbow/results_remote_*.json .
rsync -avz ancalagon:REPOS/longbow/profiles/ remote_profiles/

echo "All benchmarks finished. Results are in the current directory."
