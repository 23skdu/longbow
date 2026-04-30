#!/bin/bash

# 1. Sync to ancalagon
echo "Syncing to ancalagon..."
ssh ancalagon "mkdir -p longbow_bench/bin longbow_bench/scripts"
rsync -avz --exclude='.git' ./ ancalagon:~/longbow_bench/
ssh ancalagon "mv ~/longbow_bench/bin/longbow-linux ~/longbow_bench/bin/longbow && mv ~/longbow_bench/bin/bench-tool-linux ~/longbow_bench/bin/bench-tool && chmod +x ~/longbow_bench/bin/* ~/longbow_bench/scripts/*"

# 2. Run local benchmarks (Sequential devices)
echo "Starting local benchmarks..."
(
    bash scripts/bench_matrix.sh cpu local 3000 9090
    bash scripts/bench_matrix.sh metal local 3000 9090
) > local_bench_run.log 2>&1 &
LOCAL_PID=$!

# 3. Run remote benchmarks (Sequential devices)
echo "Starting remote benchmarks on ancalagon..."
ssh ancalagon "cd ~/longbow_bench && bash scripts/bench_matrix.sh cpu ancalagon 3000 9090 && bash scripts/bench_matrix.sh cuda ancalagon 3000 9090" > remote_bench_run.log 2>&1 &
REMOTE_PID=$!

echo "Benchmarks are running in parallel. Local PID: $LOCAL_PID, Remote PID: $REMOTE_PID"
echo "Monitoring logs for errors..."

# Monitoring loop (simplified)
while kill -0 $LOCAL_PID 2>/dev/null || kill -0 $REMOTE_PID 2>/dev/null; do
    if grep -i "error" local_bench_run.log remote_bench_run.log | grep -v "0 errors"; then
        echo "WARNING: Errors detected in logs!"
        grep -i "error" local_bench_run.log remote_bench_run.log | tail -n 5
    fi
    sleep 30
done

echo "All benchmarks completed."
