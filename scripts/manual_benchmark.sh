#!/bin/bash
# Manual benchmark suite
set -e

# Allow failures to not stop the whole suite if possible? 
# Using set -e stops on error which is good for debugging, but maybe bad for partial results.
# Let's keep set -e for now.

SIZES=(3000 5000 9000 15000 25000)
DIM=384

mkdir -p benchmark_results

for size in "${SIZES[@]}"; do
    echo "----------------------------------------------------------------"
    echo "=== BENCHMARK: Size=$size ==="
    echo "----------------------------------------------------------------"
    
    echo "Stopping existing cluster..."
    pkill longbow || true
    sleep 5
    rm -rf data
    
    echo "Starting cluster..."
    ./scripts/start_local_cluster.sh
    echo "Waiting 30s for startup..."
    sleep 30
    
    echo "Running Local Benchmark (Load + Search)..."
    # Capture output to log for analysis
    source venv/bin/activate && python3 scripts/perf_test.py \
      --rows $size \
      --dim $DIM \
      --hybrid \
      --search \
      --json benchmark_results/res_${size}_local.json \
      --data-uri grpc://localhost:3000 \
      --meta-uri grpc://localhost:3001 || echo "Local Run Failed"
    
    echo "Running Global Benchmark (Search Only)..."
    source venv/bin/activate && python3 scripts/perf_test.py \
      --rows $size \
      --dim $DIM \
      --skip-data \
      --search \
      --global \
      --json benchmark_results/res_${size}_global.json \
      --data-uri grpc://localhost:3000 \
      --meta-uri grpc://localhost:3001 || echo "Global Run Failed"
      
    echo "Finished Size=$size"
done

echo "Suite Complete."
pkill longbow || true
