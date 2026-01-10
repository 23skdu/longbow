#!/bin/bash
set -e
export LONGBOW_HNSW_FLOAT16_ENABLED=true
# Clean data
rm -rf data/node1

echo "Starting Longbow with Float16 Enabled..."
bash scripts/start_one_node.sh &
PID=$!
echo "Script PID: $PID"

# Wait for startup
sleep 15

echo "Checking Python dependencies..."
python3 -c "import pyarrow" || pip install pyarrow numpy pandas

echo "Running Benchmark..."
python3 scripts/benchmark_12gb.py

echo "Cleaning up..."
kill $PID || true
# Ensure binary is killed
pkill longbow || true
