#!/bin/bash
set -e

# Configuration
DIMS="128,384,768"
COUNTS="1000,5000,15000"
DTYPES="float32,turboquant"
MEMORY=19327352832
QUERIES=500
DURATION=20

source venv/bin/activate

# Ensure bin directory is in PATH or specified
export PATH=$PATH:$(pwd)/bin

# 1. CPU and Metal (Dense/Hybrid/Filtered/ByID)
echo "Running CPU/Metal core benchmarks..."
for mode in cpu metal; do
    python3 scripts/unified_benchmark.py --mode $mode --dims "$DIMS" --counts "$COUNTS" --dtypes "$DTYPES" --memory "$MEMORY" --queries "$QUERIES" --duration "$DURATION"
done

# 2. Specialized modes (Temporal, Geo, GraphRAG, Learned Index)
echo "Running Specialized benchmarks..."
for mode in temporal geo graphrag learned_index; do
    python3 scripts/unified_benchmark.py --mode $mode --dims "$DIMS" --counts "$COUNTS" --dtypes "$DTYPES" --memory "$MEMORY" --queries "$QUERIES" --duration "$DURATION"
done

echo "Benchmarks complete!"
