#!/bin/bash
set -e

# User Request Configuration
DIMS="128,384"
MEMORY=10737418240 # 10GB
QUERIES=500
DURATION=30
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant"

# Activate venv
source venv/bin/activate

echo "Starting Comprehensive Performance Benchmark Suite (RELIABILITY MODE)..."

# 1. GraphRAG Mode
for count in 500 1000 7000 25000 50000; do
    echo "Running GraphRAG count=$count..."
    python3 scripts/unified_benchmark.py --mode "graphrag" --dims "$DIMS" --counts "$count" --dtypes "$DTYPES" --memory "$MEMORY" --queries "$QUERIES" --duration "$DURATION" --timeout 1800 || echo "GraphRAG count=$count failed, continuing..."
done

# 2. Temporal Mode
for count in 500 1000 7000 25000 50000; do
    echo "Running Temporal count=$count..."
    python3 scripts/unified_benchmark.py --mode "temporal" --dims "$DIMS" --counts "$count" --dtypes "$DTYPES" --memory "$MEMORY" --queries "$QUERIES" --duration "$DURATION" --timeout 1800 || echo "Temporal count=$count failed, continuing..."
done

# 3. CPU Mode
python3 scripts/unified_benchmark.py --mode "cpu" --dims "$DIMS" --counts "500,1000,7000,25000,50000" --dtypes "$DTYPES" --memory "$MEMORY" --queries "$QUERIES" --duration "$DURATION" --timeout 1800 || echo "CPU matrix failed, continuing..."

# 4. Metal Mode
python3 scripts/unified_benchmark.py --mode "metal" --dims "$DIMS" --counts "500,1000,7000,25000,50000" --dtypes "$DTYPES" --memory "$MEMORY" --queries "$QUERIES" --duration "$DURATION" --timeout 1800 || echo "Metal matrix failed, continuing..."

echo "Comprehensive Benchmark Suite Complete!"
