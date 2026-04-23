#!/bin/bash
set -e

# User Request Configuration
DIMS="128,384"
MEMORY=17179869184
QUERIES=500
DURATION=30
COUNTS="500,1000,7000,25000,50000,100000"
# All dtypes as requested: int,uint,float,complex and turboquant
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant"

# Activate venv
source venv/bin/activate

echo "Starting Comprehensive Performance Benchmark Suite (PRIORITY: GraphRAG & Temporal)..."

# Helper function to run a mode
run_mode() {
    local mode=$1
    echo "Running Mode: $mode"
    python3 scripts/unified_benchmark.py --mode "$mode" --dims "$DIMS" --counts "$COUNTS" --dtypes "$DTYPES" --memory "$MEMORY" --queries "$QUERIES" --duration "$DURATION" --timeout 1800
}

# 1. GraphRAG Mode (PRIORITY)
run_mode "graphrag"

# 2. Temporal Mode (PRIORITY)
run_mode "temporal"

# 3. CPU Mode (includes 4 vector search types: Dense, Hybrid, Filtered, ByID)
run_mode "cpu"

# 4. Metal Mode
run_mode "metal"

echo "Comprehensive Benchmark Suite Complete!"
