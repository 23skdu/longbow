#!/bin/bash
set -e

# Configuration
DIMS="128,384"
COUNTS="500,1000,3000,7000,15000,25000,50000,100000"
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant"
MEMORY=19327352832
QUERIES=500
DURATION=30

source venv/bin/activate

echo "Starting Full Performance Benchmark Suite..."
echo "1. Standard Vector Search (CPU)"
python3 scripts/unified_benchmark.py --mode cpu --dims $DIMS --counts $COUNTS --dtypes $DTYPES --memory $MEMORY --queries $QUERIES --duration $DURATION

echo "2. Temporal Search"
python3 scripts/unified_benchmark.py --mode temporal --dims $DIMS --counts $COUNTS --dtypes $DTYPES --memory $MEMORY --queries $QUERIES --duration $DURATION

echo "3. GraphRAG"
python3 scripts/unified_benchmark.py --mode graphrag --dims $DIMS --counts $COUNTS --dtypes $DTYPES --memory $MEMORY --queries $QUERIES --duration $DURATION

echo "4. Standard Vector Search (Metal)"
python3 scripts/unified_benchmark.py --mode metal --dims $DIMS --counts $COUNTS --dtypes $DTYPES --memory $MEMORY --queries $QUERIES --duration $DURATION

echo "Benchmark Suite Complete!"
