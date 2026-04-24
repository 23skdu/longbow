#!/bin/bash
source bench_venv/bin/activate
export LONGBOW_MAX_MEMORY=18GB

DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant"
DIMS="128,384,768"
COUNTS="500,1000,3000,7000,15000,25000,50000"

echo "=== STARTING LOCAL BENCHMARKS (macOS Metal/CPU) ==="

# Standard (Vector + Hybrid)
echo "--- Running Standard CPU ---"
python3 scripts/unified_benchmark.py --mode cpu --dtypes $DTYPES --dims $DIMS --counts $COUNTS --queries 1000

echo "--- Running Standard Metal ---"
python3 scripts/unified_benchmark.py --mode metal --dtypes $DTYPES --dims $DIMS --counts $COUNTS --queries 1000

# Temporal
echo "--- Running Temporal ---"
python3 scripts/unified_benchmark.py --mode temporal --dtypes $DTYPES --dims $DIMS --counts $COUNTS

# Geo-spatial
echo "--- Running Geo-spatial ---"
python3 scripts/unified_benchmark.py --mode geo --dtypes $DTYPES --dims $DIMS --counts $COUNTS

# GraphRAG
echo "--- Running GraphRAG ---"
python3 scripts/unified_benchmark.py --mode graphrag --dtypes $DTYPES --dims $DIMS --counts $COUNTS

echo "=== LOCAL BENCHMARKS COMPLETE ==="
