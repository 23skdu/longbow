#!/bin/bash
# Sequential Benchmark Runner for Longbow
# Ensures hardware isolation and clean metrics

source .venv/bin/activate
mkdir -p data/perf_logs/pprof

echo "Starting Pprof Collector..."
./scratch/pprof_collector.sh &
COLLECTOR_PID=$!

echo "Starting CPU Full Matrix..."
python3 scripts/unified_benchmark.py --mode cpu --dtypes float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant --dims 128,384,768 --counts 500,1000,3000,7000,15000,25000,50000,100000 --memory 19327352832 --duration 30 --timeout 14400

echo "Starting Metal Full Matrix..."
python3 scripts/unified_benchmark.py --mode metal --dtypes float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant --dims 128,384,768 --counts 500,1000,3000,7000,15000,25000,50000,100000 --memory 19327352832 --duration 30 --timeout 14400

echo "Starting GraphRAG Benchmark..."
python3 scripts/unified_benchmark.py --mode graphrag --dims 128,384,768 --counts 1000,10000 --memory 19327352832 --timeout 7200

echo "Starting Temporal Benchmark..."
python3 scripts/unified_benchmark.py --mode temporal --dims 128,384,768 --counts 1000,10000 --memory 19327352832 --timeout 7200

echo "All benchmarks complete. Killing collector..."
kill $COLLECTOR_PID
