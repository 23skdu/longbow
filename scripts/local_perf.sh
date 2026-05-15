#!/bin/bash
export LONGBOW_MAX_MEMORY=19327352832
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768,1024,3072"
COUNTS="5000,10000,25000,100000,250000"

echo "Starting Local CPU Benchmark..."
python3 scripts/unified_benchmark.py --mode cpu --dtypes $DTYPES --dims $DIMS --counts $COUNTS --search-modes all --label local_cpu > local_cpu.log 2>&1

echo "Starting Local Metal Benchmark..."
python3 scripts/unified_benchmark.py --mode metal --dtypes $DTYPES --dims $DIMS --counts $COUNTS --search-modes all --label local_metal > local_metal.log 2>&1
