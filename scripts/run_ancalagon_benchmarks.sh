#!/bin/bash
cd ~/longbow
source ./bench_venv/bin/activate
export LONGBOW_MAX_MEMORY=19327352832

# Run CPU and CUDA benchmarks in parallel
python3 scripts/unified_benchmark.py --mode cpu --dims "128,384,768,1024,3072" --counts "500,1000,5000,10000" --dtypes "float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8" --memory 19327352832 --duration 30 --queries 1000 --label ancalagon_cpu 2>&1 | tee data/perf_logs/ancalagon_cpu.log &

python3 scripts/unified_benchmark.py --mode cuda --dims "128,384,768,1024,3072" --counts "500,1000,5000,10000" --dtypes "float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8" --memory 19327352832 --duration 30 --queries 1000 --label ancalagon_cuda 2>&1 | tee data/perf_logs/ancalagon_cuda.log &

wait
echo "Done"