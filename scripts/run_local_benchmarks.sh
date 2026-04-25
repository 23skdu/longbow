#!/bin/bash
# Benchmark runner for local CPU and Metal (runs in background)
cd /Users/rsd/REPOS/longbow

export LONGBOW_MAX_MEMORY=19327352832

# CPU mode - smaller matrix for speed
echo "Starting CPU benchmarks..."
source scripts/venv/bin/activate
python3 -c "
import subprocess, sys, os
os.environ['LONGBOW_MAX_MEMORY'] = '19327352832'

configs = [
    ('128,384', '500,1000,5000,15000,50000,100000', 'float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8'),
    ('768,1024,3072', '500,1000,5000,10000,20000', 'float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8'),
]

for dims, counts, dtypes in configs:
    cmd = f'python3 scripts/unified_benchmark.py --mode cpu --dims {dims} --counts {counts} --dtypes {dtypes} --memory 19327352832 --duration 30 --queries 1000 --label local_cpu_{dims}"
    print(f'Running: {cmd}')
    subprocess.run(cmd, shell=True)
"

echo "CPU benchmarks complete. Starting Metal..."

source scripts/venv/bin/activate
python3 -c "
import subprocess, sys, os
os.environ['LONGBOW_MAX_MEMORY'] = '19327352832'

configs = [
    ('128,384', '500,1000,5000,15000,50000,100000', 'float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8'),
    ('768,1024,3072', '500,1000,5000,10000,20000', 'float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8'),
]

for dims, counts, dtypes in configs:
    cmd = f'python3 scripts/unified_benchmark.py --mode metal --dims {dims} --counts {counts} --dtypes {dtypes} --memory 19327352832 --duration 30 --queries 1000 --label local_metal_{dims}'
    print(f'Running: {cmd}')
    subprocess.run(cmd, shell=True)
"

echo "All local benchmarks complete!"
date