#!/bin/bash
cd ~/longbow
source bench_venv/bin/python3
export LONGBOW_MAX_MEMORY=19327352832
python3 scripts/unified_benchmark.py --mode cuda --dims "128,384" --counts "500,1000" --dtypes "float32,float64" --memory 19327352832 --duration 30 --queries 1000 --label ancalagon_cuda