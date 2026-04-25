#!/bin/bash
# scripts/redo_regressions.sh

set -e

# Configuration
MEMORY=19327352832

echo "[REDO] Starting regression verification..."

# 1. Local Metal float32 128d 1000
echo "[REDO] Local Metal float32 128d 1000"
python3 scripts/unified_benchmark.py --mode metal --dims 128 --counts 1000 --dtypes float32 --memory $MEMORY --duration 30 --queries 1000 --label "redo_local_metal_float32"

# 2. Ancalagon Failed Tests
echo "[REDO] Ancalagon CUDA Failed Tests"
ssh ancalagon "cd REPOS/longbow && \
python3 scripts/unified_benchmark.py --mode cuda --dims 128 --counts 500 --dtypes float16,int32,uint8,turboquant --memory $MEMORY --duration 30 --queries 1000 --label 'redo_ancalagon_cuda_failed_500' && \
python3 scripts/unified_benchmark.py --mode cuda --dims 128 --counts 5000 --dtypes float16 --memory $MEMORY --duration 30 --queries 1000 --label 'redo_ancalagon_cuda_failed_5000'"

echo "[REDO] Verification complete."
