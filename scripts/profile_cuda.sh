#!/bin/bash
# Profile CUDA kernels on ancalagon
set -e

cd ~/REPOS/longbow

PORT=3999
OUTDIR=/tmp/cuda_profile_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUTDIR"

# Build GPU test binary
echo "=== Building GPU test binary ==="
go test -tags="gpu,linux" -c -o "$OUTDIR/gpu_test_binary" ./internal/gpu/ 2>&1

echo "=== Testing CUDA detection ==="
./bin/bench-tool -mode info 2>&1 | grep -i cuda || true

# Profile with nsys (system-level)
echo "=== Starting nsys profile ==="
LONGBOW_GPU_ENABLED=true nohup ./bin/longbow-cuda --grpc-port $PORT > /tmp/ncu_srv.log 2>&1 &
SRV_PID=$!
sleep 4

nsys profile -o "$OUTDIR/nsys_cuda_search" -t cuda,nvtx \
  ./bin/bench-tool -mode vec -uri grpc://127.0.0.1:$PORT -dim 128 -dtype float32 -scale 1000 -queries 500 -workers 4 -search-modes dense 2>&1 | tee "$OUTDIR/nsys_output.log"

kill $SRV_PID 2>/dev/null
sleep 2

# Profile with ncu (kernel-level)
echo "=== Starting ncu profile ==="
LONGBOW_GPU_ENABLED=true nohup ./bin/longbow-cuda --grpc-port $PORT > /tmp/ncu_srv.log 2>&1 &
SRV_PID=$!
sleep 4

ncu --set full -o "$OUTDIR/ncu_cuda_kernel" -f --target-processes all \
  ./bin/bench-tool -mode vec -uri grpc://127.0.0.1:$PORT -dim 128 -dtype float32 -scale 1000 -queries 100 -workers 4 -search-modes dense 2>&1 | tee "$OUTDIR/ncu_output.log"

kill $SRV_PID 2>/dev/null

echo "=== Profiling complete. Results in $OUTDIR ==="
ls -la "$OUTDIR/"
