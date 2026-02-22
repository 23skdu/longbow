# Longbow Development Roadmap

## Status: GPU Complete | io_uring Under Investigation | Remaining: FAISS Library Linking & Cross-Encoder

**Date Updated**: February 21, 2026

### ✅ Recently Completed (February 2026)

1. **Custom Zero-Lock Zero-Copy io_uring Library** - ⚠️ UNDER INVESTIGATION
   - Location: `internal/iouring/` (10 files)
   - WAL Backend Integration: `internal/storage/wal_backend_arrow_iouring.go`
   - **Issue**: CQE completion returns zeros - struct alignment issue suspected
   - **Investigation Plan**: See [docs/iouring_investigation.md](./iouring_investigation.md)
   - Ring setup works, but I/O completions return UserData=0, Res=0

2. **All GPU Support (Parts 1-15)** - COMPLETE
   - Build tag system (`gpu`, `gpu_cuda`, `gpu_metal`)
   - Makefile targets: `build-cuda`, `build-metal`, `build-gpu`
   - CUDA memory management via CGO
   - GPU backend abstraction and detection
   - GPU metrics and monitoring

3. **Concrete Quantizer Implementations** - COMPLETE
   - SQ8 (Scalar Quantization 8-bit) - 4x memory savings
   - BQ (Binary Quantization) - 32x memory savings
   - PQ (Product Quantization)

---

## 🚧 Remaining Work for CUDA Vector Operations on Linux

### 1. Real FAISS GPU Library Linking (HIGH PRIORITY)

**Status**: ⚠️ PARTIAL - Framework exists, needs library linking  
**Location**: `internal/gpu/faiss_gpu.go`, `internal/gpu/interface.go`

**What's Done**:
- ✅ Full Go wrapper implementing the Index interface
- ✅ CGO forward declarations for FAISS GPU functions
- ✅ Proper `#cgo LDFLAGS`: `-lfaiss_gpu -lcudart -lcublas -lcudadevrt`
- ✅ Runtime FAISS library detection via `IsFAISSGPULibraryAvailable()`

**Remaining Work**:
1. Install FAISS GPU library on build system:
   ```bash
   # Option A: Conda (recommended)
   conda install -c conda-forge faiss-gpu
   
   # Option B: Build from source
   git clone https://github.com/facebookresearch/faiss.git
   cmake -DFAISS_ENABLE_GPU=ON -DCMAKE_CUDA_ARCHITECTURES="80;90" ..
   make -j
   ```

2. Set library paths:
   ```bash
   export CGO_CFLAGS="-I/path/to/faiss/include"
   export CGO_LDFLAGS="-L/path/to/faiss/lib -lfaiss_gpu -lcudart -lcublas"
   ```

3. Build with GPU support:
   ```bash
   make build-cuda
   # or
   CGO_ENABLED=1 go build -tags gpu -o bin/longbow-cuda ./cmd/longbow
   ```

**Effort**: Medium (2-3 days if FAISS library is available)  
**Impact**: Enables actual GPU-accelerated vector search (vs CPU fallback)

---

### 2. Hybrid Search Cross-Encoder Scoring (MEDIUM PRIORITY)

**Status**: ⚠️ TODO - Stub implementation exists  
**Location**: `internal/store/hybrid_pipeline.go:348`

Current stub:
```go
// CrossEncoderReranker is a stub implementation
func (r *CrossEncoderReranker) Rerank(ctx context.Context, q string, results []SearchResult) ([]SearchResult, error) {
    // TODO: Implement actual cross-encoder scoring
    return results, nil
}
```

**Implementation Options**:
1. **ONNX Runtime** (Recommended):
   - Export cross-encoder model to ONNX format
   - Use `github.com/microsoft/onnxruntime-go` for inference
   - Models: `cross-encoder/ms-marco-MiniLM-L-6-v2`

2. **TensorFlow/PyTorch via CGO**:
   - More complex, requires TF/PyTorch C libraries
   - Better for model fine-tuning scenarios

**Effort**: High (1-2 weeks)  
**Impact**: Significantly improves RAG search relevance

---

### 3. Extended Chaos Testing & Validation (LOW PRIORITY)

**Status**: Available but not executed  
**Location**: `scripts/run_soak.sh`, `scripts/long_soak_local.sh`

Tests prepared but not run:
1. 24-Hour Extended Soak Test
2. Power-Loss Simulation (WAL durability)
3. Performance Characterization Report

**Effort**: Medium (3-5 days)  
**Impact**: Production readiness validation

---

## Implementation Priority

| Priority | Task | Status | Effort | Impact |
|----------|------|--------|--------|--------|
| **HIGH** | FAISS GPU Library Linking | ⚠️ Partial | 2-3 days | Enables GPU acceleration |
| Medium | Cross-Encoder Scoring | ⚠️ TODO | 1-2 weeks | Search relevance |
| Low | Extended Chaos Testing | Pending | 3-5 days | Production readiness |

---

## Completed Work Summary

### Custom io_uring Library ✅
- `internal/iouring/syscall.go` - Direct syscall wrappers using `golang.org/x/sys/unix`
- `internal/iouring/ring.go` - Ring management with mmap
- `internal/iouring/sq.go`, `cq.go` - Lock-free submission/completion queues
- `internal/iouring/buffer_pool.go` - O_DIRECT aligned buffer pool
- `internal/iouring/arrow_writer.go` - Zero-copy Arrow IPC integration
- `internal/iouring/metrics.go` - Prometheus metrics
- `internal/storage/wal_backend_arrow_iouring.go` - WAL backend integration

### GPU Support ✅
- `internal/gpu/memory_cuda.go` - CUDA memory management (CGO)
- `internal/gpu/faiss_gpu.go` - FAISS GPU index wrapper
- `internal/gpu/detection.go` - GPU backend detection
- `internal/gpu/pool.go` - GPU resource pooling
- `internal/gpu/circuit_breaker.go` - Automatic fallback
- Makefile targets: `build-cuda`, `build-metal`, `build-gpu`

### Quantization ✅
- `internal/store/scalar_quantization.go` - SQ8 encoder
- `internal/store/binary_quantization.go` - BQ encoder
- `internal/pq/encoder.go` - Product quantization

---

## Notes

### Quick Start for CUDA Development

```bash
# 1. Install CUDA toolkit
sudo apt install nvidia-cuda-toolkit

# 2. Install FAISS GPU (choose one)
conda install -c conda-forge faiss-gpu
# OR build from source

# 3. Set environment
export CUDA_HOME=/usr/local/cuda
export CGO_ENABLED=1

# 4. Build
make build-cuda

# 5. Verify
./bin/longbow-cuda --gpu-info
```

### Testing io_uring

```bash
# Run io_uring tests
go test -v -tags=linux ./internal/iouring/...

# Run WAL backend tests
go test -v -tags=linux ./internal/storage/... -run IOUring
```

---

*Last Updated: February 21, 2026*
