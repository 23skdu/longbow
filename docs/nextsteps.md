# Longbow Development Roadmap

## Status: GPU Support Phase Complete

**Date Updated**: February 17, 2026

All 15 parts of the GPU Support Implementation plan have been completed. The codebase now has comprehensive GPU support for both NVIDIA CUDA (Linux AMD64) and Apple Metal (macOS ARM64) platforms.

---

## Completed Work Summary

### GPU Foundation (Parts 1-6) ✅
- Build tag system (`gpu`, `gpu_cuda`, `gpu_metal`, `gpu_linux`, `gpu_darwin`)
- Makefile targets: `build-cuda`, `build-metal`, `build-gpu`
- Environment variable detection (`CUDA_HOME`, `FAISS_HOME`)
- `GPUBackend` type and `GPUConfig` struct
- `DetectGPUBackend()` and `GetGPURequirements()` functions
- Backend abstraction interface with all required methods
- GPU memory pool structure with CPU fallback
- GPU index factory with `NewIndexWithBackend()`

### GPU Integration (Parts 7-11) ✅
- Vector store GPU integration (`InitGPUBackend`, `GetGPUBackend`)
- Hybrid GPU/CPU search pipeline with candidate generation and CPU refinement
- GPU index synchronization with batching (`SyncBatchSize`, `SyncInterval`)
- GPU error handling with typed errors (`GPUNotAvailableError`, `GPUMemoryError`, `GPUInitializationError`, `GPUComputeError`)
- GPU circuit breaker for automatic fallback
- GPU metrics and monitoring (Prometheus metrics for latency, memory, operations, fallbacks)

### Platform Support (Parts 12-14) ✅
- Apple Metal GPU support with unified memory optimizations
- Cross-platform GPU detection (`DetectAvailableGPUs`, `GetPreferredBackend`, `ValidateBackend`)
- GPU resource pooling with idle timeout and reuse

### Documentation & Testing (Part 15) ✅
- `docs/gpu_setup.md` - Complete installation and configuration guide
- `docs/gpu_integration.md` - API usage and performance tuning guide
- GPU-specific tests with appropriate build tags (`//go:build gpu`)
- Makefile build targets for CUDA and Metal

---

## Future Improvements

The following improvements are recommended for future development:

### 1. Real FAISS GPU CGO Bindings

**Status**: ⚠️ PARTIAL - Framework exists, needs library linking
**Location**: `internal/gpu/faiss_gpu.go`, `internal/gpu/interface.go`

The current implementation has:
- Full Go wrapper code implementing the Index interface
- CGO forward declarations for FAISS GPU functions
- Runtime FAISS library detection via `IsFAISSGPULibraryAvailable()`
- Clear error messages in `GetGPURequirements()` when FAISS is missing

**Remaining Work**:
- Requires linking against actual `libfaiss_gpu.so` library
- Build requires: `CGO_ENABLED=1` with FAISS GPU installed
- See `docs/gpu_setup.md` for installation instructions

**Implementation Notes**:
- Add proper `#cgo LDFLAGS` with `-lfaiss_gpu -lcudart -lcublas -lcudadevrt`
- Handle FAISS version compatibility (1.7.x vs 1.8.x)
- Consider using `github.com/DataIntelligenceCrew/go-faiss` as alternative

**Effort**: High (1-2 weeks) - requires FAISS GPU library setup
**Impact**: Enables actual GPU acceleration for vector search

---

### 2. Native CUDA Memory Management

**Status**: ✅ IMPLEMENTED
**Location**: `internal/gpu/memory_cuda.go`

Full CUDA memory management is implemented with:
- `cudaMallocWrap()` - GPU memory allocation via CGO
- `cudaFreeWrap()` - GPU memory deallocation
- `cudaMemcpyHtoD()`, `cudaMemcpyDtoH()`, `cudaMemcpyDtoD()` - Memory copy operations
- `cudaMemsetWrap()` - GPU memory initialization
- Integrated with `GPUMemPool` struct in `memory.go`

All functions use actual CUDA Runtime API calls through CGO.

---

### 3. Hybrid Search Cross-Encoder Scoring

**Status**: TODO in code
**Location**: `internal/store/hybrid_pipeline.go:348`

The hybrid pipeline currently has a placeholder for cross-encoder scoring:

```go
// TODO: Implement actual cross-encoder scoring
```

**Implementation Notes**:
- Integrate with ONNX Runtime or similar for neural re-ranking
- Support models like `cross-encoder/ms-marco-MiniLM-L-6-v2`
- Add batching for efficient GPU inference
- Cache encoder results for repeated queries

**Effort**: High (1-2 weeks)
**Impact**: Significantly improves search relevance for RAG applications

---

### 4. Concrete Quantizer Implementations

**Status**: ✅ IMPLEMENTED
**Location**: `internal/store/scalar_quantization.go`, `internal/store/binary_quantization.go`

Full implementations completed:
- **SQ8 (Scalar Quantization 8-bit)**: `SQ8Encoder` with per-dimension min/max bounds, Encode/Decode, distance functions
- **BQ (Binary Quantization)**: `BQEncoder` with 1-bit quantization (32x compression), Hamming distance
- **PQ (Product Quantization)**: Optimized implementation in `internal/pq/encoder.go`
- SIMD optimizations in `internal/simd/sq8.go`
- Comprehensive test suite in `generic_quantizer_test.go`

Memory savings: 4x for SQ8, 32x for BQ

---

### 5. Extended Chaos Testing & Validation

**Status**: Available but not executed
**Location**: `docs/next_steps.md`

The following validation tests have been prepared but not executed:

1. **24-Hour Extended Soak Test**
   - Script: `scripts/run_soak.sh` or `scripts/long_soak_local.sh`
   - Requires: I/O throttling setup (cgroups/tc)
   - Goal: Verify stability under sustained load

2. **Power-Loss Simulation**
   - Requires: External script integration
   - Goal: Verify WAL durability and recovery
   - Tool: `kill -9` with random timing

3. **Performance Characterization Report**
   - Generate comprehensive benchmark report
   - Compare GPU vs CPU performance across dimensions
   - Document latency percentiles (p50, p99, p999)

**Implementation Notes**:
- Set up dedicated test environment (Kubernetes cluster or bare metal)
- Configure I/O throttling: `cgcreate -g blkio:/throttled` and `tc qdisc`
- Automate power-loss testing with controlled VM shutdown
- Generate visualizations from metrics data

**Effort**: Medium (3-5 days)
**Impact**: Production readiness validation and performance documentation

---

## Implementation Priority

| Priority | Improvement | Status | Effort | Impact |
|----------|-------------|--------|--------|--------|
| High | Real FAISS GPU CGO Bindings | Pending | 1-2 weeks | Critical for GPU acceleration |
| Medium | Extended Chaos Testing | Pending | 3-5 days | Production readiness |
| Low | Cross-Encoder Scoring | Pending | 1-2 weeks | Search relevance improvement |
| - | Native CUDA Memory Management | ✅ Done | - | Required for GPU usage |
| - | Concrete Quantizer Implementations | ✅ Done | - | Memory optimization |

---

## Notes

- All GPU Parts 1-15 from the original plan are now complete
- The codebase has a solid foundation for GPU acceleration
- Remaining work focuses on linking against actual GPU libraries and production validation
- Consider using `github.com/NVIDIA/go-nvml` for enhanced NVIDIA GPU management

---

*Last Updated: February 17, 2026*
