# Next Steps: GPU Support Implementation

## 15-Part Plan: Extend Longbow for CUDA and Metal GPU Support

### Part 1: GPU Build Tag System
- Create refined build tags: `gpu`, `gpu_cuda`, `gpu_metal`, `gpu_linux`, `gpu_darwin`
- Add `build/gpu.go` with build-time GPU detection using `//go:build` constraints
- Create `Makefile` targets: `build-cuda`, `build-metal`, `build-gpu`
- Add environment variable detection for `CUDA_HOME`, `FAISS_HOME`

### Part 2: GPU Configuration Package
- Create `internal/gpu/config.go` with:
  - `GPUBackend` type (CPU, CUDA, Metal, OpenCL)
  - `GPUConfig` struct extended with backend selection
  - `DetectGPUBackend()` function for runtime detection
  - `GetGPURequirements()` validation function
- Add validation for CUDA vs Metal backend availability

### Part 3: GPU Backend Abstraction
- Extend `internal/gpu/interface.go` with:
  - `Backend` method returning GPUBackend
  - `GetDeviceCount()` method
  - `GetDeviceInfo()` method
  - `GetMemoryInfo()` method
- Create `internal/gpu/cuda_backend.go` (Linux/CUDA specific)
- Create `internal/gpu/metal_backend.go` (macOS/Metal specific)
- Create `internal/gpu/cpu_backend.go` as fallback

### Part 4: CUDA/FAISS Integration Layer
- Refactor `internal/gpu/faiss_gpu.go`:
  - Remove placeholder/forward declarations
  - Add actual CGO bindings using `<faiss/gpu/GpuIndexFlat.h>`
  - Implement proper `#cgo LDFLAGS` with `-lfaiss_gpu -lcudart -lcublas -lcudart`
- Create `internal/gpu/cuda/cgo.go` for CUDA initialization:
  - `cudaInit()` - initialize CUDA runtime
  - `cudaGetDeviceCount()`
  - `cudaGetDeviceProperties()`
- Add CGO compiler flags for CUDA Toolkit paths

### Part 5: GPU Memory Allocator
- Create `internal/gpu/memory.go`:
  - `GPUMemPool` struct for GPU memory management
  - `AllocateGPU(size)` - allocate on GPU
  - `FreeGPU(ptr)` - free GPU memory
  - `MemcpyHostToDevice()` - transfer data to GPU
  - `MemcpyDeviceToHost()` - transfer data from CPU
- Add GPU memory limits and tracking
- Implement automatic cleanup with finalizers

### Part 6: GPU Index Factory
- Extend `internal/gpu/factory.go`:
  - `NewIndexWithBackend(cfg GPUConfig, backend GPUBackend) (Index, error)`
  - Auto-select backend based on config and availability
  - Graceful fallback to CPU if GPU unavailable
- Add backend priority: CUDA > Metal > CPU
- Support multi-GPU device selection

### Part 7: Vector Store GPU Integration
- Extend `internal/store/store.go`:
  - Add `gpuBackend GPUBackend` field
  - Add `gpuMemPool *gpu.GPUMemPool` field
  - `InitGPUBackend(backend GPUBackend, deviceID int) error`
- Update `internal/store/store_gpu.go`:
  - Call `NewIndexWithBackend()` instead of `NewIndex()`
  - Pass GPU memory pool to index
- Add GPU memory statistics to metrics

### Part 8: Hybrid GPU/CPU Search Pipeline
- Refactor `internal/store/hnsw_gpu.go`:
  - Implement `SearchHybrid(ctx, query, k)` properly
  - GPU candidate generation (top-k*10)
  - CPU refinement with HNSW graph traversal
  - Result merging and deduplication
- Add `HybridSearchConfig` struct:
  - `CandidateMultiplier` (default: 10)
  - `RefineTopK` (default: k)
  - `EnableGPUCache` for caching GPU results

### Part 9: GPU Index Synchronization
- Extend `internal/store/hnsw_gpu.go`:
  - Implement `SyncGPU(ids, vectors)` to keep GPU index updated
  - Batch GPU updates (not every vector, but in batches)
  - `FlushGPUUpdates()` for forcing sync
- Add synchronization options to `GPUConfig`:
  - `SyncBatchSize` (default: 1000)
  - `SyncInterval` (time-based)
- Add metrics for sync operations

### Part 10: GPU Error Handling & Fallback
- Create `internal/gpu/errors.go`:
  - `GPUNotAvailableError`
  - `GPUMemoryError`
  - `GPUInitializationError`
  - `GPUComputeError`
- Extend `internal/store/hnsw_gpu.go`:
  - Wrap all GPU operations in error handling
  - Automatic fallback to CPU on GPU errors
  - Log GPU errors with context
- Add circuit breaker for GPU operations

### Part 11: GPU Metrics & Monitoring
- Extend `internal/metrics/`:
  - `longbow_gpu_search_duration_seconds`
  - `longbow_gpu_memory_bytes` gauge
  - `longbow_gpu_sync_duration_seconds`
  - `longbow_gpu_fallback_total` counter
  - `longbow_gpu_index_size` gauge
- Add GPU device metrics (utilization, temperature if available)
- Create GPU Prometheus metrics endpoint

### Part 12: Apple Metal GPU Support
- Refactor `internal/gpu/metal_gpu.go`:
  - Extend to use `GPUMemPool` from Part 5
  - Implement proper Metal backend interface
  - Add Metal Performance Shaders for distance calc
- Implement `MetalIndex` with same interface as `FaissGPUIndex`
- Add Metal-specific optimizations (unified memory, compute kernels)
- Support Apple Silicon specific features (neural engine)

### Part 13: Cross-Platform GPU Detection
- Create `internal/gpu/detection.go`:
  - `DetectAvailableGPUs() []GPUInfo`
  - `GPUInfo` struct with name, memory, compute capability
  - `GetPreferredBackend() GPUBackend`
  - `ValidateBackend(backend GPUBackend) error`
- Add Linux CUDA detection:
  - Check for `/dev/nvidia*`
  - Check for `nvidia-smi` availability
  - Check for CUDA library paths
- Add macOS Metal detection:
  - Check for Metal framework
  - Check for Apple Silicon

### Part 14: GPU Resource Pooling
- Create `internal/gpu/pool.go`:
  - `GPUIndexPool` for reusing GPU indexes
  - `GetGPUIndex(config) (Index, error)`
  - `ReturnGPUIndex(index)` to pool
- Implement pool limits (max concurrent GPU operations)
- Add pool statistics (active, idle, waiting)
- Integrate with VectorStore lifecycle

### Part 15: Documentation & Testing
- Create `docs/gpu_setup.md`:
  - CUDA Toolkit installation instructions
  - FAISS GPU compilation steps
  - Environment variable setup
  - Build commands for each platform
- Create `docs/gpu_integration.md`:
  - API usage examples
  - Configuration options
  - Performance tuning guide
- Add GPU-specific tests:
  - `internal/gpu/cuda_test.go` (build tag `gpu_cuda`)
  - `internal/gpu/metal_test.go` (build tag `gpu_metal`)
  - `internal/store/hnsw_gpu_integration_test.go`
- Add benchmark suite for GPU vs CPU comparison

## Implementation Priority

### High Priority (Parts 1-6)
Build system, configuration, basic CUDA integration, GPU memory management, GPU index factory

### Medium Priority (Parts 7-11)
Vector store integration, hybrid search, monitoring, error handling

### Low Priority (Parts 12-15)
Metal support, detection, pooling, documentation

## Additional Notes

- Use `//go:build` tags appropriately for platform-specific code
- Ensure all GPU code has CPU fallbacks
- Add proper resource cleanup in defer statements
- Consider using `github.com/NVIDIA/go-nvml` for NVIDIA management
- For Metal, use `github.com/eiannoneg/go-metal` or direct CGO
- Test on actual hardware (NVIDIA GPU, Apple Silicon M1/M2/M3)
