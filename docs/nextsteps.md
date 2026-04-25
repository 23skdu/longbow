# Next Steps for Longbow (Updated 2026-04-25)

## 0.1.9 Release Blockers (RESOLVED)

### CUDA Backend Stability & Performance

- [x] **Top-K Algorithm**: Implemented shared-memory atomic heap Top-K kernel in `kernels.cu`.
- [x] **Memory Isolation**: Refactored `CUDAIndexHandle` to use distinct buffers for FP32, FP16, PQ, and TQ to prevent corruption.
- [x] **PQ Data Integrity**: Implemented `AddPQ` and `cuda_add_vectors_pq` to correctly handle uint8 codes on the device.
- [x] **P0: Hardware-accelerated GraphRAG**: Native traversal of billion-scale knowledge graphs via CUDA/Metal kernels.
- [x] **P0: Adaptive Re-quantization**: Real-time memory and precision optimization based on query load and memory pressure.
- [x] **High-Dimension Optimizations**: Specialized SIMD kernels for 768, 1024, 1536, and 3072 dimensions across all architectures.

### Metal Backend Gaps

- [x] **PQ Implementation**: Fully implemented Metal ADC kernels and PQ interface in `MetalIndexOptimized`.
- [x] **Graph Kernels**: Metal kernels for BFS expansion and activation propagation.

### IVF-HNSW Composite Index

- [x] **Persistence**: Implemented full `Save`/`Load` logic including OPQ encoder and cluster serialization.
- [x] **State Management**: Implemented `ExportState`/`ImportState` for full index synchronization.

### General Hardening

- [x] **TPU Index Safety**: Explicitly flagged `TPUIndex` as an experimental stub.
- [x] **Test Placeholders**: Implemented real recall validation in `DualIndexHarness` and dependency injection tests for `ShardedHNSW`.
- [x] **Namespace Metrics**: Finished implementation of `NamespaceCreationTotal` metrics in `servers.go`.

## 0.1.9 Hardening & Performance (RESOLVED)

- [x] **SIMD Metadata Filtering for NEON**: Ported AVX-512 filters to ARM64 NEON with optimized assembly kernels for Int64, Int32, Float64, and Float32.
- [x] **Persistent HNSW Memory Mapping**: Transitioned HNSW graph storage to direct Arrow-backed memory mapping to eliminate indexing load times.
- [x] **Distributed GraphRAG Traversal**: Implemented cross-node BFS and activation propagation protocols for multi-billion node knowledge graphs.
- [x] **Autonomous efSearch Tuning**: Replaced adaptive heuristic with a PID-controller to auto-tune search depth per query based on recall targets.
- [x] **TurboQuant V2 (2-bit)**: Extended the quantization pipeline to support 2-bit packing for extreme memory compression (up to 64x).
- [x] **Native Float64/Complex128 SIMD**: Direct SIMD distance kernels for double-precision types, eliminating float32 conversion overhead.
- [x] **Vector Extraction Buffer Reuse**: Extended `sync.Pool` pattern to all vector extraction paths to reduce heap pressure.
- [x] **Zero-Copy SearchByID**: Optimized `SearchByID` to use Arrow-native slices directly for target vectors.

## Roadmap for 0.1.9 (Updated)


### 1. Full Test Coverage (95% Target)
...

Comprehensive hardening of core packages (`store`, `query`, `storage`) to meet enterprise stability standards.

- [ ] **Fuzzing**: Implement Go fuzz tests for the ingestion pipeline and HNSW graph mutations.
- [ ] **Concurrency Tests**: Add `sync/atomic` and `sync.RWMutex` stress tests for the `store` package under high read/write contention.
- [ ] **Mocking Framework**: Introduce mock network interfaces and disk layers to isolate `query` and `storage` package tests.
- [ ] **CI Enforcement**: Configure GitHub Actions to fail PRs that drop coverage below 95%.

### 2. TPU Support

Production-ready implementation of Google TPU-accelerated indexing and search.

- [ ] **XLA Kernels**: Write XLA compilation targets for HNSW graph traversal and IVF centroid assignments.
- [ ] **Memory Mapping**: Implement zero-copy tensor transfers to TPU memory using Arrow Flight.
- [ ] **TPU Orchestration**: Add Cloud TPU node discovery, orchestration, and health-checking mechanisms.
- [ ] **Feature Parity**: Replace the experimental `TPUIndex` stub with full `TrainPQ` and `Search` implementations.

### 3. GPU Sharding

Automated partitioning of billion-scale indices across multiple GPUs.

- [ ] **Partitioning Algorithm**: Design a cluster-aware index splitting algorithm (e.g., K-Means based graph partitioning).
- [ ] **Inter-Device Communication**: Implement multi-GPU peer-to-peer data transfers using NCCL (Nvidia) and RCCL (AMD).
- [ ] **Query Routing**: Build a fast dispatcher to route batched queries to the appropriate GPU shard.
- [ ] **Result Aggregation**: Implement an optimized device-to-host Top-K merge algorithm to combine shard results.

### 4. AMD GPU & CPU Support

Dedicated HIP/ROCm and AMD-optimized CPU backends for Longbow.

- [ ] **ROCm Kernels**: Port existing CUDA kernels (L2, Cosine, GraphRAG) to AMD HIP/ROCm.
- [ ] **Build Pipeline**: Adapt Makefile to output a `longbow-rocm` binary alongside the CUDA version.
- [ ] **Zen Optimization**: Fine-tune AVX2/AVX-512 SIMD assembly specifically for AMD Zen architecture cache-lines.
- [ ] **CI/CD Integration**: Add AMD GPU runners to the validation pipeline.

### 5. Windows Support

Native compilation and binary distribution for Windows environments.

- [ ] **Memory Mapping**: Port Unix-specific `mmap` calls to Windows `MapViewOfFile` for zero-copy file access.
- [ ] **CGO Portability**: Remove `pthreads` and `sys/mman.h` assumptions from CGO kernels, falling back to cross-platform abstractions.
- [ ] **Path & FS Fixes**: Ensure all filesystem operations properly handle Windows path separators and file locking limits.
- [ ] **Packaging**: Create an MSI installer, and potentially Scoop/Chocolatey manifests for easy installation.

### 6. PiZero & Edge Optimization

Full completion of low-memory, low-power CPU-only coverage for Raspberry Pi Zero (ARMv6/v7/v8) and other edge devices.

- [ ] **Memory Caps**: Implement strict memory-mapped constraints for devices with 512MB RAM (`LONGBOW_LOW_MEM=1`).
- [ ] **ARMv6 Fallbacks**: Ensure numerical stability and fast fallback loops for devices lacking advanced NEON/SIMD capabilities.
- [ ] **Binary Size Reduction**: Implement build flags to strip symbols, optionally remove CGO, and reduce the binary footprint to < 20MB.
- [ ] **Edge Containers**: Publish static, minimal Scratch/Alpine-based Docker images specifically tagged for `arm32v6`.

## Architecture & Planning (For Delegation)

### GPU Sharding Design Doc

- **Scope**: Support indices larger than a single GPU's VRAM (e.g., 50GB index on 4x 16GB GPUs).
- **Partitioning Strategy**: Use a global IVF centroids set; each GPU holds a subset of the Voronoi cells.
- **Search Flow**: Client -> Router (CPU) -> Parallel Search (All GPUs) -> Merge & Sort (CPU) -> Client.
- **Dependency**: Must implement NCCL/RCCL bindings for fast intra-node transfers during index build.

### TPU Search Pipeline

- **Core Challenge**: XLA requires static shapes.
- **Approach**: Batch queries into fixed buckets (e.g., 1, 8, 32, 64) to maximize TPU throughput.
- **Data Layout**: Pad vectors to TPU-friendly alignment (typically 128 bytes).

## 0.1.9 Release Management

- [ ] **Versioning**: Transition to semantic versioning (SemVer) starting with 0.1.9.
- [ ] **Migration Guides**: Document breaking changes in disk format (if any) for 0.1.8 -> 0.1.9 upgrades.
- [ ] **Documentation**: Complete the API reference for all new hardware-specific flags.

### Suggestions for Next Release

1. **NUMA-Aware Parallel Refinement**: Pin parallel search workers to specific CPU cores matching the NUMA node where the Arrow RecordBatches are stored, reducing cross-socket memory latency.
2. **Zero-Alloc Response Building**: Extend the zero-allocation philosophy to the response serialization path. Currently, converting Arrow RecordBatches to JSON or Flight responses involves significant heap allocations.
3. **Kernel Fusing for GraphRAG**: Optimize GraphRAG performance by fusing activation calculation and graph traversal kernels into a single GPU dispatch.
4. **Asynchronous Index Compaction**: Move HNSW graph compaction and level-balancing to a background priority-throttled thread.
5. **ARM64 NEON SIMD Optimization**: The Darwin arm64 CPU benchmarks show significant room for improvement in high-dimension (768, 1024, 3072) searches. Consider adding NEON SIMD kernels for float32/float64 distance calculations.
6. **Metal Ingest Acceleration**: Metal shows lower ingest rates than CPU for high-dim vectors. Investigate Metal-specific ingest kernels to improve throughput.
7. **Complex Type Support**: Complex64 and complex128 show no search results - these types need optimized distance kernels.
8. **TurboQuant Search Optimization**: turboquant2/4/8 variants show lower QPS than expected. Investigate bit-pack search kernels.
9. **Large Batch Search Optimization**: 100k+ vector searches show significant performance drop-off. Consider batched search optimizations for large datasets.
10. **Learned Index Activation**: Ensure the k-NN classifier is being activated in production - current results show 0 predictions.

## 0.2.0 Roadmap

### 1. Large Batch Search Optimization (100k+ vectors)

Design and implement batched search optimizations for billion-scale datasets.

#### Problem Analysis

- Current single-query path doesn't scale to 100k+ vector batches
- Memory bandwidth saturation at high batch sizes
- SIMD utilization drops with non-aligned batch sizes
- GPU search kernels underperform due to launching overhead

#### Proposed Architecture

- **Hierarchical Batching**: Divide large queries into hierarchical batches (1k → 10k → 100k → 1M vectors)
  - Level 1: Cache-friendly 1k vector batches with SIMD prefetch
  - Level 2: Thread-block optimized 10k batches
  - Level 3: Multi-GPU sharded 100k+ batches
  
- **Streaming Distance Computation**:
  - Process vectors in stream (pipeline) rather than all-at-once
  - Use ring buffer for distance accumulation to reduce peak memory
  - Implement early termination with distance thresholds
  
- **GPU Batch Kernels**:
  - Vectorized batch kernel launch (single dispatch per batch)
  - Shared memory reduction for Top-K per thread block
  - Multi-pass merging for global Top-K aggregation
  
- **Adaptive Batch Sizing**:
  - Auto-tune batch size based on vector dimension and available memory
  - Dynamic adjustment based on observed throughput

#### Implementation Phases

- [ ] **Phase 1 (v0.2.0-alpha)**: CPU batch search with hierarchical batching and SIMD unrolling
- [ ] **Phase 2 (v0.2.0-beta)**: GPU batch kernels with shared memory reduction
- [ ] **Phase 3 (v0.2.0-stable)**: Multi-GPU sharding for 1M+ vector batches
- [ ] **Phase 4 (v0.2.0)**: Adaptive batch sizing and early termination

#### Dependency

- Requires completion of GPU sharding (GPU Sharding section above)

### 2. ARM64 NEON SIMD Kernels (768/1024/3072 dimensions)

Implementation completed in v0.1.9 - added optimized blocked NEON kernels for float32.

#### P0: ARM64 NEON Float64 Kernel (BUG - INVESTIGATION REQUIRED)

**Issue**: Go slice headers not being accessed correctly in `euclideanFloat64NEONKernel` assembly.

**Current Status**: Temporarily disabled - uses fallback to `euclideanFloat64Unrolled4x`.

**Symptoms**: 
- Kernel returns 0 for all inputs despite correct computation in Go wrapper
- Debug prints show function is called with correct length
- Slice headers (`a_base`, `b_base`, `a_len`) don't seem accessible

**Comparison with Working Code**:
```go
// Working float32 kernel uses slice header access:
TEXT ·euclideanNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0   // a_base
    MOVD    a_len+8(FP), R1    // a_len  
    MOVD    b_base+24(FP), R2  // b_base
```

**Attempted Solutions** (all failed):
1. Direct slice parameters with `.base`/`.len` offset access
2. Changed to unsafe.Pointer + explicit length parameter
3. Used post-increment (`FMOVD.P`) vs pre-indexed (`FMOVD`) addressing

**Investigation Required**:
- Verify Go slice ABI for float64 vs float32 on ARM64
- Test with pure indexed addressing (no post-inc)
- Consider using C wrapper to verify ABI compatibility
- Reference: https://go.dev/arch/ARM64

**Workaround**: Uses `euclideanFloat64Unrolled4x` fallback

### 3. Metal Ingest Acceleration

- [ ] **Metal Ingest Kernels**: Add dedicated Metal kernels for vector ingestion
- [ ] **Batch Encoding**: Implement batched PQ/TurboQuant encoding on Metal
- [ ] **Async Copy**: Use Metal async compute for zero-blocking ingest

### 4. Complex Type Support (COMPLETED v0.1.9)

Complex64/Complex128 kernels already implemented in v0.1.9:
- Added `euclideanComplex64Optimized` for float32 path
- Added `euclideanComplex128Unrolled` for float64 path
- Verified search paths in `arrow_hnsw.go`

### 5. TurboQuant Bit-Pack Optimization

- [ ] **Bit-Pack Distance Kernels**: Implement SIMD bit-pack distance for 2/4/8-bit TQ
- [ ] **Lookup Table**: Use precomputed distance tables for bit operations
- [ ] **Vectorized Comparison**: Add SIMD bitwise comparison for TQ search
