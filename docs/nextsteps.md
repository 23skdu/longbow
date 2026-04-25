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

## Roadmap for 0.2.0

### 1. Full Test Coverage (95% Target)

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

## 0.2.0 Release Management

- [ ] **Versioning**: Transition to semantic versioning (SemVer) starting with 0.2.0.
- [ ] **Migration Guides**: Document breaking changes in disk format (if any) for 0.1.9 -> 0.2.0 upgrades.
- [ ] **Documentation**: Complete the API reference for all new hardware-specific flags.
