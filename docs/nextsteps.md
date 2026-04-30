# Next Steps for Longbow (0.2.0 Roadmap)

This document outlines the active work items and planned features for the upcoming 0.2.0 release, derived from v0.1.9 performance benchmarks and profiling.

---

## Suggestions for Next Release (from 0.2.0-pre Benchmarks)

### P0: SIMD & Acceleration (Performance Scaling)

- [x] **SIMD Kernel: Vectorized Type Conversion**: Implement hardware-accelerated kernels for converting all supported types (int8, uint8, fp16, etc.) to float32. (Implemented for NEON/ARM64, AVX2/x86_64, and AVX-512/x86_64).
- [ ] **SIMD Kernel: Math & Activations**: Implement vectorized `Exp`, `Log`, `Softmax`, and `Sigmoid` for NEON and AVX. (Currently using generic fallbacks).
- [x] **SIMD Kernel: Memory Pipeline**: Use non-temporal SIMD instructions (VMOVNTDQ/STNP) for zero-copy memory operations in `GraphData` chunk promotions. (Implemented for NEON/ARM64 and AVX2/AVX-512).
- [ ] **SIMD Kernel: Matrix & Reductions**: Implement vectorized `matmul`, `dot products` (beyond Euclidean), `sum`, `max`, and `min` for all supported SIMD architectures.
- [x] **SIMD Cross-Platform Parity**: Implement AVX2/AVX-512 counterparts for type conversions and memory pipeline to match ARM64 performance. (COMPLETED).

### P0: Resolve Search QPS Regressions

- [x] **Fine-grained locking for index traversal**: Removed redundant `insertMus` shard locks in `ArrowHNSW` ingestion path.

### P0: Stabilize Scheduler Latency

- [x] **Scheduler optimization**: Refactored `DoGet` and `DoGetPipeline` to use `SharedWorkerPool`. Eliminated `runIndexWorker` polling with `Notify()` signaling.

### P1: Temporal Cache Stabilization

- [x] **Temporal cache stabilization**: Optimized `TemporalResultCache` with LRU ($O(1)$) and `TemporalTree` with binary search ($O(\log N)$).
- [x] **Observability around contention**: Added labels and optimized instrumentation for `LockNode` spin cycles and `insertMus` wait times.

---

## 0.2.0 Roadmap - Core Features

### 1. TPU Production Implementation

- Move TPU index from experimental to production-ready.
- Implement missing TurboQuant and PQ operations for TPU.
- Optimize XLA kernels for high-dimensional vector search.

### 2. High-Performance Concurrency & Sharding

- **GPU Sharding / Multi-device**: Support for distributing workloads across multiple GPUs.
- **Advanced Graph Updates**: Support for dynamic graph updates in Metal Hybrid Index.
- **NVIDIA/CUDA Offloading**: Port Metal-optimized distance kernels (dim > 1024) and Top-K selection to CUDA (using CUB/Thrust) for Linux/NVIDIA environments.
- **Metal Performance Shaders (MPS)**: Leverage `MPS` for Apple Silicon to accelerate high-dimensional distance computation and batch processing.

### 3. Cross-Platform Support

- **Windows Port**: Bring Longbow to Windows environments (WSL2 and Native).
- **Advanced SIMD for all platforms**: Complete bit-packing for NEON TurboQuant.

### 4. Robustness & Stability

- **Comprehensive Fuzzing**: Expand fuzz tests for all index types (IVF, HNSW, TQ).
- **171 Skipped Tests**: Resolve and enable platform-specific tests that are currently skipped.

---

## Trigonometric SIMD Kernel Optimization (0.2.0)

**Expected Impact:** 10-50x for vectorized trig operations.

### Subtasks

- **AVX-512 (x86_64)**: Implement `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh` kernels.
- **AVX2 (x86_64)**: Implement 8-element float32 variants.
- **NEON (ARM64)**: Implement 4-element float32 kernels and `fastTanh` approximations.
- **Integration**: Add dispatch table entries and metrics.

---

## External Dependency Replacement Candidates (0.2.0)

We aim to reduce the external dependency surface to improve build times and security.

### HIGH Priority

- **Replace zerolog**: Implement `internal/logger` with a compatible API. (Estimated: 2 weeks)

### MEDIUM Priority

- **Replace joho/godotenv + envconfig**: Create `internal/env` for simplified environment management.

### LOW Priority

- **Replace klauspost/cpuid/v2**: Implement `internal/cpu` for hardware feature detection.
- **Replace gonum/v1/gonum**: Implement `internal/math/matrix` for specific matrix operations.

---

## Technical Debt & Feature Gaps

- **IVF-PQ Method Gaps**: Implement `makeClusterDists`, `decodeVector`, and `computeResidualScore` in `ivf_opq_index.go`.
- **Metal Hybrid Index**: Add missing `AddPQ`, `UpdateGraph`, and `GraphExpand` operations.
- **Tiled Batch Precision**: Resolve numerical precision differences in `EuclideanDistanceTiledBatch`.

---

## Suggestions for Next Release (v0.2.0 Roadmap)

### P0: Shared-Read Optimization for GraphData

- **Observation**: Even with single-clone-per-layer, large-scale ingestion (500k+) creates temporary memory spikes during the linkage phase.
- **Action**: Implement a lock-free or shared-read model for `GraphData` to avoid cloning entirely during bulk updates.

### P0: Recursive Filter Evaluator Safety

- **Observation**: `FilterEvaluator.Reset` was missing cases for boolean, int32, and uint64 types, and didn't recursively handle compound filters, leading to panics on batch size transitions.
- **Action**: Periodically audit the `Bind` and `Reset` paths in `filter_evaluator.go` to ensure parity with all supported Arrow types.

### P1: Adaptive Client Throttling

- **Observation**: Server-side backpressure logs (100+ queue length) effectively signal saturation.
- **Action**: Enhance the Flight API to return a formal `SHOULD_BACKOFF` signal, allowing clients like `bench-tool` to automatically adjust ingestion rates without polling `check_readiness`.

### P1: Index Worker Starvation Check

- **Observation**: High search load can sometimes starve index workers, leading to "Still indexing..." hangs.
- **Action**: Implement priority-based scheduling in the server's shared worker pool to ensure background indexing progress during heavy query bursts.
