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

## Suggestions for Next Release (from v0.2.0 Stabilization)

### 1. Ingestion Performance Recovery
- **Problem**: Ingestion throughput on Remote CPU dropped from 333k to 246k vec/s (-26%).
- **Hypothesis**: The migration of `ChunkedLocationStore` to `atomic.Value` for chunk headers adds overhead in the critical `AddBatch` path due to frequent `Store()` calls during growth and interface boxing.
- **Suggestion**: Implement a "Growth-only Mutex" or a read-copy-update (RCU) pattern specifically for the slice header to reduce atomic write frequency. Profile the `SharedWorkerPool` synchronization overhead under high-load parallel ingestion.

### 2. Adaptive GPU Offloading Thresholds
- **Problem**: Metal and CUDA acceleration show significant overhead for small datasets (1k vectors), underperforming CPU.
- **Observation**: Kernel launch latency and buffer synchronization dominate for small counts.
- **Suggestion**: Implement a dynamic dispatcher that keeps workloads on the CPU for `count < 5,000` or `dim < 384`, only activating GPU pipelines when the compute density justifies the transfer costs.

### 3. SIMD Activation Kernels (AVX-512 & NEON)
- **Problem**: Current activation functions (Exp, Log, Softmax) use generic Go fallbacks.
- **Observation**: High-dimensional search modes (GraphRAG, Temporal) spend significant time in these activations.
- **Suggestion**: Prioritize native assembly implementations for `Exp` and `Softmax` using AVX-512 `VEXP2PS` (or range reduction approximations) and NEON unrolled loops to reclaim the 20-30% performance gap in complex search modes.

### 4. Port Persistence Optimization
- **Observation**: Port binding conflicts during parallel benchmarking indicate that the server shutdown sequence may be trailing the benchmark runner.
- **Suggestion**: Implement a `GracefulShutdownWithTimeout` that ensures port release before the process exit, and add a randomized port fallback for benchmark environments.

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

---

## Suggestions for Next Release (from 2026-04-30 Matrix)

### 1. GraphRAG Search Optimization
- **Observation**: GraphRAG search throughput is significantly lower (~1k QPS) compared to Dense search (~3.7k QPS), despite shared traversal logic.
- **Analysis**: GraphRAG likely performs more complex neighbor expansions and metadata lookups per hop.
- **Suggestion**: Optimize the GraphRAG expansion loop with prefetching and consider caching intermediate expansion sets for frequently accessed "hub" nodes.

### 2. AVX-512 Assembly Refinement
- **Observation**: Build failures in AVX-512 assembly during this task indicate that the Plan 9 assembly syntax for masked instructions is fragile and error-prone.
- **Suggestion**: Shift towards using a high-level SIMD generator (like `avo`) or provide more robust unit tests specifically for the assembly kernels to catch syntax and operand ordering issues during CI.

### 3. Cross-Host Parallelism Efficiency
- **Observation**: Running benchmarks on `ancalagon` requires manual rsync and SSH coordination.
- **Suggestion**: Formalize the `scripts/bench_all.sh` into a proper benchmark controller that can handle remote orchestration, result collection, and automated pprof analysis (e.g., using a tool like `go-torch` or similar).
