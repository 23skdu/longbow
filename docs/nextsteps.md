# Next Steps for Longbow (0.2.0 Roadmap)

This document outlines the active work items and planned features for the upcoming 0.2.0 release, derived from v0.1.9 performance benchmarks and profiling.

---

## Suggestions for Next Release (from 0.2.0-pre Benchmarks)

### P0: Resolve Search QPS Regressions
- **Investigation**: Dense and Temporal search QPS dropped by ~30% in v0.1.9. 
- **Hypothesis**: Contention on `insertMu` or overhead from `insertPool`. 
- **Action**: Implement fine-grained locking or lock-free reads for the index traversal path.

### P0: Stabilize Scheduler Latency
- **Observation**: `pprof` shows significant time in `runtime.findRunnable` and `runtime.mcall`.
- **Action**: Optimize goroutine lifecycle in `runIndexWorker` and `handleDoGetSearch`. Reduce the number of short-lived goroutines spawned per query.

### P1: Temporal Cache Stabilization
- **Observation**: Temporal QPS varies between 3k and 14k across identical runs.
- **Action**: Investigate cache eviction policy and ensure consistent pre-warming for temporal indices.

---

## 0.2.0 Roadmap - Core Features

### 1. TPU Production Implementation
- Move TPU index from experimental to production-ready.
- Implement missing TurboQuant and PQ operations for TPU.
- Optimize XLA kernels for high-dimensional vector search.

### 2. High-Performance Concurrency & Sharding
- **GPU Sharding / Multi-device**: Support for distributing workloads across multiple GPUs.
- **Advanced Graph Updates**: Support for dynamic graph updates in Metal Hybrid Index.

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
