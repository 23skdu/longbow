# Longbow v0.2.0+ Roadmap: Performance & SIMD Hardening (COMPLETED)

This document outlines the completed implementation plan for high-performance SIMD infrastructure and system stability.

## Phase 1: Ingestion Backpressure Tuning & Cache Coherency ✅

**Goal**: Stabilize CPU metrics during ingestion bursts and eliminate false-sharing contention.

### Subtasks

- [x] **GCTuner Burst Mode**: Modified `internal/memory/gc_tuner.go` to support `IsBursting()` state.
- [x] **Indexing Backpressure**: Updated `SharedWorkerPool` to intercept `IsBursting()` and pause background tasks.
- [x] **Cache Coherency Padding**: Implemented `PaddedMutex` in `internal/store/types/graph_data.go` with 64-byte alignment.

---

## Phase 2: Reduction & Top-K Kernels (SIMD) ✅

**Goal**: Build horizontal reduction primitives and MatMul kernels required for fast vector expansions.

### Subtasks

- [x] **Scalar Fallbacks**: Implemented unrolled scalar loops for `ArgMax`, `ArgMin`, and `MatMul`.
- [x] **AVX2 Kernels**: Integrated Avo-generated assembly for x86_64.
- [x] **NEON Kernels**: Implemented high-performance NEON wrappers in `internal/simd/simd_arm64.go`.
- [x] **Blocked MatMul**: Implemented cache-friendly matrix multiplication in `internal/simd/simd_blocked.go`.

---

## Phase 3: Spatial & Temporal GPU Kernels ✅

**Goal**: Offload `HaversineBatch` and `NormBatch` into GPU engines.

### Subtasks

- [x] **Metal Shaders**: Added `haversine_batch` and `norm_batch_f32` to `graph_kernels.metal`.
- [x] **Metal Bindings**: Integrated new shaders into `MetalIndex`.
- [x] **CUDA Kernels**: Implemented corresponding kernels in `kernels.cu`.
- [x] **CUDA Bindings**: Exposed via CGO in `cuda_index.go`.

---

## Phase 4: Scale Testing & Documentation ✅

**Goal**: Validate performance and document the infrastructure.

### Subtasks

- [x] **Performance Validation**: Verified throughput (15k+ ops/sec) and search latency (17k+ searches/sec) on local hardware.
- [x] **Package Documentation**: Created `internal/simd/doc.go` with architectural summary.
- [x] **Temporal Validation**: Resolved regressions and verified temporal search hit rates.
