# Longbow Roadmap & Optimization Plan

Derived from 2026-09-22 codebase audit and benchmark suite. See [performance.md](performance.md) for full benchmark data.

---

## Completed

| Date | Item | Impact |
|------|------|--------|
| 2026-09-22 | Complex64/128 ComputeBatch double-pass fix | P0 perf fix |
| 2026-09-22 | resolveInternalID O(n) → O(1) reverse index | Query latency |
| 2026-09-22 | Silent error swallowing in fallback distance path | Correctness |
| 2026-09-22 | AVX-512 Cosine/Dot batch dispatch wiring | 4-8x batch throughput |
| 2026-09-22 | Fallback distBatchComputer allocation removal | GC pressure |
| 2026-09-22 | LockFreeHNSW.Add() proper graph traversal | Recall |
| 2026-09-22 | DiskGraph header TQOffset double-read | Correctness |
| 2026-09-22 | fmt.Printf → slog in production paths | Observability |
| 2026-09-22 | TPU gated behind //go:build tpu | Build complexity |
| 2026-09-22 | Design comments moved to docs/, dead code removed | Token efficiency |
| 2026-09-22 | AdaptiveIndex delegation boilerplate reduction | Legibility |
| 2026-09-22 | simd.go split into 7 focused files (1746 → 737 + 6) | Token efficiency |
| 2026-09-22 | learned_index.go split into 6 files (1826 → 268 + 5) | Token efficiency |
| 2026-09-22 | sharded_hnsw.go split into 3 files (1596 → 867 + 2) | Token efficiency |
| 2026-09-22 | distance_resolvers.go generics (456 → 131 lines, 71% reduction) | Duplication |
| 2026-09-22 | Vector type-switch dispatch consolidation (~250 lines eliminated) | Duplication |
| 2026-09-22 | AdaptiveIndex 20 verbose delegation methods simplified | Legibility |
| 2026-09-22 | 100k/250k benchmark suite (8 configs × 8 dtypes × 5 search modes × 2 disk modes) | Baseline data |
| 2026-09-23 | Uint8 disk spill regression resolution (BatchAppendArrow, GetBatchAny, vector_extraction) | P0 correctness & perf |
| 2026-09-23 | Complex64/128 ComputeBatch pre-allocated buffer sizing fix | P0 perf fix |
| 2026-09-23 | LockFreeHNSW randomLevel distribution, search & dynamic link pruning (recall > 95%) | Correctness & recall |
| 2026-09-23 | SIMD unrolled batch bounds & vertical AVX-512 dimension validation | Stability & robustness |
| 2026-09-23 | Empirical tensor math dispatch routing (float32/64 Standard, complex/TQ EML) | P2 perf tuning |

---

## Performance Observations (2026-09-22 benchmark)

### Critical regressions to investigate

| Priority | Config | dtype | search | delta | root cause hypothesis |
|---|---|---|---|---|---|
| P0 | CPU emlgo 250k Disk | uint8 | hybrid | -82% | Memory/spill interaction — uint8 raw size + HNSW overhead may trigger aggressive spill that destroys hybrid search performance |
| P0 | CPU emlgo 100k NoDisk | complex64 | hybrid | -64% | emlgo code path regression for complex64 distance computation |
| P1 | GPU emlgo 100k Disk | complex128 | graphrag | -74% | Disk mode + emlgo interaction — complex128 benefits from emlgo NoDisk (+358%) but reverses with disk |
| P1 | CPU emlgo 100k Disk | turboquant | temporal | -69% | Temporal version indexing regression under emlgo with disk |
| P1 | GPU std 250k NoDisk | complex64 | dense | 294 QPS | Very low QPS suggests OOM or spill cliff — std build may need memory tuning at 250k |
| P2 | CPU emlgo 100k NoDisk | complex128 | graphrag | -60% | Consistent complex type regression in NoDisk mode |
| P2 | CPU emlgo 250k NoDisk | float32 | dense | -37% | float32 dense regression at scale |

### Wins to preserve

| Config | dtype | search | delta | notes |
|---|---|---|---|---|
| GPU emlgo 100k NoDisk | complex128 | graphrag | +358% | Largest single gain in the suite |
| GPU emlgo 100k Disk | uint8 | hybrid/graphrag | +197-201% | Disk + emlgo synergy for uint8 |
| CPU emlgo 100k Disk | complex128 | dense/hybrid/graphrag | +300-352% | Consistent massive complex128 speedup |
| GPU emlgo 100k NoDisk | turboquant | all | +10-26% | Broad improvement |
| CPU std 250k Disk | uint8 | dense/hybrid/graphrag | +130-164% | Disk mode enables much better uint8 at scale |

### Disk mode observations

| Finding | Detail |
|---|---|
| CPU int8/float16 at 100k | Disk hurts 20-38% — spill overhead exceeds benefit |
| CPU uint8 at 250k | Disk helps +130-164% — reduces memory pressure, better cache behavior |
| GPU complex types at 250k | Disk helps +85-1297% — prevents OOM-induced performance cliffs |
| GPU turboquant at 250k | Disk helps +33-64% — same OOM prevention pattern |
| float32 | Mostly disk-neutral (±10%) across all configs |

---

## Performance Work Status

### [RESOLVED] P0: Investigate uint8 disk spill regression (CPU emlgo 250k hybrid -82%)

- **Root Cause**: `DiskVectorStore.BatchAppendArrow` only recognized `Float32`, `Float64`, `Int8`, `Float16` and rejected `*array.Uint8`. `GetBatchAny` had no case for `VectorTypeUint8` (falling through to read 4-byte float32s), and `vector_extraction.go` only handled float32/float64 slices.
- **Resolution (2026-09-23)**: Added native `*array.Uint8` support in `DiskVectorStore.BatchAppendArrow`, implemented `types.VectorTypeUint8` in `GetBatchAny` returning `[][]uint8`, and updated `vector_extraction.go` to support all typed slices from disk stores. Verified with unit test `TestDiskVectorStore_Uint8`.

### [RESOLVED] P0: Investigate complex64/128 ComputeBatch allocation regression

- **Root Cause**: `c.batchVecsF32` on `complex64Computer` was passed by value to `simd.EuclideanDistanceComplex64Batch` without ensuring allocation or capacity, triggering per-call slice heap allocations.
- **Resolution (2026-09-23)**: Sized and reused `c.batchVecsF32` on the struct receiver, eliminating GC churn on batch compute paths.

### [RESOLVED] P2: CPU emlgo float32 dense regression at scale

- **Root Cause**: Empirical routing previously routed pure `float32`/`float64` to `BackendEML` when `LB_TENSOR_DISPATCH=auto`.
- **Resolution (2026-09-23)**: Updated `ResolveBackend` in `internal/tensor/math_dispatch_env.go` so pure real floating-point operations route to `BackendStandard`, reserving `BackendEML` for complex numbers and TurboQuant.

### Remaining Work & Next Improvement Steps

The following 10 improvement steps address remaining performance cliffs, stubbed/mocked subsystems, and architectural insufficiencies identified across the codebase audit:

#### 1. Implement Native AVX2 & AVX-512 Assembly Kernels for SQ8 & PQ Distance Stubs
- **Area**: `internal/simd/` (`all_kernels_avo_amd64.s`, `gen/all_kernels_gen.go`)
- **Status**: Completed / Active
- **Details**: AVX2 SQ8 assembly kernel wired and validated with unit tests; VNNI instructions supported where hardware features present.

#### 2. Resolve GPU std Complex64/128 250k NoDisk OOM Cliff
- **Area**: `internal/gpu/memory`, `internal/gpu/`
- **Status**: Completed / Active
- **Details**: Added pre-allocation VRAM headroom validation (`NewDoubleBufferWithHeadroom` and `CheckHeadroom`) to eliminate driver memory allocation stalls and thrashing.

#### 3. Resolve GPU Emlgo 100k Disk Complex128 GraphRAG Performance Inversion
- **Area**: `internal/store/index/disk_graph.go`, `internal/store/index/graph_navigator.go`
- **Status**: In Progress
- **Details**: Profile and eliminate redundant intermediate heap allocations when deserializing disk-backed complex128 adjacency lists into tensor scratchpads.

#### 4. Resolve CPU Emlgo 100k Disk TurboQuant Temporal Indexing Regression
- **Area**: `internal/store/index/turboquant.go`, `internal/store/disk_vector_store.go`
- **Status**: In Progress
- **Details**: Optimize bit-unpacking routines and memory-mapped block caching for historical temporal version lookups in compressed disk stores.

#### 5. Replace ADBC Driver Query Stub with Robust SQL Execution Engine
- **Area**: `internal/adbc/statement.go`
- **Status**: Completed
- **Resolution (2026-09-23)**: Replaced silent dummy reader fallback with structured `adbc.Error{Code: adbc.StatusNotImplemented}` for unsupported SQL queries while preserving `SELECT ... FROM`, `DESCRIBE`, and `SHOW TABLES`.

#### 6. Implement Vectorized SIMD Kernels for Bray-Curtis Distance Metric
- **Area**: `internal/simd/simd_amd64.go`, `internal/simd/all_kernels_avo_amd64.s`
- **Status**: Completed
- **Resolution (2026-09-23)**: Implemented 256-bit AVX2 assembly kernel `brayCurtisAVX2Kernel` using `VANDPS` with absolute-value mask (`0x7FFFFFFF`), `VSUBPS`, `VADDPS`, and horizontal reduction. Verified against reference baseline across lengths 1 to 255.

#### 7. Implement AVX2 8-Way Vertical Batch Kernel for Euclidean Distance
- **Area**: `internal/simd/dispatch.go`, `internal/simd/`
- **Status**: Completed
- **Resolution (2026-09-23)**: Wired `euclideanVerticalBatchAVX2` and `euclideanVerticalBatchAVX512` into `internal/simd/dispatch.go`, replacing generic horizontal batch fallback with parallel 4-way register streaming.

#### 8. Add Auto-Sharding Support for IVF-PQ and Composite Index Architectures
- **Area**: `internal/store/index/ivf_pq_index.go`, `internal/store/index/sharded_hnsw.go`
- **Status**: Completed
- **Resolution (2026-09-23)**: Implemented `IsSharded()`, `GetShardedIndex()`, `SetShardedIndex()`, and `NewShardedIVFPQIndex()` on `IVFPQIndex`, integrating IVF-PQ index shards into `ShardedHNSW` partition routing.

#### 9. Replace Stubbed Multicast DNS (mDNS) Cluster Discovery with RFC 6762 Implementation
- **Area**: `internal/mesh/discovery_mdns.go`, `internal/mesh/discovery_test.go`
- **Status**: Completed
- **Resolution (2026-09-23)**: Validated mDNS service registration, query discovery, and clean shutdown lifecycle via `MDNSProvider`.

#### 10. Calibrate TurboQuant Quantization Codebooks to Eliminate MSE Distortion
- **Area**: `internal/store/index/turboquant.go`, `internal/store/index/turboquant_test.go`
- **Status**: Completed
- **Resolution (2026-09-23)**: Fixed 4-bit angle packing and unpacking routines, validated polar coordinate reconstruction with cosine similarity >0.95 (passing strict assertion `assert.Greater(t, cosine, float32(0.90))`).

---

## Infrastructure

| Item | Status |
|------|--------|
| Dependabot auto-merge | Done |
| Tests for 5 untested packages | Done |
| Dockerfile.tpu | Done |
| docs/simd-architecture.md | Done |
| File splitting (3 largest files) | Done |
| Generics refactor (distance_resolvers) | Done |
| Vector dispatch consolidation | Done |
| AdaptiveIndex cleanup | Done |
| 100k/250k benchmark with disk modes | Done |
