# Next Steps & Roadmap

Based on benchmark results from 2026-09-11, updated 2026-09-12.

## 10-Part Improvement Plan

### Part 1: GPU TurboQuant Batched Kernel
**Goal**: Replace serial per-page kernel launch loop with a single batched kernel.
**Problem**: `SearchTurboQuant` (`cuda_index.go:1271-1319`) launches one kernel per page with synchronous memcpy — 98 round-trips at 100k. FP32 path uses `launch_l2_distance_kernel_v2_batched` for all pages in one launch.
**Action**: Create `turboquant_distance_kernel_v2_batched` mirroring the FP32 batched pattern: page pointer arrays + single kernel launch. Eliminates PCIe round-trip bottleneck.
**Impact**: Estimated 2-5x improvement for GPU TQ at 100k+ scale.

### Part 2: GPU TurboQuant Lookup Tables
**Goal**: Replace per-call `sincosf()` with pre-computed sin/cos lookup tables.
**Problem**: `kernels.cu:748-773` — thread 0 performs 127 sequential `sincosf()` calls while 255 threads idle. CPU path uses `tqLookup4`/`tqLookup8` tables (`turboquant.go:48-61`).
**Action**: Port CPU lookup table logic to CUDA shared memory. Pre-compute sin/cos for all quantization levels at kernel launch, store in `s_recon` staging area.
**Impact**: Eliminates single-thread reconstruction bottleneck, reduces per-vector latency.

### Part 3: GPU TurboQuant HNSW Graph Traversal
**Goal**: Implement greedy descent kernel for TQ instead of brute-force scan.
**Problem**: `cuda_index.go:2172-2183` — `SearchGreedy` returns entry point unchanged. GPU TQ brute-forces all N vectors; CPU HNSW visits O(ef·logN) nodes.
**Action**: Implement `turboquant_greedy_descent_kernel` using the existing `graph_bfs_expand_kernel` pattern. Reconstruct TQ vectors on-device, compare against query, follow HNSW graph edges.
**Impact**: Reduces GPU TQ search from O(N) to O(ef·logN), massive improvement at 100k+.

### Part 4: GPU TurboQuant Async Streams
**Goal**: Use explicit CUDA streams instead of nil (default stream).
**Problem**: `kernels.cu:1300` passes `nil` stream, forcing default-stream synchronization. FP32 path uses `idx.handle.streams[0]` for async operation.
**Action**: Pass explicit stream from handle to TQ kernel launch. Enable overlap of kernel execution and PCIe transfers.
**Impact**: 20-40% improvement from async kernel/memcpy overlap.

### Part 5: GPU TurboQuant Memory Coalescing
**Goal**: Align TQ packed stride to warp boundaries (32 bytes).
**Problem**: `kernels.cu:737-742` — TQ stride is 84 bytes (not power-of-2), causing uncoalesced warp memory access. `PackedSize` pads to 4 bytes only.
**Action**: Increase padding to 32-byte warp-aligned boundaries. Update `PackedSize` in `turboquant.go:293` to round up to 32-byte alignment.
**Impact**: Improved L2 cache utilization, reduced memory stalls at 100k+ scale.

### Part 6: CPU Temporal emlgo Investigation
**Goal**: Understand why CPU temporal regresses -18-36% with emlgo while GPU temporal improves +3-18%.
**Problem**: All dtypes at 10k/100k show CPU temporal regression with emlgo. GPU temporal consistently faster. Same emlgo dispatch, different results.
**Action**: Profile CPU temporal path vs GPU temporal path. Compare dispatch overhead, memory access patterns, and goroutine scheduling. Port GPU temporal optimization patterns to CPU.
**Impact**: Potential 15-30% improvement for CPU temporal workloads.

### Part 7: CPU Float64 emlgo Exclusion
**Goal**: Prevent emlgo from degrading float64 at 500k scale.
**Problem**: Float64 500k shows -21% dense, -41% sparse, -38% temporal with emlgo. Root cause: binary size + GC interaction, not code path difference.
**Action**: Add float64 exclusion guard in `tensor/math_dispatch_emlgo.go` — route float64 operations to standard math when dimensions exceed threshold. Or tune `GOGC` for emlgo builds.
**Impact**: Eliminates float64 500k regression (~47% memory savings).

### Part 8: GPU Complex128 Dense 100k Profiling
**Goal**: Diagnose -50% regression for GPU complex128 dense at 100k (3419 vs 1718 QPS).
**Problem**: Only affects complex128 at 100k on GPU. Other scales and types are fine.
**Action**: Profile GPU kernel for complex128 at 100k — check shared memory usage, register pressure, occupancy. Compare with complex64 which doesn't show this regression.
**Impact**: Recovery of 1700 QPS for complex128 workloads.

### Part 9: Benchmark Infrastructure
**Goal**: Achieve statistical confidence and detect regressions early.
**Problem**: Current benchmarks are single-run. Non-monotonic scaling observed (float32 dense: 1244 QPS at 100k, 3398 at 500k).
**Action**: Implement 3x benchmark runs with mean/stdev reporting in `unified_benchmark.py`. Add memory soak test (1+ hour at 500k). Add emlgo benchmarks to CI with regression threshold alerts.
**Impact**: Reliable performance tracking, early regression detection.

### Part 10: Conditional Dispatch Strategy
**Goal**: Route emlgo selectively by type and scale to maximize wins and minimize regressions.
**Problem**: Emlgo helps complex128/turboquant but hurts int8/float16 at small scale. No single build wins everywhere.
**Action**: Implement runtime dispatch in `tensor/math_dispatch_emlgo.go`: emlgo for complex types + turboquant above 50k vectors; standard for int/float below 50k. Add `LONGBOW_MATH_DISPATCH` env var for manual override.
**Impact**: Net positive across all dtype/scale combinations.

## Resolved

| Priority | Issue | Before | After | Fix | Date |
|----------|-------|--------|-------|-----|------|
| P0 | CPU float32 dense 50k | -63% | -2.5% | Dispatch overhead fix | 2026-09-11 |
| P0 | CPU turboquant dense 500k | -55% | -2.6% | PackedSize caching fix | 2026-09-11 |
| P0 | GPU turboquant 100k dense (emlgo) | -57% (1363 QPS) | +1% (3135 QPS) | QJL correction asymmetry bug | 2026-09-12 |
| P1 | CPU complex64 dense 10k | +10% | +0.02% | (natural resolution) | 2026-09-11 |
| P1 | Duplicate Dockerfile | Dockerfile == Dockerfile.cpu | Removed duplicate | Removed Dockerfile | 2026-09-12 |
| P1 | Dockerfile.metal missing ENTRYPOINT | No entrypoint | Added ENTRYPOINT | Fixed Dockerfile.metal | 2026-09-12 |
| P1 | docker-compose deprecated version key | version: '3.8' | Removed | Fixed docker-compose.yml | 2026-09-12 |
| P1 | Docker builds VCS error | No -buildvcs=false | Added -buildvcs=false | Updated all Dockerfiles | 2026-09-12 |

## Open Issues

### P0 — Critical

| # | Issue | Impact | Recommended Action |
|---|-------|--------|--------------------|
| 1 | CPU complex64 dense 500k | -38% regression (404 vs 251 QPS) | Profile hot path at 500k scale — dispatch overhead returns at large N |
| 2 | CPU complex128 dense 500k | +21% gain but P99 75ms | Investigate tail latency — allocation or GC pressure at large scale |
| 3 | GPU uint8 graphrag 500k | +290% gain (4981 vs 1276 QPS) | Validate with 3x runs — breakthrough or measurement artifact |

### P1 — Important

| # | Issue | Impact | Recommended Action |
|---|-------|--------|--------------------|
| 4 | CPU emlgo temporal mode | -18-36% across ALL dtypes at 10k/100k | Compare CPU vs GPU temporal dispatch (see Part 6) |
| 5 | GPU complex128 dense 100k | -50% regression (3419 vs 1718 QPS) | Profile GPU kernel (see Part 8) |
| 6 | CPU float64 emlgo memory | +47% memory (10632 vs 7172 MB) | Exclude float64 from emlgo dispatch (see Part 7) |

### P2 — Improvement

| # | Issue | Impact | Recommended Action |
|---|-------|--------|--------------------|
| 7 | CPU sparse search with emlgo | -5-17% slower at 100k | Compare CPU vs GPU sparse dispatch — GPU emlgo is +3-16% faster |
| 8 | CPU 10k scale emlgo overhead | -15-26% on graphrag/temporal | Consider 50k minimum activation threshold |
| 9 | Benchmark variance | Non-monotonic scaling at 100k | 3x benchmark runs (see Part 9) |

## Timeline

| Week | Focus |
|------|-------|
| 1 | Parts 1-2: GPU TQ batched kernel + lookup tables |
| 2 | Parts 3-5: GPU TQ graph traversal + async streams + coalescing |
| 3 | Parts 6-8: CPU temporal investigation + float64 exclusion + complex128 profiling |
| 4 | Parts 9-10: Benchmark infrastructure + conditional dispatch |
