# Longbow Performance Optimization Plan

**Date**: 2026-03-27
**Platform**: Apple M3 Pro (Bahamut), macOS ARM64 + Linux (Ancalagon)

---

## Completed Bottlenecks

| Bottleneck | Worst Config | Status | Notes |
|------------|-------------|--------|-------|
| Dimension scaling | turboquant/float32 128→384 | ✅ DONE | Blocked processing for 384 dims |
| P50 latency cliff | turboquant @ dim=384/5k | ✅ DONE | Cached rotated query in search context |
| Metal GPU Stability| High-load sustained benchmarks | ✅ DONE | Resource limits and CPU fallback |
| TurboQuant ARM64 | NEON L2 and FWHT | ✅ DONE | Full NEON optimized rotation |
| Zero-Copy Hot Paths| Filter bitset caching | ✅ DONE | Zero-copy filter bitset reuse |
| IPC Reliability | Nil/Empty record handling | ✅ DONE | Integration tests for edge cases |
| Concurrency Safety| Search/Ingestion race fixes | ✅ DONE | Fixed SearchHybrid deadlock and races |
| complex64/complex128 ByID | Dimension mismatch @ 10k+ | ✅ DONE | DataType not propagated during ShardedHNSW migration |

---

## Recommendations for Next Steps

### 1. High Priority

**a. turboquant Hybrid Search Optimization**

- Current: Hybrid QPS drops to 776 at dim=384/count=10k vs 1,476 Dense QPS
- Root cause: Sparse vector computation overhead in hybrid path
- Recommendation: Add batch sparse vector computation, similar to Metal batch search

**b. Metal Server Stability (Audit)**

- Current: Basic resource limits and graceful fallback implemented.
- Recommendation: Perform long-duration soak testing to ensure no gradual leaks.

**c. Integer Overflow Warnings (gosec G115)**

- Fixed 10 HIGH severity issues in core arena/pointer conversions.
- 181 issues remain (mostly lower severity).
- Recommendation: Systematically apply the established bounds-check pattern to remaining issues.

**d. Race Condition Testing**

- `go test -race` takes >5 minutes on store package
- Recommendation: Run focused race tests on critical paths (search, ingestion, sharding) rather than full suite

### 2. Medium Priority

### 2. Medium Priority

**a. gosec Issues**

- 493 total issues (mostly G104: unhandled errors)
- Low severity - mostly test/benchmark code
- Recommendation: Continue systematic error handling in tools; fixed G104/G115/G404 in bench-tool.

**b. Documentation**

- Update performance.md with complete 72-config results
- Add architecture diagram for Arrow zero-copy flow
- Document Metal GPU integration patterns

**c. Full ARM64 NEON for TurboQuant**

- ✅ DONE: Optimized distance kernels and Fast Walsh-Hadamard Transform (FWHT) for ARM64.
- Resolved Go assembler instruction issues for vector FADD/FSUB.

---

## Completed Work (Reference)

The following optimizations were implemented in the March 2026 performance sprint:

| Part | Description | Impact |
|------|-------------|--------|
| 1 | PrefetchLimit: 8→dynamic mMax | Cache locality improvement |
| 2 | turboquant math.Sincos optimization | 2,500-3,500 QPS |
| 3 | PrimaryIndex async update outside lock | DoPut throughput |
| 4 | Parallel dense/sparse in hybrid search | Hybrid QPS |
| 5 | Pre-filter for filtered search | ~10% overhead |
| 8 | Complex SIMD kernels | complex64: 7,900-8,400 QPS |
| 10 | CI benchmark workflow | Regression detection |
| Dimension | Blocked SIMD for 384 dims on ARM64 | Cache locality |

**Files changed:**

- `internal/store/arrow_hnsw.go` — prefetchLimit
- `internal/store/store_actions.go` — async PrimaryIndex
- `internal/store/dataset.go` — primaryIndexMu
- `internal/store/hybrid_search.go` — parallel searches
- `.github/workflows/benchmark.yml` — regression detection
- `internal/simd/distance_functions.go` — blocked 384 dims
- `internal/simd/simd_blocked.go` — euclideanBlocked
- `internal/store/hnsw_gpu.go` — GPU candidate multiplier
- `internal/gpu/interface.go` — SearchBatch interface
- `internal/gpu/metal_gpu*.go` — batch search implementations

---

---

## complex64/complex128 ByID Fix (2026-03-27)

**Root Cause**: When `AutoShardingIndex` migrates from `ArrowHNSW` to `ShardedHNSW` (at 10k vectors), the `DataType` field was not being propagated. The `ShardedHNSWConfig` lacked a `DataType` field, so shards were created with default `float32` type, causing dimension mismatches.

**Fix Applied**:
1. Added `DataType` field to `ShardedHNSWConfig` struct
2. Added logic to propagate DataType in `ShardedHNSW.newShard()`
3. Added logic in `hnsw_autoshard.migrateToSharded()` to extract DataType from old index
4. Added query dimension validation in `SearchVectorsWithBitmap` to handle `[]float32` queries against complex64 indices

**Verification** (Metal 25k@128):
- Dense: 2804 QPS ✓
- Hybrid: 2352 QPS ✓
- Filtered: 2452 QPS ✓
- ByID: 2647 QPS ✓

**Files Changed**:
- `internal/store/sharded_hnsw.go` — DataType field + propagation
- `internal/store/hnsw_autoshard.go` — DataType extraction
- `internal/store/arrow_hnsw.go` — query dimension validation
- `benchmark_tool/main.go` — query vector generation fix

---

## Zero-Alloc & Zero-Copy Design Review

**Verification (2026-03-27)**: Code review confirms Longbow maintains zero-copy Arrow principles:

| Component | Implementation | Status |
|-----------|---------------|--------|
| DoPut | Direct Arrow RecordBatch injection | ✅ Zero-copy |
| DoGet | Zero-copy record reader | ✅ Zero-copy |
| HNSW Search | `arrow.Record` reuse, pooled vectors | ✅ Zero-alloc |
| Filter Bitsets | Cached, reused across searches | ✅ Zero-copy |
| SIMD Kernels | Stack-allocated buffers | ✅ Zero-alloc |
| Hybrid Search | Parallel dense/sparse with pool | ✅ Zero-alloc |

**Potential Improvements**:
- `SearchVectorsWithBitmap`: Consider pooling result structs
- `AutoShardingIndex`: Pre-allocate shard maps

---

**Last Updated**: 2026-03-27
