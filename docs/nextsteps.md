# Longbow Performance Optimization Status

## Current Benchmark Results (2026-03-23)

### M3 Pro (Bahamut) — 61-Config Matrix Complete ✅

**Test**: 8 dtypes × 2 dims × 4 counts = 64 configs (61 completed)
**Memory**: 20GB allocated
**Queries**: 1,000 per test

| DType | Dim=128 Best QPS | Dim=384 Best QPS | Notes |
|-------|-----------------|-----------------|-------|
| int16 | 11,951 (Filtered) | 8,366 (Filtered) | Best overall performer |
| int32 | 11,960 (Filtered) | 8,512 (Filtered) | Excellent |
| uint32 | 12,139 (Filtered) | 9,305 (Filtered) | Best Filtered QPS |
| int8 | 8,797 (Dense) | 7,000 (Dense) | Best DoGet (8.2M/s) |
| float32 | 13,036 (Filtered) | 7,679 (Hybrid) | Excellent at 25k |
| complex64 | 11,869 (Filtered) | 8,055 (Filtered) | Good |
| float64 | 5,093 (Filtered) | 3,823 (Filtered) | Moderate |
| complex128 | 8,287 (Dense) | 2,961 (Filtered) | Slow at high dims |

**Anomalies**:
- `complex64_384_5000`: Very low QPS (347 Dense, 34 Filtered) — indexing not complete
- `complex128_384_5000/10000/25000`: Timed out (>5 min per test)

---

## Previous Benchmark Results (2026-03-16)

### SIMD Microbenchmarks (Apple M3 Pro)

| Operation | ns/op | MB/s |
|-----------|-------|-------|
| Euclidean128 | 31.33 | 16,133 |
| Euclidean384 | 98.91 | 15,433 |
| Euclidean768 | 155.5 | 19,055 |
| Euclidean1536 | 368.4 | 16,548 |

### Integration Benchmarks (float32, dim=384, InitialCapacity=50k)

| Vectors | DoPut (MB/s) | DoGet (MB/s) | Search (QPS) |
|---------|--------------|--------------|--------------|
| 1,000 | 414 | 443 | 1,526 |
| 5,000 | 716 | 1,240 | 622 |
| 10,000 | 1,270 | 1,779 | 944 |
| 15,000 | 1,297 | 1,874 | 897 |
| 25,000 | 1,416 | 2,099 | 812 |

### Validation Tests (25k vectors, dim=128)

| Metric | Result | Target | Status |
|--------|--------|--------|--------|
| Ingest | 1,235 MB/s | 800 MB/s | ✅ PASS |
| DoGet | 2,223 MB/s | 1,700 MB/s | ✅ PASS |

---

## Float32 Fragmentation Fix ✅

### Problem

When vectors exceeded InitialCapacity (10k), Grow() created multiple small arena allocations causing:

- DoGet: 85% throughput drop
- Search: 92% QPS drop
- Incorrect search results (0 found)

### Solution

Increased default InitialCapacity from 10,000 to **50,000** in `internal/store/arrow_hnsw.go:102`

### Results After Fix

- 15k vectors: DoGet 271→1,874 MB/s (**6.9x**), Search 75→897 QPS (**12x**)
- 25k vectors: DoGet 271→2,099 MB/s, Search 812 QPS (correct results)

### 1.1 [HIGH PRIORITY] Float32 Regression at Scale 15,000 🔴

**Status**: ✅ FIXED - Root cause was timing overlap between DoGet and indexing
**Problem**: The validation matrix run on 2026-03-21 revealed that `float32` performance collapses at `Count=25,000` for Dim 384.

- **float32 384 25k**: DoGet ~61 MB/s, Dense QPS ~39 (vs expected 1500-2000 MB/s, 500-800 QPS)
- **float32 384 20k**: DoGet ~2,016 MB/s, Dense QPS ~795 (normal)

**Root Cause**: DoGet/DoSearch ran while HNSW indexing was still in progress.

1. The Python benchmark had only a 2-second sleep before DoGet — but float32 384 25k indexing takes ~8-15 seconds.
2. The Go benchmark shared one 15-minute `ctx` for all phases — `waitForIndexingComplete` would use remaining deadline, then DoGet ran while server was still indexing.
3. The `check_readiness` action returned `READY` or "complete" before indexing fully finished.

**Fixes Applied** ✅:

1. **`benchmark_tool/main.go`**: Each benchmark phase (DoPut, wait, DoGet, search) now uses independent contexts with dedicated timeouts. `waitForIndexingComplete` creates its own Background context for the polling loop, preventing parent cancellation from affecting the wait. Added 50ms IPC flush delay.
2. **`scripts/benchmark_comprehensive.py`**: Added `wait_for_readiness(clients, timeout=600)` before DoGet/DoSearch phases, with 10-minute timeout for large datasets.

### 1. Optimize HNSW Dimension Index Parameters (Float32 Collapse Fix)

**Files**: `internal/store/arrow_hnsw.go`, `internal/store/insertion_core.go`, `internal/store/arrow_hnsw_adaptive.go`

**Status**: ✅ Resolved
**Problem**: The integration benchmarks revealed a massive throughput dropoff for Float32 dense searches under specific configurations (e.g., Dimension 384, Scale 15,000+), falling below 100 QPS.
**Analysis**:

- Complex64 and Float32 both execute mathematically identical scalar/pointer arithmetic under unrolled Go loops.
- Both use zero-copy direct array fetch mechanisms (`GetVector`).
- This isolates the QPS dropoff to graph-traversal iterations sizing (number of node steps taken). The `Float32` pathways are taking significantly longer paths, caused by suboptimal M/MMax/MMax0 connectivity parameters for high-dimensional data.
**Root Causes Found**:

1. **Init-time optimization missing levelMultiplier recalculation** — The high-dim M adjustment at `arrow_hnsw.go:378-394` changed M/MMax/MMax0 but didn't recalculate `levelMultiplier`, causing incorrect level distributions.
2. **Dynamic index growth not covered** — The init-time optimization only fired based on `InitialCapacity`. If the index grew past 10k nodes with insufficient initial capacity, no adjustment occurred.
**Fix Applied**:
3. `arrow_hnsw.go:394` — Added `levelMultiplier` recalculation after M adjustment in init-time optimization.
4. `insertion_core.go:177-194` — Added dynamic M optimization trigger when nodeCount crosses 10k for high-dim Float32/Float64.
5. `arrow_hnsw_adaptive.go:123` — Added `levelMultiplier` recalculation in `adjustMParameter`.
**Expected Impact**: Float32 high-dim graphs will have proper connectivity and level distributions, matching Complex64/Float64 performance.

### 2. Review Grow() Trigger Alignment Thresholds

**Files**: `internal/store/insertion_core.go`, `internal/store/pq_training.go`

**Status**: ✅ Resolved
**Problem**: Sub-optimal alignment constraints inside graph slice chunks might force cascading reallocations or skew HNSW level multipliers.
**Analysis**:

- `ChunkSize = 1024` alignment in Grow() was correct — `(newCap + ChunkSize - 1) & ^(ChunkSize - 1)` properly rounds up.
- `levelMultiplier` recalculation is now consistent (fixed in item #1).
**Issues Found**:

1. `AdaptiveMThreshold` defaults (2000, 5000, 10000) were NOT ChunkSize-aligned (1024), causing triggers between chunk boundaries.
2. `count == threshold` condition could be skipped by batch inserts that jump over the threshold.
**Fix Applied**:
3. `insertion_core.go:165-170` — Changed default thresholds to ChunkSize-aligned values: 2048 / 5120 / 10240.
4. `insertion_core.go:177` — Changed `count == threshold` to `count >= threshold` to avoid batch-skip.
5. `pq_training.go:65,71` — Applied same fix (threshold → 1024, `==` → `>=`).

---

## MEDIUM PRIORITY Issues

### 3. SIMD Filter Operations - NOT ENABLED

**File**: `internal/query/filter_evaluator.go`, `internal/query/filter_evaluator_test.go`

**Status**: ✅ Verified — Already Implemented
**Analysis**: The "skipped" tests are fuzz test parameter guards (`t.Skip()` on invalid inputs), not disabled SIMD tests. SIMD filter operations are already implemented and enabled:

- `simd.MatchInt64` / `simd.MatchFloat32` — fast scalar comparisons
- `simd.AndBytes` — SIMD bitmap combination
- `VectorizedFilter` uses Arrow Compute for vectorized filtering
- `FastPathEqual` / `FastPathNotEqual` — fast paths bypassing Arrow Compute

---

### 9. Generic Quantizer - Limited Types

**Files**: `internal/store/scalar_quantization.go`, `internal/store/generic_quantizer.go`, `internal/store/generic_quantizer_test.go`

**Status**: ✅ Resolved
**Issue**: Float16 and Int8 types not supported in quantizer.
**Fix Applied**:

1. `scalar_quantization.go` — Added `TrainSQ8EncoderFloat16` and `TrainSQ8EncoderInt8` for training from non-float32 types. Added `EncodeFloat16` and `EncodeInt8` methods on `SQ8Encoder`.
2. `generic_quantizer.go` — Updated `GenericSQ8Quantizer.Encode` to handle `[]float16.Num` and `[]int8` with automatic conversion to `[]float32`.
3. `generic_quantizer_test.go` — Unskipped `TestQuantizer_TypeConversion_Float16ToFloat32` and `TestQuantizer_TypeConversion_Int8ToFloat32` with proper test data.

---

### 10. Arrow Utils - Type Casting

**File**: `internal/store/arrow_utils.go`

**Status**: ✅ Verified — Mostly Implemented
**Analysis**: `ExtractVectorFromArrow` already supports conversions for: float32, float16, float64, int8, uint8, int16, uint16, int32, uint32, int64, uint64. The remaining default case handles Complex types which require a different approach (not a simple cast).

---

### 4. Test Fixes Needed

**Status**: ✅ Mostly Resolved (1 remaining)

| Test File | Status | Action |
|-----------|--------|--------|
| `dataset_map_rcu_test.go` | ✅ Fixed | Implemented `TestVectorStore_RCU_Integration_Stub` — 100 concurrent readers, 10 concurrent writers updating datasets. Passes. |
| `vector_search_action_test.go` | ✅ Fixed | Added `mockVectorSearchDoActionServer` and `TestVectorSearchAction_Basic`. Passes. |
| `structured_errors_test.go` | ✅ Fixed | Implemented `TestStructuredErrors` — tests `ConfigError`, `ErrVectorDimensionMismatch`, `ErrNeighborSelectionLengthMismatch`. Passes. |
| `rate_limit_integration_test.go` | ⚠️ Skipped | `NewVectorStoreWithCompaction` and `RateLimitBytesPerSec` config do not exist in production code. Rate limiting is handled externally via GOGC tuner / `LONGBOW_MAX_MEMORY`. |
| `arrow_neighbors_test.go:38` | 🔴 Skipped | `ValidSelection` subtest skipped due to Arrow memory management issues. `LengthMismatchError` and `EmptySelection` subtests pass. |

---

## LOW PRIORITY / BY DESIGN

### Platform-Specific Stubs (No Action Needed)

| Feature | File | Platform |
|---------|------|----------|
| NUMA | `internal/store/numa_*_stub.go` | Linux only |
| io_uring | `internal/storage/wal_backend_stub.go` | Linux only |
| ONNX Metal | `internal/onnx/metal/stub.go` | macOS ARM64 only |
| GPU | `internal/gpu/memory.go` | Build tag gated |

### GPU Memory Operations (Requires `-tags=gpu`)

**File**: `internal/gpu/memory.go:170,175,185,190,195,200`

These return "not implemented yet" when built without GPU support. Build with `-tags=gpu` for actual implementations.

---

## Scripts Updated for 12GB Memory

Updated benchmark scripts to use 20GB memory limit for performance testing:

| Script | Previous | Updated |
|--------|----------|---------|
| `scripts/benchmark_runner.py` | 12GB | 20GB |
| `scripts/run_3node_performance.py` | 12GB | 20GB |
| `scripts/benchmark_suite.sh` | 12GB | 20GB |
| `scripts/start_bench_node.sh` | 12GB | 20GB |
| `scripts/run_iouring_comparison.sh` | 12GB | 20GB |
| `scripts/start_one_node.sh` | 12GB | 20GB |
| `scripts/benchmark_incremental.sh` | 12GB | 20GB |
| `scripts/benchmark_tool_incremental.sh` | 12GB | 20GB |

---

Last Updated: 2026-03-23 (61-config benchmark complete, M3 Pro performance documented)

---

## RaspberryPiZero Platform Plan

### Constraints

- **Memory**: Extremely limited (512MB RAM).
- **CPU**: ARMv6 (Pi Zero) or ARMv8 (Pi Zero 2). No AVX, maybe limited Neon.
- **Storage**: SD Card (slow I/O).

---

## Linux (ancalagon) Platform Issues — 2026-03-22

### 11. Linux Build Failure: Missing AVX512 Kernels 🔴

**Files**: `internal/simd/simd_amd64.go`, `internal/simd/simd_amd64.s`

**Status**: ✅ FIXED

**Problem**: `go build ./cmd/longbow` failed on Linux (gccgo 1.24.4) with:

```
github.com/23skdu/longbow/internal/simd.euclidean768AVX512: relocation target not defined
github.com/23skdu/longbow/internal/simd.euclidean1536AVX512: relocation target not defined
github.com/23skdu/longbow/internal/simd.dot768AVX512: relocation target not defined
github.com/23skdu/longbow/internal/simd.dot1536AVX512: relocation target not defined
```

**Root Causes**:

1. **Missing assembly kernels**: `euclidean768AVX512Kernel`, `euclidean1536AVX512Kernel`, `dot768AVX512Kernel`, `dot1536AVX512Kernel` were never implemented in `simd_amd64.s`. Only `euclidean384AVX512Kernel` and `dot384AVX512Kernel` existed. The `euclidean768` and `euclidean1536` Go functions existed but had no matching assembly implementations.
2. **Missing AVX512 guards**: The Go wrapper functions (`euclidean384AVX512`, `euclidean768AVX512`, `euclidean1536AVX512`, `dot768AVX512`, `dot1536AVX512`) called their kernels unconditionally without checking `features.HasAVX512`. On systems without AVX512 (e.g., i7-12650H which lacks AVX512), these would attempt to call undefined symbols.

**Fix Applied**:

1. `simd_amd64.s` — Added 4 new AVX512 assembly kernels (768 and 1536 dims for Euclidean and Dot product), following the same 4x-unrolled pattern as the existing 384-dim kernels.
2. `simd_amd64.go` — Added `if !features.HasAVX512` guards to 5 functions, with appropriate fallbacks:
   - `euclidean384AVX512` → falls back to `euclidean384AVX2`
   - `euclidean768AVX512` → falls back to `euclidean768AVX2`
   - `euclidean1536AVX512` → falls back to `euclidean1536AVX2`
   - `dot768AVX512` → falls back to `dotGeneric`
   - `dot1536AVX512` → falls back to `dotGeneric`

**Verified**: Build succeeds on both macOS (native) and Linux (gccgo/amd64). Benchmark tool and longbow binary run correctly.

### 12. CPU Detection: i7-12650H Lacks AVX512 ✅ FIXED

**Files**: `internal/simd/simd_amd64.go`

**Status**: ✅ FIXED

**Problem**: On AVX2-only systems, `euclidean768AVX2` and `euclidean1536AVX2` fell back to the generic `euclideanAVX2` function (8 floats/iter), which is slower than the scalar unrolled4x Go implementation for high dimensions.

**Fix Applied**: Changed both functions to delegate to the scalar unrolled4x implementations (`euclidean768Unrolled4x`, `euclidean1536Unrolled4x`) instead of the generic AVX2 loop. On non-AVX512 systems, the scalar Go implementation is faster for 768/1536 dims due to better loop efficiency.

### 13. int8 AVX2 Kernel Performance ✅ FIXED

**Files**: `internal/simd/simd_amd64.go`, `internal/simd/simd_amd64.s`

**Status**: ✅ FIXED

**Problem**: The original `euclideanInt8AVX2Kernel` processed only 16 bytes (16 int8s) per iteration, requiring 768/16 = 48 iterations for a 768-dim vector. The float32 AVX2 kernel processes 32 bytes per iteration (8 floats), so int8 was ~2x slower per byte.

**Fix Applied**:

1. **New assembly kernel** `euclideanInt8Unrolled4xAVX2Kernel`: Processes 64 bytes (64 int8s) per iteration — 4x wider than the single-chunk kernel. Uses 4 YMM accumulators (one per 16-byte chunk), stays in int16→int32 domain for accumulation, converts to float32 only once at the end.
2. **Algorithm**: VPMOVSXBW (sign-extend) → VPSUBW (diff) → VPMADDWD (square + pair-reduce to int32) → VPADDD (accumulate) → single horizontal reduction → VCVTDQ2PS → VSQRTSS.
3. **Go wrapper updated**: `euclideanInt8AVX2` now calls the new 4x-unrolled kernel instead of the single-chunk version.

---

## RaspberryPiZero Platform Plan

1. **Low-Memory Mode Configuration**:
    - Introduce a \`low_mem\` profile in configuration or via environment variable.
    - Reduce default \`InitialCapacity\` (e.g., 5,000 instead of 50,000).
    - Downsize or disable memory-heavy pools/caches.
2. **CPU Optimization**:
    - Ensure clean fallback to scalar Go code for architectures without SIMD.
    - Disable high-performance SIMD instructions that require specific instruction sets (AVX/Neon if not available on 32-bit ARM).
3. **Build Configuration**:
    - Exclude GPU, Metal, and io_uring backends by default for \`arm\` builds.
    - Verify build with \`GOOS=linux GOARCH=arm GOARM=6\` (for original Pi Zero) or \`arm64\` (for Pi Zero 2).

---

## Feature Parity with Leading Vector Databases

*Last Updated: 2026-03-22 — Updated based on codebase analysis vs Milvus, Qdrant, Weaviate, Pinecone*

### ✅ Implemented

| Feature | Status | Notes |
|---------|--------|-------|
| HNSW index | ✅ Stable | ArrowHNSW with adaptive M, configurable `efConstruction` |
| DiskANN / IVF index | ✅ Stable | DiskANN SSD-based offloading |
| SQ8 scalar quantization | ✅ Stable | `GenericSQ8Quantizer`, `SQ8Encoder`, type conversion |
| Hybrid search (Dense + Sparse) | ✅ Stable | Filtered search, BM25 via hybrid search |
| Cross-encoder reranking | ✅ Stable | ONNX runtime with Metal/Apple Silicon |
| Multi-vector types | ✅ Stable | float32/64, float16, int8-64, uint8-64, complex64/128 |
| Arrow Flight protocol | ✅ Stable | Zero-copy via Apache Arrow IPC |
| WAL + Parquet snapshots | ✅ Stable | Batched WAL, snapshot interval configurable |
| Prometheus metrics | ✅ Stable | 100+ custom metrics |
| Distributed gossip protocol | ✅ Stable | Consistent hashing, SWIM, DoExchange mesh |
| Go + Python clients | ✅ Stable | Smart client with routing |
| JS/TS client | 🟡 Partial | `longbowclientsdk/src/longbow/` — exists but not published |
| Namespace isolation | ✅ Stable | `CreateNamespace`, `DeleteNamespace`, `ListNamespaces` |
| Metadata filtering | ✅ Stable | Predicate pushdown, `ColumnInvertedIndex` for exact match |
| Distance metrics | ✅ Stable | Euclidean, Cosine, Dot Product, Hamming (SIMD) |
| Binary quantization | 🟡 Partial | `arrow_hnsw_bq_test.go` exists but not user-facing |
| Consistency quorum infra | 🟡 Partial | `internal/store/quorum.go` exists; not user-facing |
| Distributed NUMA | ✅ Linux | NUMA-aware memory allocators |
| io_uring WAL | ✅ Linux | High-throughput storage engine |
| Kubernetes Helm | ✅ | Helm chart available |

### 🔴 HIGH PRIORITY — Production Gaps

#### 14. efSearch Per-Query Configuration

**Milvus/Qdrant/Pinecone**: Expose `ef`/`efSearch` as a per-query parameter for tuning recall vs. speed.
**Longbow**: `efSearch` is auto-computed internally (`arrow_hnsw.go:1112-1168`, floor=100). Users cannot override per-query.
**Action**: Add `efSearch` parameter to `VectorSearchRequest`. Allow runtime override for production tuning. Range: 16–4096.

#### 15. Standalone IVF-PQ Index

**Milvus/Qdrant**: Offer IVF-PQ as a primary index type (coarse quantizer + product codes).
**Longbow**: PQ exists as a compression layer within HNSW, not as a standalone index type.
**Action**: Implement IVF-PQ index: (1) k-means coarse quantizer, (2) PQ encoder per cluster, (3) inverted index mapping cluster IDs to PQ codes. Target: 4-16x memory reduction.

#### 16. Upsert (Update-in-Place)

**Milvus/Qdrant/Pinecone**: Update a vector by ID — insert or replace atomically.
**Longbow**: Only insert (append) and delete (tombstone). No update.
**Action**: Implement upsert that marks old vector tombstoned and inserts new version atomically.

#### 17. REST API / OpenAPI Spec

**Milvus/Pinecone/Weaviate**: Full REST API with OpenAPI spec.
**Longbow**: Arrow Flight only (gRPC), no HTTP/REST.
**Action**: HTTP/REST wrapper for core endpoints (upsert, search, get). Auto-generate OpenAPI from gRPC descriptor.

### 🟡 MEDIUM PRIORITY — Ecosystem Gaps

#### 18. Rich Filter Expression Language

**Milvus**: `must/must_not/should` boolean filters with nested conditions.
**Qdrant**: JSON payload conditions with nested booleans.
**Longbow**: Post-filtering with simple predicates; `ColumnInvertedIndex` for exact match only.
**Action**: Add compound filter expressions (AND/OR/NOT) with nested field paths. Target: Milvus v3 `must`/`should` parity.

#### 19. User-Facing Consistency Levels

**Milvus**: Strong / Bounded Staleness / Eventually Consistent per request.
**Longbow**: Quorum infrastructure exists (`internal/store/quorum.go`, supports One/Quorum/All) but not exposed in client SDK.
**Action**: Add `ConsistencyLevel` enum to `VectorSearchRequest` and `DoPutRequest`. Wire through quorum manager.

#### 20. Per-Collection HNSW Tuning

**Milvus**: Index params (`M`, `efConstruction`) per collection.
**Longbow**: `LONGBOW_HNSW_M`, `LONGBOW_HNSW_EF_CONSTRUCTION` are global env vars only.
**Action**: Allow index parameters at dataset creation time. Fall back to global env vars.

#### 21. Batch Import CLI (Parquet/NumPy)

**Milvus**: Bulk import from Parquet, NumPy, CSV files.
**Longbow**: Programmatic only via SDK.
**Action**: CLI tool to import `*.parquet` / `*.npy` files directly into a dataset.

#### 22. Published JS/TS SDK

**Longbow**: `longbowclientsdk/src/longbow/` contains a TypeScript client but not published to npm.
**Action**: Publish `@longbow/client` to npm. Add async/await support for Node.js.

### 🟢 LOW PRIORITY — Nice to Have

#### 23. Change Data Capture (CDC)

**Milvus**: Kafka/RabbitMQ integration.
**Longbow**: Not implemented.
**Action**: Consider event stream integration for production replication.

#### 24. Schema Evolution / ALTER

**Milvus**: ALTER collection schema.
**Longbow**: Schema fixed at creation.
**Action**: Low priority — consider read-only schema enforcement.

---

### Feature Comparison Matrix

| Feature | Milvus | Qdrant | Weaviate | Pinecone | Longbow |
|---------|--------|--------|----------|----------|---------|
| HNSW efSearch tuning | ✅ | ✅ | ✅ | ✅ | ❌ (auto-only) |
| IVF-PQ index | ✅ | ✅ | ✅ | ✅ | ❌ (compression only) |
| Upsert | ✅ | ✅ | ✅ | ✅ | ❌ |
| Rich filter expr | ✅ | ✅ | ✅ | ✅ | ❌ (predicates only) |
| User-facing consistency | ✅ | ✅ | ✅ | ✅ | ❌ (infra exists) |
| REST API | ✅ | ✅ | ✅ | ✅ | ❌ |
| Published TS/JS SDK | ✅ | ✅ | ✅ | ✅ | ❌ (unpublished) |
| DiskANN on-disk | ✅ | ✅ | ✅ | ✅ | ✅ |
| Namespaces | ✅ | ✅ | ✅ | ✅ | ✅ |
| BM25 sparse | ✅ | ✅ | ✅ | ✅ | ✅ (via hybrid) |
| Multi-vector types | ✅ | ✅ | ✅ | ✅ | ✅ |
| Binary quantization | ✅ | ✅ | ✅ | ✅ | 🟡 (test-only) |
| Cross-encoder | ✅ | ❌ | ✅ | ✅ | ✅ |
| Distributed | ✅ | ✅ | ✅ | ✅ | ✅ |

---

### Recommended Priority Order

1. **efSearch per-query** — High impact, low effort. One parameter change.
2. **Upsert** — Table stakes for production RAG / knowledge base workloads.
3. **REST API** — Lowest effort, highest ecosystem impact. gRPC→HTTP wrapper.
4. **Published TS/JS SDK** — Expands reach to web/Node.js ecosystem.
5. **Consistency levels (user-facing)** — Required for multi-region deployments.
6. **IVF-PQ standalone** — Critical for petabyte-scale / disk-based workloads.
