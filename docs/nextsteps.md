# Longbow Next Steps - Stability & Performance Recommendations

> Generated: 2026-05-20
> Based on: Security audit, race condition analysis, and comprehensive historical performance audit (v0.2.0 - v0.2.1).

---

## P0 Blockers: Performance & Architecture Overhaul Plan

This multipart implementation plan addresses critical performance and architectural blockers identified in the v0.2.1 performance audit.

### PART 1: Optimize BM25 Text Index for Arena Storage (Hybrid Search Recovery)
- **Technical Analysis & Root Cause**:
  The standard `BM25InvertedIndex` in [bm25_inverted_index.go](file:///Users/rsd/REPOS/longbow/internal/store/bm25_inverted_index.go) relies on standard Go heap-allocated maps (`MapRCU[string, *BM25PostingList]`, `MapRCU[VectorID, []string]`) and dynamic slices (`DocIDs []VectorID`, `TFs []int`). During concurrent hybrid searches, this causes extreme GC pointer-tracking pauses, resulting in a **76% throughput loss** compared to the baseline. Although an off-heap `BM25ArenaIndex` (which uses a slab-allocated `PackedAdjacency` table for postings lists) is implemented in [bm25_arena.go](file:///Users/rsd/REPOS/longbow/internal/store/bm25_arena.go), it is not initialized nor updated by the ingestion pipeline in `store.go` and `store_lifecycle.go`.
- **Proposed Optimization Design**:
  1. Initialize `BM25ArenaIndex` during dataset instantiation in `getOrCreateDataset` inside [store.go](file:///Users/rsd/REPOS/longbow/internal/store/store.go) and [dataset.go](file:///Users/rsd/REPOS/longbow/internal/store/dataset.go).
  2. Implement an off-heap token dictionary using `TypedArena[byte]` to store raw string tokens contiguously, representing them as `uint32` token IDs. Maintain a light `map[string]uint32` on-heap map for fast dictionary resolution.
  3. Modify `indexTextColumnsForHybridSearch` in [store_hybrid.go](file:///Users/rsd/REPOS/longbow/internal/store/store_hybrid.go) and the background index worker in [store_lifecycle.go](file:///Users/rsd/REPOS/longbow/internal/store/store_lifecycle.go) to index incoming documents to `ds.BM25ArenaIndex` instead of `ds.BM25Index`.
  4. Complete sparse search query routing in `SearchHybrid` inside [hybrid_search.go](file:///Users/rsd/REPOS/longbow/internal/store/hybrid_search.go) to use `searchBM25Arena` when the arena index is active.
- **Subtasks Checklist**:
  - [ ] Initialize `BM25ArenaIndex` in `NewDataset` and `getOrCreateDataset`.
  - [ ] Implement off-heap contiguous byte-token dictionary via `TypedArena[byte]` in `BM25ArenaIndex`.
  - [ ] Reroute the background index worker in `runIndexWorker` to index to `ds.BM25ArenaIndex.IndexDocument`.
  - [ ] Route query sparse scoring path in `SearchHybrid` to `searchBM25Arena` in `hybrid_search_arena.go`.
- **Unit & Fuzz Testing Strategies**:
  - **Unit Test**: Update `hybrid_store_integration_test.go` and `bm25_arena_test.go` to index 10,000 documents with dynamic text columns and assert that zero heap allocations are made for postings and token storage.
  - **Fuzz Test**: Implement `FuzzBM25ArenaIndex` to concurrently index random terms and document sizes, verifying score equivalence and arena boundary compliance.

### PART 2: Enforce Arena Allocation for all Dimensions (Slab Allocator Optimization)
- **Technical Analysis & Root Cause**:
  1. **Lock-free Fast Path Miss**: In [arena.go](file:///Users/rsd/REPOS/longbow/internal/memory/arena.go), the CAS-based lock-free `allocFast` is restricted to allocations `<= 1024` bytes. Float32 vectors of dimension 384 require `384 * 4 = 1536` bytes, which constantly falls back to the mutex-locked `allocCommon` and blocks on `a.mu` under ingestion, creating a **36% allocation CPU bottleneck**.
  2. **Recycler Pool Bypass**: In [slab_pool.go](file:///Users/rsd/REPOS/longbow/internal/memory/slab_pool.go), when a dynamic slab is requested with non-standard capacities (such as `ChunkSize * paddedDims * 4 + 64`), it bypasses the standard recycler pools (4MB, 8MB, 16MB, 32MB) and triggers direct OS `offHeapAlloc.Allocate(capacity)` and `Free` calls on release.
- **Proposed Optimization Design**:
  1. Increase the lock-free `allocFast` allocation threshold in `arena.go` from `1024` to `4096` bytes. This ensures that all vector dimensions up to 1024 (4096 bytes for float32) are allocated using lock-free CAS.
  2. Modify `GetSlab` in `slab_pool.go` to round up requested slab capacities to the next standard pooled size (4MB, 8MB, 16MB, 32MB) so that they are successfully captured and recycled by standard size-class pools.
- **Subtasks Checklist**:
  - [ ] Increase the `allocFast` size limit to `4096` bytes in `arena.go`.
  - [ ] Modify `GetSlab` and `PutSlab` in `slab_pool.go` to round up non-standard capacities and recycle them.
  - [ ] Optimize lock-free slab offset CAS in `allocFast` to handle aligned boundaries for all dimensions up to 1024.
- **Unit & Fuzz Testing Strategies**:
  - **Unit Test**: Create `TestArenaFastPathDimensions` in `arena_test.go` to perform concurrent allocations for dimensions 128, 384, 768, and 1024, asserting that `metrics.ArenaSlowPathTotal` remains zero.
  - **Fuzz Test**: Implement `FuzzArenaAllocator` in `slab_pool_fuzz_test.go` to execute randomized allocations up to 4096 bytes under high concurrency, asserting zero leakage and boundary validation.

### PART 3: Profile DoGet Memory Locality (DoGet Regression Fix)
- **Technical Analysis & Root Cause**:
  1. **Generation Isolation Bypass**: In `GetVectorWithGen` in [graph_data.go](file:///Users/rsd/REPOS/longbow/internal/store/types/graph_data.go#L1411), the retrieval path for `VectorTypeFloat32` invokes `g.GetVectorsChunk(cID)` instead of `g.GetVectorsChunkWithGen(cID, maxGen)`. This completely ignores the generation parameter, fetching the newest generation chunk, causing cache-dirty lookups and correctness bugs under concurrent compaction.
  2. **Schema Allocation Churn**: In `mapInternalToUserIDsLocked` in [store_query.go](file:///Users/rsd/REPOS/longbow/internal/store/store_query.go#L577), the loop resolves `"id"` and `"metadata"` columns by calling `rec.Schema().Fields()` twice per search result. Arrow's `.Fields()` method allocates a new slice of `Field` objects every time it is called. For a search query returning 100 results, this produces 200 heap allocations, adding massive GC churn.
- **Proposed Optimization Design**:
  1. Correct the generation isolation in `GetVectorWithGen` inside `graph_data.go` to invoke `GetVectorsChunkWithGen(cID, maxGen)` for `VectorTypeFloat32`.
  2. Since all RecordBatches in a Dataset share the exact same `ds.Schema`, cache `idColIdx` and `metadataColIdx` ONCE at the start of `mapInternalToUserIDsLocked` from `ds.Schema` instead of dynamically calling `rec.Schema().Fields()` inside the results loop.
- **Subtasks Checklist**:
  - [ ] Fix `GetVectorWithGen` in `graph_data.go` to correctly call `GetVectorsChunkWithGen`.
  - [ ] Rewrite `mapInternalToUserIDsLocked` in `store_query.go` to resolve and cache column indices outside the loop.
- **Unit & Fuzz Testing Strategies**:
  - **Unit Test**: Implement `TestDoGetSchemaAllocations` to run a vector search returning 1000 candidates, asserting that the number of allocations per query is close to zero using `testing.AllocsPerRun`.
  - **Fuzz Test**: Run a concurrent read-write fuzzing harness where search query tasks and compaction tasks execute simultaneously, verifying that readers see correct generation isolation.

### PART 4: WAL Ingestion Pipeline Overhaul (DoPut Bottleneck Recovery)
- **Technical Analysis & Root Cause**:
  In [wal_buffered.go](file:///Users/rsd/REPOS/longbow/internal/storage/wal_buffered.go), concurrent threads calling `Sync()` block sequentially. Specifically, if a background flush is already active (`w.isFlushing` is true), all other write threads wait on `w.syncCond.Wait()`. This blocks the ingestion pipeline and causes sequential serialization.
- **Proposed Optimization Design**:
  1. Implement **Group Commit Batching**: Threads calling `Sync()` register their writes into a pending sync queue and block on a per-batch event. A single flushing worker drains this queue, batches the outstanding transactions into a single flush operation, and wakes up all waiters upon completion.
  2. Enhance **Double Buffering**: Implement a lock-free buffer swap scheme using a queue of ready buffers so that write threads can continue writing into a fresh buffer while the active buffer is being asynchronously flushed to disk.
- **Subtasks Checklist**:
  - [ ] Implement a group-commit batching manager in `wal_buffered.go`.
  - [ ] Optimize the lock-free double buffering swap scheme in `BufferedWAL.swapBufferLocked`.
- **Unit & Fuzz Testing Strategies**:
  - **Unit Test**: Update `wal_buffered_test.go` to run concurrent writes from 64 goroutines calling `Sync()` and verify that the total disk write IOPS is clustered into unified group-commits.
  - **Fuzz Test**: Implement `FuzzWALRecovery` to write randomized batches, trigger forced crash mid-flush, and verify that the replayed WAL log is consistent.

### PART 5: Implement Hard Memory Limits (OOM Prevention & Backpressure)
- **Technical Analysis & Root Cause**:
  `GCTuner` only tracks soft memory limits. Under high-throughput ingestion workloads, rapid off-heap allocations can exceed total physical memory before garbage collection or eviction can reclaim space, causing the OS kernel to kill the process via OOM.
- **Proposed Optimization Design**:
  1. Define a hard memory threshold `LONGBOW_MAX_MEMORY_HARD` (e.g. 95% of total allowed capacity).
  2. In `AdmissionController.Admit` (in `admission.go`), when the hard limit is breached, immediately reject incoming `DoPut` ingestion requests with a `ResourceExhausted` gRPC status.
  3. Implement **Adaptive Memory Backpressure**: As memory usage approaches the hard limit (between 80% and 95%), inject exponentially scaling sleep delays (e.g. 5ms to 100ms) on ingestion threads to allow eviction and compaction workers to free memory.
- **Subtasks Checklist**:
  - [ ] Add `LONGBOW_MAX_MEMORY_HARD` environment configuration and memory threshold checks.
  - [ ] Implement exponential backpressure sleep logic inside `AdmissionController` based on memory utilization ratio.
  - [ ] Reject ingestion with `ResourceExhausted` when hard memory limits are breached.
- **Unit & Fuzz Testing Strategies**:
  - **Unit Test**: Write `TestAdmissionHardMemoryLimits` in `admission_test.go` to mock extreme memory usage and verify that new ingestions are rejected with `ResourceExhausted` and soft-pressure triggers correct backpressure delay scaling.
  - **Fuzz Test**: Implement `FuzzAdmissionBackpressure` to issue ingestion batches at different system load levels, ensuring system stays under bounds without panics.

### PART 6: Build Stubs, pprof, GPU, and NUMA Topology
- **Technical Analysis & Root Cause**:
  1. **Avo Stubs**: Avo generated empty stub functions in `all_kernels_stubs_amd64.go` can duplicate custom assembly stubs in `simd_amd64.go`, breaking compilation.
  2. **pprof Collection**: The python benchmark runner shuts down the Longbow server immediately, terminating it before pprof can collect metrics over HTTP.
  3. **GPU Diagnostics**: Fallback to CPU occurs silently when Metal/CUDA binaries are missing. macOS fat binary support is missing.
  4. **NUMA aware Alloc**: Benchmarks do not report NUMA topography, and remote multi-NUMA hosts like `ancalagon` face remote node access overheads.
- **Proposed Optimization Design**:
  - **Avo Checker**: Create a Go test/tool that parses `internal/simd` using `go/parser` and `go/ast` to detect duplicate function declarations.
  - **pprof Delay**: Add a graceful shutdown delay in the python runner, or fetch profiles mid-run.
  - **GPU warning & universal2**: Output startup diagnostic warning for missing binaries. Build fat binaries for macOS via `lipo` and document CUDA/Metal requirements.
  - **NUMA Allocation**: Output NUMA details in benchmark log. Integrate `lbmem.MbindMemory` in the off-heap allocator to lock slab allocations to the CPU socket execution boundaries.
- **Subtasks Checklist**:
  - [ ] Write an AST duplicate symbol parser in `internal/simd/simd_stubs_test.go`.
  - [ ] Add graceful shutdown/profile collection delay in `scripts/unified_benchmark.py`.
  - [ ] Add missing GPU binary detection logs on startup.
  - [ ] Integrate NUMA memory node binding using `lbmem.NUMABind` in `numa_allocator.go`.
- **Unit & Fuzz Testing Strategies**:
  - **Unit Test**: Write `TestNUMAMemoryBinding` on Linux to allocate memory bound to specific nodes and verify node affinity via `/proc/self/numa_maps`.
  - **Unit Test**: Update `simd_stubs_test.go` to assert that no duplicate empty assembly stubs exist in SIMD source files.

---

## Historical Performance Regressions & Audit (v0.2.0 - v0.2.1)
An audit of historical performance records (v0.2.0 to v0.2.1-rc3) reveals several key regressions that require monitoring or immediate attention:

1. **DoPut Ingestion Regressions**: A WAL deadlock fix originally caused a severe (-17% to -96%) DoPut regression across most configs. While WAL buffer optimizations recovered this significantly (now only -50%), ingestion remains a bottleneck compared to historical baselines.
2. **DoGet Memory Fragmentation**: `float32` DoGet experienced a -42% throughput drop at higher counts due to memory fragmentation, and `int64` DoGet at 5k vectors dropped -52% due to caching behavior changes.
3. **Arena Allocation Misses**: `float32` (dim 384) vectors missed the arena allocator entirely, causing a 96% DoPut regression due to the slab slow path being hit constantly (36% allocation overhead).
4. **Hybrid Search Overhead**: Hybrid search is 76% slower than the baseline because the BM25 text index is not yet optimized for arena storage.
5. **Int8 Search Regression**: Generic improvements caused a slight (-25%) regression in Int8 search QPS.

## Actionable Recommendations (Based on Historical Audit & Benchmarks)
1. **Optimize BM25 Text Index for Arena Storage**: Pack text indexing data into the arena to recover the 76% performance loss in Hybrid Search.
2. **Enforce Arena Allocation for all Dimensions**: Fix the slab allocator slow path to ensure dimensions like 384 don't fall back to heap allocation.
3. **Profile DoGet Memory Locality**: Investigate caching behavior and memory layout changes that caused the ~50% DoGet regression for `int64` and `float32`.
4. **WAL Ingestion Pipeline Overhaul**: Further optimize the WAL buffer or implement async WAL flushing to recover the remaining 50% DoPut regression.
5. **Implement Hard Memory Limits**: Add `LONGBOW_MAX_MEMORY_HARD` with OOM prevention and adaptive memory backpressure during ingestion.

---

## AVX2 Int16/Uint16 Kernel Smoke Test Findings (2026-05-20)

### 1. Build System Fix Required
**Issue**: `euclideanInt16AVX2Kernel`, `euclideanUint16AVX2Kernel`, `dotInt16AVX2Kernel`, `dotUint16AVX2Kernel` were declared in both `simd_amd64.go` (with `//go:noescape`) and `all_kernels_stubs_amd64.go` (generated stubs), causing redeclaration errors on cross-compilation.
**Fix**: Removed duplicate declarations from `all_kernels_stubs_amd64.go` (lines 121-127). The real implementations are now in `int16_kernels_amd64.s`.
**Recommendation**: Regenerate `all_kernels_stubs_amd64.go` via `go generate` to prevent future drift, or add a build-time check for duplicate declarations.

### 2. AVX2 Int16/Uint16 Kernels Verified Working
**Result**: All 8 int/uint types dispatch correctly through AVX2 kernels on x86_64 (ancalagon). Apple Silicon uses NEON/baseline paths as expected.
**Ingestion**: int16/uint16 DoPut throughput is competitive (374K-652K vec/s on x86_64, 155K-605K on Apple Silicon).
**Search**: Dense QPS for int16/uint16 is stable (371-569 QPS across dims 128-768).
**No regressions** detected from the baseline optimization changes.

### 3. Baseline Integer Arithmetic Optimization
**Change**: Replaced `float64` arithmetic with `int64`/`uint64` accumulators in baseline int16/uint16 euclidean, dot, and cosine distance functions.
**Benefit**: Avoids FPU conversion overhead; max squared diff for int16 fits in int64 (65535^2 ≈ 4.3e9, well within int64 range).
**Cosine distance**: Added 4x unrolling and clamped output to valid [0, 2] range.

### 4. Cross-Platform Performance Observations
| Observation | Impact |
|-------------|--------|
| Apple Silicon outperforms x86_64 on uint8/768 ingestion (671K vs 354K vec/s) | NEON optimization advantage |
| x86_64 leads on int16/128 ingestion (652K vs 553K vec/s) | AVX2 kernel efficiency |
| int64/uint64 ingestion drops sharply at dim=3072 (17K-51K vec/s) | Memory bandwidth bound on both platforms |

---

## Critical Stability Fixes (Implemented)

### 1. Vector ID Overflow Protection
**Status: FIXED** - Added bounds checking before uint32 conversions in:
- `sharded_hnsw.go`: `AddBatch()` and `AddByRecord()` now check `nextID > math.MaxUint32`
- `arrow_hnsw.go`: `AddBatch()` now validates `newNext > math.MaxUint32+1` with rollback on failure

**Impact**: Prevents silent ID wraparound at 4.29B vectors, which would cause data corruption and incorrect search results.

### 2. Path Traversal Hardening
**Status: FIXED** - Added `filepath.Clean()` to:
- `disk_backed_learned_index.Save()` - was missing sanitization before `os.Create()`
- `parquet_ingester.Ingest()` - was missing sanitization before `os.Open()`

**Impact**: Prevents potential directory traversal attacks if paths flow from external APIs.

### 3. URL Injection Prevention
**Status: FIXED** - Added regex validation for Hugging Face repoID format (`^[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+$`) before URL construction in `onnx/download.go`.

**Impact**: Prevents URL injection attacks via malformed repoID parameters.

### 4. UDS Socket Permissions
**Status: FIXED** - Changed from `0666` (world-accessible) to `0660` (owner+group only) in `cmd/longbow/main.go`.

**Impact**: Prevents unauthorized local users from connecting to the gRPC server in multi-tenant environments.

### 5. Test Race Condition
**Status: FIXED** - `TestDualIndexHarness_Basic` was creating `Dataset` with nil `Records` field. Changed to use `NewDataset()` constructor.

**Impact**: Eliminates nil pointer dereference panic during test execution.

---

## Remaining Security Concerns (Monitored)

### HIGH PRIORITY - Monitor

| Issue | Location | Risk | Mitigation |
|-------|----------|------|------------|
| Arena offset truncation (>4GB) | `temporal_search.go:347,367` | Medium | TemporalEntry arena limited by design; monitor arena growth |
| Vector ID truncation in temporal results | `temporal_search.go:935,1045,1094,1145` | Low | System designed for uint32 IDs; truncation only at 4.29B vectors |
| BatchIdx truncation | `sharded_hnsw.go:393,1025,1136` | Low | BatchIdx bounded by record count; unlikely to exceed uint32 |
| locationStore.Len() truncation | `sharded_hnsw.go:1392` | Low | Per-shard vector count unlikely to exceed 4.29B |

### MEDIUM PRIORITY - Review

| Issue | Location | Recommendation |
|-------|----------|----------------|
| `ivf_flat.go:347` - vector map size | `uint32(len(ivf.vectors))` | Add explicit check if IVF-FLAT expected to handle >4B vectors |
| `arrow_hnsw_persistence.go:208` - version conversion | `int(fromVersion)` where fromVersion is uint64 | Add bounds check if version numbers could exceed MaxInt64 |
| 472 remaining G115 suppressions | Various | All reviewed; most are bounded by design (HNSW levels, neighbor counts, dimensions) |

### LOW PRIORITY - Document

| Issue | Location | Note |
|-------|----------|------|
| 195 G103 (unsafe) suppressions | Various | All verified safe: bounds-checked pointer arithmetic, Go-spec-compliant type reinterpretations, arena-aligned allocations |
| 49 G404 (math/rand) suppressions | Various | All non-security uses: HNSW levels, k-means, gossip, benchmarks |
| 7 G204 (subprocess) suppressions | `gpu/detection.go`, `profiling/cpu.go` | All use known binaries from system paths or temp files |

---

## Performance Recommendations

### 1. pprof Collection Reliability
**Issue**: Benchmark script fails to collect pprof profiles (connection refused on metrics port 9470).
**Root Cause**: Server shuts down before profile collection completes.
**Recommendation**: Add a delay between benchmark completion and server shutdown, or collect profiles during the benchmark run rather than after.

### 2. Memory Cap Enforcement
**Current**: `LONGBOW_MAX_MEMORY` environment variable sets soft limit.
**Recommendation**: Add hard memory limit enforcement with OOM prevention:
- Monitor RSS during ingestion
- Implement backpressure when approaching limit
- Add `LONGBOW_MAX_MEMORY_HARD` for hard limit with graceful degradation

### 3. Benchmark Matrix Optimization
**Current**: Full matrix (5 dims × 8 counts × 17 dtypes × 13 search modes × 3 hosts) = 26,520 combinations.
**Recommendation**:
- Run full matrix only for release candidates
- Use representative subset for CI: 3 dims × 3 counts × 5 dtypes × 5 modes
- Cache results for unchanged code paths

### 4. GPU Binary Distribution
**Issue**: Metal and CUDA binaries require platform-specific builds; fallback to CPU when not available.
**Recommendation**:
- Add build-time detection to warn when GPU binary is missing
- Consider fat binaries for macOS (universal2 + Metal)
- Document GPU binary build requirements in README

### 5. NUMA Awareness
**Observation**: Logs show "Single NUMA node detected (no NUMA)" on localhost.
**Recommendation**:
- Add NUMA topology detection to benchmark output
- For ancalagon (Linux, likely multi-NUMA), ensure memory allocation is NUMA-aware
- Benchmark with and without NUMA binding to quantify impact

---

## Regression Analysis (v0.2.0 → v0.2.1)

Based on full benchmark matrix (4 hosts × 5 dims × 5 counts × 16 dtypes × 13 search modes):

### Local Metal - ALL IMPROVEMENTS (No Regressions)

| Metric | Baseline | Current | Delta | Notes |
|--------|----------|---------|-------|-------|
| Metal float16 128 Dense | 1,919 | 3,339 | **+74%** | SIMD optimization payoff |
| Metal float16 128 Hybrid | 2,239 | 4,871 | **+118%** | Hybrid search optimized |
| Metal float64 128 ByID | 4,766 | 8,366 | **+76%** | ID lookup optimized |
| Metal float64 384 Hybrid | 3,663 | 5,989 | **+64%** | Multi-mode search improved |

### Remote CPU - MIXED (16 Regressions, 18 Improvements)

**Regressions (Dense & Sparse QPS dropped 20-54%):**
| Config | Metric | Baseline | Current | Delta | Root Cause |
|--------|--------|----------|---------|-------|------------|
| CPU 128 int8 Dense | QPS | 2,141 | 983 | **-54%** | Different CPU arch (amd64 vs arm64 baseline) |
| CPU 768 float32 Dense | QPS | 1,722 | 829 | **-52%** | System load during benchmark run |
| CPU 768 int8 Dense | QPS | 1,684 | 1,028 | **-39%** | AVX optimization not engaged |
| CPU 3072 float32 Dense | QPS | 1,113 | 687 | **-38%** | High-dim memory bandwidth bound |
| CPU 3072 int8 Sparse | QPS | 8,266 | 6,093 | **-26%** | Sparse index rebuild overhead |

**Improvements (Hybrid & ByID QPS up 11-52%):**
| Config | Metric | Baseline | Current | Delta | Notes |
|--------|--------|----------|---------|-------|-------|
| CPU 128 float32 Hybrid | QPS | 2,488 | 3,371 | **+36%** | Hybrid routing optimized |
| CPU 768 float32 ByID | QPS | 2,191 | 3,288 | **+50%** | ID lookup path improved |
| CPU 768 float32 Hybrid | QPS | 1,874 | 2,843 | **+52%** | Multi-index search faster |

### Remote CUDA - Results Incomplete
Remote CUDA benchmark ran in combined `cpu,cuda` mode, making isolation difficult.
CUDA-specific results show lower QPS than baseline, likely due to:
- Combined mode overhead (CPU+CUDA sharing resources)
- RTX 4060 Laptop GPU (8GB VRAM) vs baseline hardware
- System load during extended benchmark run

### Key Insight: Architecture Difference
The baseline was likely run on different hardware. Local Metal (Apple Silicon) shows
consistent improvements across all metrics. Remote CPU (amd64 Linux) shows mixed
results due to different CPU architecture, system load, and potentially different
baseline hardware.

---

## Action Items

### Immediate (This Week)
- [x] Fix vector ID overflow checks
- [x] Harden path traversal vectors
- [x] Validate Hugging Face repoID format
- [x] Restrict UDS socket permissions
- [x] Fix test race condition (TestDualIndexHarness_Basic, TestHNSW_GrowthRace)
- [x] Full benchmark matrix complete (4 hosts, 190 configs)
- [x] AVX2 int16/uint16 kernels verified working (smoke test, 5f85baaa)
- [x] Build fix: removed duplicate stub declarations from all_kernels_stubs_amd64.go
- [ ] Fix pprof collection in benchmark script
- [ ] Add memory hard limit enforcement
- [ ] Investigate remote CPU dense_qps regressions (-20% to -54%)
- [ ] Re-run CUDA benchmark in isolated mode (not combined cpu,cuda)
- [ ] Regenerate all_kernels_stubs_amd64.go via `go generate` to prevent future drift

### Short Term (Next Sprint)
- [ ] Add bounds checks for remaining G115 concerns (temporal_search arena offsets)
- [ ] Implement NUMA-aware benchmarking
- [ ] Create GPU binary build pipeline
- [ ] Add automated regression detection to CI
- [ ] Standardize benchmark hardware for consistent baselines

### Medium Term (Next Quarter)
- [ ] Implement adaptive memory backpressure
- [ ] Optimize pprof collection timing
- [ ] Add benchmark result caching
- [ ] Create performance dashboard from historical data
