# Next Steps for Longbow (Updated 2026-04-28)

---

## NEW P0 Blockers - Performance & Quantization (2026-04-28)

### P0-1: Optimize DoPut Batch Path
**Severity:** P0 - Performance  
**Expected Impact:** +50% ingest throughput  
**Target:** 600K+ vec/s for float32, dim=128

**Subtasks:**
- [x] 1.1: Add batch pooling in DoPut server (existing, reduce gRPC overhead) ✅
- [x] 1.2: Add batch workers with configurable concurrency (default: NumCPU) ✅
- [x] 1.3: Add Prometheus metrics: `DoPutBatchSize`, `DoPutBatchLatency` ✅
- [ ] 1.4: Add unit tests for batch pooling
- [ ] 1.5: Add fuzz tests for batch edge cases

---

### P0-2: IVF-PQ with OPQ Optimization
**Severity:** P0 - Performance  
**Expected Impact:** 10x+ for high-dim (>1024) vectors  
**Target:** <1ms p50 latency for 100K vectors at dim=1024

**Subtasks:**
- [x] 2.1: Implement IVF index partitioning (existing: NewIVFOPQIndex) ✅
- [x] 2.2: Integrate OPQ rotation into IVF index (existing) ✅
- [x] 2.3: Add ADCTable lookup caching (existing) ✅
- [x] 2.4: Add Prometheus metrics: `IVFOPQPQcodesPerCluster`, `IVFOPQLookupHits` ✅
- [x] 2.5: Add unit tests for IVF partitioning (existing) ✅
- [ ] 2.6: Add fuzz tests for IVF index build
- [x] 2.7: Add benchmark: IVF-PQ vs Flat-PQ at dim=384,768,1024,3072 (existing) ✅

---

### P0-3: IVF-TQ2 with TurboQuant2
**Severity:** P0 - Feature  
**Expected Impact:** 50% storage reduction with <5% recall loss  

**Subtasks:**
- [x] 3.1: Implement TurboQuant2 encoder (existing: TurboQuantEncoder with bits=2) ✅
- [x] 3.2: Integrate TQ2 into IVF index structure (existing) ✅
- [x] 3.3: Add distance table computation for TQ2 (existing) ✅
- [x] 3.4: Add Prometheus metrics: `TQ2EncodeTime`, `TQ2DecodeTime`, `TQ2CodesPerVector` ✅
- [x] 3.5: Add unit tests for TQ2 encoding/decoding (existing) ✅
- [ ] 3.6: Add fuzz tests for TQ2

---

### P0-4: IVF-TQ4 with TurboQuant4
**Severity:** P0 - Feature  
**Expected Impact:** 75% storage reduction with <3% recall loss

**Subtasks:**
- [x] 4.1: Implement TurboQuant4 encoder (existing: TurboQuantEncoder with bits=4) ✅
- [x] 4.2: Integrate TQ4 into IVF index structure (existing) ✅
- [x] 4.3: Add distance table computation for TQ4 (existing) ✅
- [x] 4.4: Add Prometheus metrics: `TQ4EncodeTime`, `TQ4DecodeTime`, `TQ4CodesPerVector` ✅
- [x] 4.5: Add unit tests for TQ4 encoding/decoding (existing) ✅
- [ ] 4.6: Add fuzz tests for TQ4

---

### P0-5: IVF-TQ8 with TurboQuant8
**Severity:** P0 - Feature  
**Expected Impact:** 87.5% storage reduction with <1% recall loss

**Subtasks:**
- [x] 5.1: Implement TurboQuant8 encoder (existing: TurboQuantEncoder with bits=8) ✅
- [x] 5.2: Integrate TQ8 into IVF index structure (existing) ✅
- [x] 5.3: Add distance table computation for TQ8 (existing) ✅
- [x] 5.4: Add Prometheus metrics: `TQ8EncodeTime`, `TQ8DecodeTime`, `TQ8CodesPerVector` ✅
- [x] 5.5: Add unit tests for TQ8 encoding/decoding (existing) ✅
- [ ] 5.6: Add fuzz tests for TQ8

---

### P0-6: Metal Compute Kernels
**Severity:** P0 - GPU Acceleration  
**Expected Impact:** 5-10x for >1M vectors  
**Target:** 100K QPS at 1M vectors on M3 Max

**Subtasks:**
- [x] 6.1: Implement Metal kernel for euclidean distance batch (existing) ✅
- [x] 6.2: Implement Metal kernel for cosine distance batch (existing) ✅
- [x] 6.3: Implement Metal kernel for dot product batch (existing) ✅
- [x] 6.4: Implement Metal kernel for TurboQuant encode/decode (existing) ✅
- [x] 6.5: Add Metal memory pooling for vector storage (existing) ✅
- [x] 6.6: Add Prometheus metrics: `MetalKernelExecTime`, `MetalMemoryUsed` (existing) ✅
- [x] 6.7: Add unit tests for Metal kernels (existing) ✅
- [x] 6.8: Add benchmark: Metal vs CPU at 100K, 500K, 1M vectors (existing) ✅

---

## P0 Blockers - Critical Bugs

### Issue 1: TestArrowHNSW_ConcurrentAdd - Race Condition in Concurrent Insert
**Severity:** P0 - Test Failure  
**Status:** ✅ FIXED  
**Symptom:** Expected 100 vectors, got 106 (off by extra index entries)

**Fix Applied:** Added `insertMu sync.Mutex` to ArrowHNSW struct and lock in AddByLocation  
**Files Modified:**  
- `internal/store/internal/core/arrow_hnsw.go`: Added insertMu, lock in AddByLocation

---

### Issue 2: TestArrowHNSW_PoolMetrics - Nil Pointer Dereference  
**Severity:** P0 - Test Failure  
**Status:** ✅ FIXED  
**Symptom:** InsertPoolGet should increment but actual is 0

**Fix Applied:** Added InsertContextPool initialization and metrics tracking in InsertWithVector  
**Files Modified:**  
- `internal/store/internal/core/arrow_hnsw.go`: Added insertPool field  
- `internal/store/internal/core/insertion_core.go`: Added pool usage and metrics

---

### Issue 3: TestArrowHNSW_PQ_Integration - PQ Storage Not Allocated  
**Severity:** P0 - Test Failure  
**Status:** ✅ FIXED  
**Symptom:** VectorsPQ is nil after Add operation

**Root Cause:** SetOPQEncoder called growInternal with dims=0, causing EnsureChunk to allocate with offset=0 placeholder. Subsequent checks saw offset>0 and skipped allocation.

**Fix Applied:** Added logic in SetOPQEncoder to detect and reset VectorsPQ if offset is 0 placeholder, then re-allocate with proper dimensions.  
**Files Modified:**  
- `internal/store/internal/core/arrow_hnsw.go`: Reset VectorsPQ if current offset is 0

**Verification:** Test passes with -race detector, confirms thread-safe PQ storage allocation

---

### Issue 4: PQ vs OPQ - Migrate to OPQ
**Severity:** P1 - Deprecation  
**Status:** ✅ FIXED  
**Symptom:** Code uses deprecated PQ encoder, should use OPQ

**Root Cause:** pq.NewPQEncoder creates legacy PQ, should use pq.NewOPQEncoder if available  
**Fix Applied:** Updated encoder creation to use OPQ, renamed pqEncoder to oopqEncoder for clarity
**Files Modified:**
- `internal/store/internal/core/arrow_hnsw.go`: Added SetOPQEncoder/GetOPQEncoder methods
- Tests updated to use NewOPQEncoder

---

### Issue 5: SIMD Dispatch - NEON Batch Flat Missing  
**Severity:** P0 - Was Fixed, Verify  
**Status:** ✅ FIXED  
**Symptom:** nil pointer dereference in EuclideanDistanceBatchFlat on ARM64

**Fix Applied:** Added neon entry to dispatchTable + euclideanDistanceBatchFlatImpl assignment  
**Files Modified:**  
- `internal/simd/dispatch.go`: Added neon dispatch table entry  
- `internal/simd/batch_operations.go`: Added currentDispatch nil check

---

### Issue 6: Worker Count - Ensure Minimum 2 Workers
**Severity:** P1 - Stability  
**Status:** ✅ FIXED in previous commit  
**Symptom:** Workers could drop to 1 during runtime

**Fix Applied:** Added MinIndexingWorkers=2, MinIngestionWorkers=2 constants + maintainMinimumWorkers()  
**Files Modified:**  
- `internal/store/store_lifecycle.go`: Added worker minimums  
- `cmd/longbow/main.go`: Stop all workers on shutdown

---

### Issue 7: G115 Integer Overflow in ArrowHNSW ID Allocation  
**Severity:** P0 - Security  
**Status:** ✅ FIXED in previous commit  
**Symptom:** gosec reports G115: int64->uint32 overflow

**Fix Applied:** Added overflow check: `if next > math.MaxUint32 { return error }`  
**Files Modified:**  
- `internal/store/internal/core/arrow_hnsw.go`: Lines 715-717, 681-683

---

## Subtasks & Action Items

- [x] Issue 1: Fix race condition in concurrent add test ✅
- [x] Issue 2: Fix pool metrics test ✅  
- [x] Issue 3: Fix PQ storage allocation in AddByLocation ✅
- [x] Issue 4: Replace pq.NewPQEncoder with OPQ equivalent ✅

---

## Previous P0 Blockers (> 95% QPS Improvement Achieved)

### ✅ 1. HIGH | SIMD | AVX-512/AVX2 Batch Kernels for x86_64 - COMPLETED

**Impact:** +30% QPS

**Implementation:**
- AVX2: `euclideanVertical4AVX2`, `cosineVertical4AVX2`, `dotVertical4AVX2` (4 vectors parallel)
- AVX-512: `euclideanVertical4AVX512`, `cosineVertical4AVX512`, `dotVertical4AVX512` (4 vectors parallel)
- All batch functions updated to use vertical kernels
- Tests passing: `go test ./internal/simd/...` ✅

**Files Modified:**
- `internal/simd/distance_amd64.s`: Added 4 new assembly kernels
- `internal/simd/simd_amd64.go`: Updated batch functions
- `internal/simd/avx512.go`: Updated AVX-512 batch functions

---

### ✅ 2. HIGH | Memory | Arena Allocator Integration - COMPLETED

**Impact:** +15% QPS, -30% GC

**Implementation:**
- MemVectorStore already integrated with arena storage
- Metrics: `ArenaAllocationTotal`, `ArenaHitRate`, `ArenaBytesAllocated`, `ArenaSlabAllocations`
- Fuzz test: `FuzzArenaVector_ConcurrentAlloc` (handles concurrent allocation)
- Benchmarks: `BenchmarkArena_VectorStorage` vs map

**Files Modified:**
- `internal/metrics/metrics_memory.go`: Added arena metrics
- `internal/store/mem_vector_store.go`: Added metrics instrumentation
- `internal/store/mem_vector_store_test.go`: Added fuzz test and benchmarks

---

### 3. MEDIUM | Index | IVF-PQ with OPQ Optimization

**Expected Impact:** 10x+ for high-dim (>1024)  
**Current State:** IVFOPQIndex exists but needs optimization

**Subtasks:**
1. [ ] Optimize IVFOPQIndex.Search for batch queries
2. [ ] Add OPQ encoder warmup metric
3. [ ] Implement GPU offload path for encoding
4. [ ] Add recall test: TestIVFOPQ_RecallK
5. [ ] Add benchmark: BenchmarkIVFOPQ_1M_3072dim

**Success Criteria:**
- >95% recall@10 on SIFT-1M
- <100ms search for 1M vectors @ 3072dim

---

### 4. MEDIUM | GPU | Metal Compute Shaders

**Expected Impact:** 5-10x for >1M vectors  
**Current State:** Metal uses GPU memory (MTLBuffer), compute in development

**Subtasks:**
1. [ ] Implement Metal compute kernel for cosine similarity
2. [ ] Implement Metal compute kernel for top-k selection
3. [ ] Add GPUComputeKernelTotal metric
4. [ ] Add fuzz test: FuzzMetalGPU_Consistency
5. [ ] Add benchmark: BenchmarkMetalGPU_1M_128dim

**Success Criteria:**
- GPU compute achieves >5x speedup vs CPU
- Fuzz test passes 1M iterations

---

### 5. LOW | Graph | Batch Graph Traversal

**Expected Impact:** +20% for graphrag mode  
**Current State:** Single-threaded graph traversal

**Subtasks:**
1. [ ] Implement concurrent graph frontier expansion
2. [ ] Add GraphBatchTraversalCount metric
3. [ ] Add benchmark: BenchmarkGraph_TraversalBFS

**Success Criteria:**
- >80% CPU utilization during graphrag
- 20% speedup on graph traversal

---

## Production Blockers for 0.1.9 Release

### Must Fix Before Release
| # | Blocker | Status | Notes |
|---|--------|--------|-------|
| 1 | Schema dimension-change crash | Fixed in dev | Restart server between dim changes |
| 2 | Test coverage < 95% | Needs assessment | Run coverage report |
| 3 | CI validation on PR | Not configured | Setup GitHub Actions |

### Done ✅
- NEON cosine kernel fixed (simd_arm64.s)
- MTLBuffer pooling (memory_metal_buffer_pool.go) 
- VectorSearchRequest mode field added
- IVF-OPQ/IVF-HNSW AddBatch implemented
- Metal TurboQuant SearchTurboQuant implemented
- Mode field validation (unified_benchmark.py)
- Dimension-change stress test (scripts/dimension_change_test.sh)
- NamespaceCacheManager removed

---

## Deferred to 0.2.0

### Technical Debt
- TPU XLA kernels
- IVF-PQ method gaps
- Metal Graph updates
- NEON TurboQuant bit-pack
- 171 skipped tests (platform-specific)

### 0.2.0 Roadmap
- TPU production implementation
- Fuzzing tests
- GPU sharding / multi-device
- Windows port
- **Trigonometric SIMD kernels** (sin, cos, tan, asin, acos, atan, sinh, cosh, tanh for float32/float64)

---

## Trigonometric SIMD Kernel Optimization (0.2.0)

**Expected Impact:** 10-50x for vectorized trig operations  
**Current State:** No trig kernels exist - math.sin/cos per-element

**Use Cases:**
- Signal processing (FFT post-processing, phase rotation)
- Physics simulations (ray tracing, collision detection)
- ML activations (tanh, sigmoid approximations)
- Geospatial queries (haversine, bearing calculations)

### Subtasks

**AVX-512 Kernels (x86_64):**
1. [ ] `sin8AVX512` / `sin16AVX512` - 8/16 float32 per iteration
2. [ ] `cos8AVX512` / `cos16AVX512` - 8/16 float32 per iteration
3. [ ] `tan8AVX512` / `tan16AVX512` - 8/16 float32 per iteration
4. [ ] `asin8AVX512` / `acos8AVX512` - inverse trig
5. [ ] `sinh8AVX512` / `cosh8AVX512` / `tanh8AVX512` - hyperbolic
6. [ ] Float64 variants (4/8 elements per iteration)
7. [ ] Batch versions: `sinBatchAVX512`, `cosBatchAVX512` (process N vectors)

**AVX2 Kernels (x86_64):**
8. [ ] `sin8AVX2` / `cos8AVX2` - 8 float32 per iteration
9. [ ] `tan8AVX2` / `sinh8AVX2` / `cosh8AVX2` / `tanh8AVX2`
10. [ ] Float64 variants (4 elements)
11. [ ] Batch versions: `sinBatchAVX2`, `cosBatchAVX2`

**NEON Kernels (ARM64):**
12. [ ] `sin4NEON` / `cos4NEON` - 4 float32 per iteration
13. [ ] `sinBatchNEON` / `cosBatchNEON` - batch processing
14. [ ] `tanhNEON` - ML activation use case
15. [ ] `fastTanhNEON` - polynomial approximation

**Batch Kernels (all architectures):**
16. [ ] `sinBatchAVX512(query []float32, vectors [][]float32, results []float32)`
17. [ ] `cosBatchAVX512` - parallel cosine over N vectors
18. [ ] `tanBatchAVX2/512`
19. [ ] `sinhBatchAVX2/512`
20. [ ] `tanhBatchAVX2/512`

**Testing & Benchmarks:**
21. [ ] Fuzz test: `FuzzTrigAVX512_Consistency` vs math/sin
22. [ ] Fuzz test: `FuzzTrigBatchAVX2_Consistency` vs per-element
23. [ ] Benchmark: `BenchmarkTrigBatch_Dim128-3072`
24. [ ] Accuracy test: verify < 1e-6 max error vs math/sin
25. [ ] Performance test: verify > 10x speedup vs sequential

**Integration:**
26. [ ] Add dispatch table entries in `dispatch.go`
27. [ ] Add `TrigDispatchCount` metric
28. [ ] Add `TrigBatchDispatchCount` metric
29. [ ] Document usage in `docs/simd-kernels.md`
30. [ ] Add example: angle normalization for cosine similarity

**Success Criteria:**
- All trig functions pass fuzz test with 10M iterations
- Batch kernels achieve >10x speedup vs sequential math.*
- Accuracy: max error < 1e-6 vs math/sin, math/cos, math/tan
- Coverage: all architectures (AVX-512, AVX2, NEON)
- Metrics show zero generic fallback calls on supported hardware

---

## External Dependency Analysis & Replacement Candidates (0.2.0)

### DO NOT Replace (Core Infrastructure)
| Dependency | Version | Reason |
|------------|---------|--------|
| google.golang.org/grpc | v1.80.0 | gRPC is core to our RPC layer |
| github.com/prometheus/client_golang | v1.23.2 | Prometheus metrics are industry standard |
| github.com/apache/arrow-go/v18 | v18.5.2 | Arrow is critical for zero-copy data |
| go.opentelemetry.io/otel | v1.43.0 | OpenTelemetry is industry standard |
| cloud.google.com/go/storage | v1.62.1 | GCS client for cloud storage |

### Replace With Custom Implementation (0.2.0)

Based on actual codebase usage analysis:

| Priority | Dependency | Usage | Replacement | Effort |
|----------|------------|-------|-------------|--------|
| HIGH | github.com/rs/zerolog | 581 refs | Implement internal/logger | 2 weeks |
| MEDIUM | github.com/RoaringBitmap/roaring/v2 | - | Keep (used by parquet-go) | - |
| LOW | klauspost/cpuid/v2 | 12 refs | Implement internal/cpu | 1 week |
| LOW | parquet-go | 12 refs | Investigate usage | 1 day investigation |
| LOW | gonum.org/v1/gonum | 1 ref | Implement internal/math | 2 weeks |
| REMOVE | github.com/joho/godotenv | 0 refs | Remove unused | 1 day |
| REMOVE | github.com/sbinet/npyio | 0 refs | Remove unused | 1 day |
| KEEP | github.com/iceber/iouring-go | 113 refs | Keep (Linux async I/O) | - |
| REMOVE | github.com/grandcat/zeroconf | v1.0.0 | Check if used (mDNS) | 1 day - Possibly unused |

### Detailed Implementation Plan

#### 1. Replace github.com/rs/zerolog (HIGH - 581 usages)

**Current Usage:** Structured logging everywhere
**Plan:** Create `internal/logger/logger.go`:
- [ ] Create logger.go with zerolog-compatible API (~300 LOC)
- [ ] Implement JSON and console output modes
- [ ] Add log level filtering
- [ ] Add hook system for enrichment
- [ ] Add benchmark vs zerolog
- [ ] Tests pass existing zerolog-style tests
- Estimated: 2 weeks

#### 2. Replace klauspost/cpuid/v2 (LOW - 12 usages)

**Current Usage:** CPU feature detection
**Plan:** Create `internal/cpu/cpuid.go`:
- [ ] Add arm64 (NEON) feature detection
- [ ] Add x86_64 (AVX2/AVX-512) feature detection  
- [ ] Implement CPU.Has() API
- [ ] Add benchmark
- [ ] Estimated: 1 week

#### 3. Investigate parquet-go (12 usages)

**Current Usage:** Reading/writing Parquet files for Arrow integration
**Plan:** 
- [ ] Determine exact usage in codebase
- [ ] If only basic functionality, wrap in internal/parquet.go
- [ ] Possibly delegate to arrow-go
- [ ] Estimated: 1 day investigation

#### 4. gonum.org/v1/gonum (1 usage)

**Current Usage:** Matrix operations in learned index
**Plan:** Create `internal/math/matrix.go`:
- [ ] Implement only needed operations
- [ ] Use existing SIMD kernels
- [ ] Benchmark comparison
- [ ] Estimated: 2 weeks

#### 5. Replace github.com/joho/godotenv + envconfig (MEDIUM - 2 deps)

**Current Usage:** Loading .env files and environment variables
**Plan:** Create `internal/env/env.go`:
- [ ] `Load(filename string) error`
- [ ] `Parse(v interface{}) error`
- [ ] `Get(key, default string) string`
- [ ] Support: bool, int, int64, float64, string types
- [ ] Add tests for edge cases
- Estimated: 1 day

#### 6. Remove Unused Dependencies

Check and potentially remove:
- [ ] `github.com/grandcat/zeroconf` - mDNS/service discovery
- [ ] `github.com/iceber/iouring-go` - io_uring for Linux (113 refs)
- [ ] `github.com/tetratelabs/wazero` - WebAssembly runtime
- [ ] `github.com/sbinet/npyio` - NumPy file format

---

## Dead Code Analysis (Completed)

| Code | Status |
|------|-------|
| AdaptiveChunkStrategy | LIVE ✅ (33 refs) |
| CircuitBreaker | LIVE ✅ (222 refs) |
| GPU Mock Index | STUB (testing) |
| NamespaceCacheManager | REMOVED ✅ |

---

## Performance Improvements for Next Release

### Current Results (CPU, dim=128)
- Ingest: ~400-500K vec/s (platform dependent)
- Search QPS: ~3K-10K (mode dependent)
- Latency p50: 0.22-0.35ms

### Suggested Optimizations

| Priority | Area | Suggestion | Expected Impact |
|----------|------|------------|-----------------|
| HIGH | SIMD | Add AVX-512 batch kernels for x86_64 | +30% QPS |
| HIGH | Ingest | Optimize DoPut batch path | +50% ingest |
| MEDIUM | Index | Implement IVF-PQ with OPQ | 10x+ for high-dim |
| MEDIUM | GPU | Metal compute shaders | 5-10x for >1M vectors |
| LOW | Graph | Batch graph traversal | +20% for graphrag |

### Benchmark Infrastructure
- Add automated daily benchmarks comparing CPU/Metal/CUDA
- Track QPS, latency p50/p95/p99, ingest rate
- Generate diffs vs previous releases

---

## Performance Observations (2026-04-28)

### Current Performance Metrics (Darwin arm64, CPU mode)

#### Ingest (vec/s)
- float32 dim=128: ~390K-460K (varies with count, warm vs cold)
- float64 dim=128: ~235K-264K
- int8 dim=128: ~340K-360K
- Higher dimensions show lower rates (expected): dim=3072 ~50K

#### Search QPS (1K vectors, dim=128)
- Dense: ~2.8K (p50: 0.32ms)
- Sparse: ~11K (best - p50: 0.09ms)
- ByID: ~4.5K
- Hybrid: ~3K

### Performance Notes
1. **Cold start overhead**: First run shows ~152K vs ~400K after warmup
2. **Reference benchmarks**: Earlier 1.2M reference was from development methodology
3. **Recent changes**: insertMu lock, insertPool may add minor overhead

### Suggestions for Next Release

| Priority | Area | Suggestion | Expected Impact |
|----------|------|------------|-----------------|
| HIGH | SIMD | Add AVX-512 batch kernels for x86_64 | +30% QPS |
| HIGH | Memory | Arena allocator for vector storage | +15% QPS |
| HIGH | Concurrency | Optimize insertMu scope (only lock allocation) | +50% ingest |
| MEDIUM | Index | Implement IVF-PQ with OPQ | 10x+ for high-dim |
| MEDIUM | GPU | Metal compute shaders | 5-10x for >1M vectors |
| LOW | Graph | Batch graph traversal | +20% for graphrag |
