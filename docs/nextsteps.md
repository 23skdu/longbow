# Longbow Next Steps - Stability & Performance Recommendations

> Generated: 2026-05-20
> Based on: Security audit (793 nosec suppressions reviewed), race condition analysis, code quality review
> Commit: `04edb659` (fix: resolve race condition, add overflow checks, and harden security)

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

Based on comparison of `docs/performance.md` historical data:

### Search Performance (QPS) - Key Changes

| Metric | v0.2.0 Baseline | v0.2.1 Current | Delta | Notes |
|--------|-----------------|----------------|-------|-------|
| Metal float32 128 Dense | ~4200 | 4495 | +7% | SIMD optimization payoff |
| Metal float32 3072 Dense | ~950 | 1069 | +12.5% | High-dim optimization |
| CPU float32 128 Dense | ~2100 | 2274 | +8.3% | AVX improvements |
| CUDA float32 128 Dense | ~2200 | 2316 | +5.3% | CUDA kernel tuning |

### Ingestion Performance (MB/s) - Key Changes

| Metric | v0.2.0 Baseline | v0.2.1 Current | Delta | Notes |
|--------|-----------------|----------------|-------|-------|
| Metal float32 128 | ~200 | 222 | +11% | Batch optimization |
| Metal float64 3072 | ~1100 | 1225 | +11.4% | Memory alignment fixes |
| CPU float32 3072 | ~340 | 375 | +10.3% | Parallel ingestion |

### Identified Regressions

| Area | Issue | Severity | Fix Status |
|------|-------|----------|------------|
| Temporal search latency | P95 spikes at 384 dims | Medium | Under investigation |
| GraphRAG at high dims | GlobalGraphRAG P99 > 35ms | Low | Expected for graph traversal |
| TurboQuant ingestion | Lower throughput than float32 | Medium | Known trade-off for compression |

---

## Action Items

### Immediate (This Week)
- [x] Fix vector ID overflow checks
- [x] Harden path traversal vectors
- [x] Validate Hugging Face repoID format
- [x] Restrict UDS socket permissions
- [x] Fix test race condition
- [ ] Fix pprof collection in benchmark script
- [ ] Add memory hard limit enforcement

### Short Term (Next Sprint)
- [ ] Add bounds checks for remaining G115 concerns (temporal_search arena offsets)
- [ ] Implement NUMA-aware benchmarking
- [ ] Create GPU binary build pipeline
- [ ] Add automated regression detection to CI

### Medium Term (Next Quarter)
- [ ] Implement adaptive memory backpressure
- [ ] Optimize pprof collection timing
- [ ] Add benchmark result caching
- [ ] Create performance dashboard from historical data
