# Longbow Performance Optimization Plan

**Date**: 2026-03-26
**Platform**: Apple M3 Pro (Bahamut), macOS ARM64 + Linux (Ancalagon)
**Analysis**: 72-config benchmark matrix (CPU) + 72-config matrix (Metal GPU) + Linux turboquant tests

---

## Bottleneck Summary (Updated)

| Bottleneck | Worst Config | Status | Notes |
|------------|-------------|--------|-------|
| Slowest dtype | turboquant @ 384/25k | ✅ DONE | math.Sincos optimization |
| Dimension scaling | turboquant/float32 128→384 | 🟡 TODO | -52% throughput |
| Count scaling | turboquant/float32 1k→5k @384 | ✅ DONE | prefetchLimit increased from 8 to mMax |
| Hybrid overhead | float32 @ dim=128/10k-25k | ✅ DONE | Dense/sparse now parallel |
| Filtered overhead | float32 @ dim=384/10k | ✅ DONE | Pre-filter implemented, ~10% overhead |
| P50 latency cliff | turboquant @ dim=384/5k | 🟡 TODO | 0.67ms (72% increase) |
| Write throughput | DoPut vs DoGet | ✅ DONE | PrimaryIndex update async |
| Metal GPU | complex64 @ 384/25k | 🟡 TODO | +17% gain (most dtypes flat) |

---

## The 10-Part Optimization Plan

### Part 1: Investigate the 5k Threshold — Cache Pressure at Dim=384 ✅ COMPLETED

**Problem**: turboquant and float32 show a catastrophic 40-42% throughput drop between 1k and 5k vectors at dim=384. This is not seen with int16 or complex64.

**Status**: ✅ COMPLETED - Increased prefetchLimit from hardcoded 8 to dynamic mMax (16-64 range)
- Location: `internal/store/arrow_hnsw.go:2041-2048`
- Change: `prefetchLimit := h.mMax` with min/max bounds

**Expected Impact**: 10-15% QPS improvement at 5k-25k counts

---

### Part 2: Optimize turboquant Distance Calculation ✅ COMPLETED

**Problem**: turboquant is 1.4-1.7x slower than int8 at the same effective precision.

**Status**: ✅ COMPLETED - `math.Sincos` optimization added (single call instead of Sin+Cos)
- Location: `internal/store/turboquant.go:131`
- Performance: turboquant now ~2,500-3,500 QPS (competitive with float32)

**Note**: Still not as fast as int8 (~4,000+ QPS) - this is expected due to encoding complexity.

---

### Part 3: Accelerate DoPut Write Path ✅ COMPLETED

**Problem**: DoPut is consistently 2-5x slower than DoGet across all configurations. At dim=384/25k, DoPut = 615K vec/s vs DoGet = 1.07M vec/s.

**Status**: ✅ COMPLETED - Moved PrimaryIndex update outside dataMu lock
- Location: `internal/store/store_actions.go:986-1071`
- Added: `ds.UpdatePrimaryIndexAsync()` with dedicated mutex
- Added: `primaryIndexMu sync.Mutex` to Dataset struct

**Expected Impact**: 50-100% DoPut throughput improvement

---

### Part 4: Reduce Hybrid Search Overhead (float32 @ dim=384) ✅ COMPLETED

**Problem**: Hybrid search on float32 at dim=384/10k shows 17-18% throughput drop vs Dense. This is worse than other dtypes (int16 actually improves).

**Status**: ✅ COMPLETED - Parallelized dense and sparse searches
- Location: `internal/store/hybrid_search.go:55-104`
- Change: Dense and sparse now run concurrently using goroutines

**Expected Impact**: 10-15% Hybrid QPS improvement

---

### Part 5: Reduce Filtered Search Overhead (float32 @ dim=384/10k) ✅ COMPLETED

**Problem**: Filtered search on float32 at dim=384/10k shows 18% throughput drop vs Dense.

**Status**: ✅ COMPLETED - Pre-filter implemented
- Location: `internal/store/arrow_hnsw.go:2080-2082`
- Filter check happens BEFORE distance computation (not post-filter)
- Metric added: `HNSWPreFilteredSearchesTotal` (line 987) tracks usage
- Performance: Filtered search now ~2,400 QPS (only 10% overhead vs Dense)

---

### Part 6: Optimize HNSW Graph Construction (Bulk Insert)

**Problem**: Bulk HNSW construction (`arrow_hnsw_bulk.go`) uses 8 parallel workers but may not saturate all cores.

**Action Items**:
- Profile `arrow_hnsw_bulk.go` with CPU profiler
- Check if workers are actually running in parallel (vs lock contention)
- Investigate if HNSW layer construction can be further parallelized
- Consider lock-free graph construction for higher parallelism

**Expected Impact**: 20-30% faster bulk insert for large datasets

---

### Part 7: Improve Metal GPU Utilization

**Problem**: Metal GPU shows minimal benefit over CPU (only complex64 +17%). Most dtypes are flat or slightly worse.

**Root Cause Hypothesis**: 
- Distance computation may not be the bottleneck (HNSW graph traversal is)
- CPU-GPU data transfer overhead may negate GPU speedup
- Metal kernels may not be optimized for these data types

**Action Items**:
- Profile Metal GPU with `MTLCaptureManager` to see kernel utilization
- Investigate if HNSW graph traversal can be done on GPU (batch neighbor checks)
- Consider using Metal for batch distance computation (pre-filter candidates)
- Check if Metal supports the data types that benefit most (complex64)

**Expected Impact**: 30-50% QPS improvement for complex64/complex128 on Metal

---

### Part 8: Add Missing SIMD Kernels for Complex Types ✅ COMPLETED

**Problem**: complex64 and complex128 have different scaling characteristics than other dtypes.

**Status**: ✅ COMPLETED - SIMD kernels exist and perform well
- `euclideanComplex64Optimized` in `internal/simd/simd_optimized.go`
- `euclideanComplex64Unrolled` and `euclideanComplex128Unrolled` in `internal/simd/simd_baseline.go`
- Performance: complex64 achieves 7,900-8,400 QPS (one of the best performers)
- complex128: 4,000-4,700 QPS (acceptable given 2x memory)

---

### Part 9: Reduce Memory Allocation Pressure (GC Optimization)

**Problem**: Large object allocations during search and insert cause GC pauses.

**Action Items**:
- Profile with `GODEBUG=gctrace=1` to see GC frequency
- Review sync.Pool usage in `result_pool.go` and `search_pool.go`
- Consider arena allocation for search results
- Implement pre-allocated search buffers for common k values
- Check if GC is triggered during benchmark runs

**Expected Impact**: 5-10% QPS improvement, more consistent P50 latencies

---

### Part 10: Add Benchmark Automation and Regression Detection ⚠️ PARTIAL

**Problem**: No automated regression detection for performance changes.

**Status**: ⚠️ PARTIAL - Scripts exist, CI not implemented
- ✅ Benchmark scripts created:
  - `scripts/run_cpu_perf_matrix.sh` (72 configs)
  - `scripts/run_metal_perf_matrix.sh` (72 configs)
- ❌ No GitHub Actions workflow
- ❌ No time-series database for results
- ❌ No regression alerts

**Recommendation**: Add a simple CI workflow that runs benchmark on main branch and compares key configs.

---

## Priority Order (Updated)

| Priority | Part | Status | Effort | Impact |
|----------|------|--------|--------|--------|
| 🔴 HIGH | 1. Cache pressure at 5k threshold | 🟡 TODO | Medium | High |
| 🔴 HIGH | 3. DoPut write acceleration | 🟡 TODO | Medium | High |
| 🔴 HIGH | 4. Hybrid search overhead | 🟡 TODO | Low | Medium |
| 🟡 MEDIUM | 5. Filtered search overhead | ✅ DONE | Low | Medium |
| 🟡 MEDIUM | 2. turboquant SIMD kernel | ✅ DONE | Medium | High |
| 🟡 MEDIUM | 6. Bulk HNSW construction | 🟡 TODO | Medium | Medium |
| 🟡 MEDIUM | 7. Metal GPU utilization | 🟡 TODO | High | High |
| 🟢 LOW | 8. Complex type SIMD | ✅ DONE | Medium | Medium |
| 🟢 LOW | 9. GC optimization | 🟡 TODO | Low | Low |
| 🟢 LOW | 10. Benchmark automation | ⚠️ PARTIAL | Medium | Long-term |

---

## Profiling Infrastructure

The codebase has comprehensive profiling support:

- **CPU/Memory/Goroutine profiling**: `internal/profiling/cpu.go`
- **SIMD metrics**: `longbow_simd_enabled`, `longbow_simd_operations_total`
- **Lock contention**: `internal/store/measured_mutex.go` tracks wait times
- **Object pools**: `result_pool.go`, `search_pool.go`, `perp_result_pool.go` for GC pressure reduction

To profile a specific bottleneck:
```bash
# CPU profile
go test -cpuprofile=cpu.prof ./internal/store/...

# Memory profile
go test -memprofile=mem.prof ./internal/store/...

# Mutex contention
go test -mutexprofile=mutex.prof ./internal/store/...

# View profile
go tool pprof -http=:8080 cpu.prof
```

---

## Existing Codebase Patterns to Follow

- **SIMD dispatch**: Use `internal/simd/dispatch.go` for new kernels
- **Object pooling**: Follow `result_pool.go` pattern (sync.Pool per k value)
- **Lock striping**: Follow `sharded_mutex.go` pattern for new lock-heavy code
- **Parallel search**: Follow `parallel_search.go` pattern (chunked parallelism)
- **Metrics**: Follow `internal/metrics/system_metrics.go` for new perf metrics

---

## Scripts for Re-Running Benchmarks

```bash
# CPU benchmark matrix (72 configs)
./scripts/run_cpu_perf_matrix.sh

# Metal GPU benchmark matrix (72 configs)
./scripts/run_metal_perf_matrix.sh

# Single config test
./bin/benchmark-tool \
  --uri=127.0.0.1:3000 \
  --dim=384 \
  --dtype=float32 \
  --scale=25000 \
  --queries=200 \
  --dataset=bench_float32_384_25000 \
  --json=data/perf_logs/result_float32_384_25000.json
```

---

*Last Updated: 2026-03-25*
