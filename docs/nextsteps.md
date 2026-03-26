# Longbow Performance Optimization Plan

**Date**: 2026-03-26
**Platform**: Apple M3 Pro (Bahamut), macOS ARM64 + Linux (Ancalagon)

---

## Remaining Bottlenecks

| Bottleneck | Worst Config | Status | Notes |
|------------|-------------|--------|-------|
| Dimension scaling | turboquant/float32 128→384 | ✅ DONE | Blocked processing for 384 dims |
| P50 latency cliff | turboquant @ dim=384/5k | 🟡 TODO | 0.67ms (72% increase) |
| Metal GPU | complex64 @ 384/25k | 🟡 TODO | +17% gain (most dtypes flat) |

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

---

## Active Work Items

### Dimension Scaling (128→384): -52% throughput

**Problem**: Large dimension vectors show significant throughput drop when scaling from 128 to 384 dimensions.

**Next Steps**:
- Profile memory access patterns at 384 dims
- Consider blocked/tiled distance computation
- Investigate if SIMD width is limiting factor

---

### P50 Latency Cliff: turboquant @ dim=384/5k

**Problem**: P50 latency increases 72% (0.67ms) at 5k vectors with turboquant.

**Next Steps**:
- Profile with latency histograms
- Check if GC pauses correlate with latency spikes
- Investigate HNSW search path for latency outliers

---

### Metal GPU Utilization

**Problem**: Metal GPU shows minimal benefit over CPU (only complex64 +17%).

**Root Cause**: 
- HNSW graph traversal is the bottleneck, not distance computation
- CPU-GPU transfer overhead negates GPU speedup

**Next Steps**:
- Use Metal only for batch distance pre-filter (not full search)
- Profile with MTLCaptureManager to validate

---

## Profiling Infrastructure

```bash
# CPU profile
go test -cpuprofile=cpu.prof ./internal/store/...

# Memory profile
go test -memprofile=mem.prof ./internal/store/...

# GC trace
GODEBUG=gctrace=1 go test ./internal/store/...
```

---

*Last Updated: 2026-03-26*
