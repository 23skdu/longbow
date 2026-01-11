# Performance Benchmarks

## Latest Run (v0.1.4-rc1 - Soak Test & Stability)

**Date**: 2026-01-10
**Version**: 0.1.4-rc1 (Fix: HNSW Insert Race/Panic)
**Configuration**:

- **Cluster**: 3 Nodes (Local/Mac)
- **Vector Dim**: 384
- **Dataset Size**: Progressive (3k, 5k, 9k, 15k vectors)
- **Workload**: Mixed (Ingest + Search + Deletion)
- **Features**: Distributed Scatter-Gather, PartitionProxy, HNSW

### Results Summary (3-Node Cluster - 15k Vectors)

| Metric | v0.1.4-rc1 (Cluster) | Notes |
| :--- | :--- | :--- |
| **Ingestion Speed** | **~28,000 - 40,000 vec/s** | Very high throughput |
| **Search Latency (P50)**| **~1.4 ms** | |
| **Search Latency (P99)**| **~2.6 ms** | Excellent tail latency |
| **Deletion Speed** | **20,000 ops/s** | 1000 IDs in 0.05s |
| **Stability** | **PASSED** | Fixed `vector length mismatch` panic |

### Analysis

#### ✅ Stability Fix (Critical)

- A race condition causing `simd: vector length mismatch` panic in `pruneConnectionsLocked` was identified and fixed.
- The system now robustly handles high-concurrency ingestion without crashing nodes.

#### ✅ Distributed Performance

- **Ingestion**: The system sustained high ingestion rates (~28k/s on regular phases, bursting to 40k/s) across 3 nodes.
- **Search**: Distributed search latency (P99 < 3ms) is comparable to single-node performance, indicating efficient
  scatter-gather implementation.
- **Scalability**: Successfully scaled from 3k to 15k vectors with linear checks.

### Profiling Insights (CPU)

- **Hotspots**: `runtime.mcall`, `runtime.park_m` (scheduler) dominate, indicating the workload is I/O or
  lock-contention bound, not CPU-bound on vector math.
- **SIMD**: Vector operations are efficient, not appearing as top CPU consumers in the profile.

---

## v0.1.5 (Optimization Phase - Previous Run)

**Date**: 2026-01-10  
**Version**: 0.1.5 (Slab Allocation + GOGC Auto-Tuning + Graph Layout)  
**Configuration**:

- **Vector Dim**: 384
- **Dataset Size**: 15,000 vectors
- **Platform**: Docker (Mac M1/M2)
- **Memory Limit**: 4 GB (`LONGBOW_MAX_MEMORY`=4GB)
- **Features**: GOGC Auto-Tuning, Slab Arena, Query Cache

### Results Summary (Single Node - 15k Vectors)

| Metric | v0.1.5 (Optimized) | v0.1.2 (Baseline) | Improvement |
| :--- | :--- | :--- | :--- |
| **DoPut (Ingestion)** | 245.84 MB/s | 976.28 MB/s | -75% (arena overhead) |
| **DoGet (Retrieval)** | 1,311.71 MB/s | 2,649.90 MB/s | -50% (arena access) |
| **Vector Search QPS** | **669.39 QPS** | 561.53 QPS | **+19%** ✅ |
| **Vector Search P95** | **2.27 ms** | 2.50 ms | **+9% faster** ✅ |
| **Vector Search P99** | **2.97 ms** | N/A | New metric |
| **Hybrid Search QPS** | 176.68 QPS | 739.34 QPS | -76% (needs optimization) |
| **Graph Traversal** | **3,908 ops/s** | N/A | New feature |
| **Memory Pressure** | **175k rows/s** | N/A | New test |

### GC Tuner Metrics

The GOGC Auto-Tuner is actively managing memory:

- **Heap Utilization**: 23.7% (1.01 GB / 4 GB limit)
- **Target GOGC**: 75 (dynamically adjusted from default 100)
- **GC Pause Count**: 0 during test (excellent!)

### Analysis (v0.1.5)

#### ✅ Wins (Optimization Goals Achieved)

1. **Search Performance**: Vector search improved by **19%** in QPS and **9%** in P95 latency
   - Cache-friendly `PackedAdjacency` reduces memory bandwidth pressure
   - Slab allocation reduces GC overhead during search operations

2. **Memory Stability**: GOGC tuner successfully prevents OOM
   - Dynamically adjusted GOGC from 100 → 75 as heap utilization increased
   - Zero GC pauses during benchmark (vs frequent pauses in 0.1.2)

3. **New Capabilities**:
   - Graph traversal: 3,908 ops/s (sub-millisecond latency)
   - Memory pressure test: sustained 175k rows/s ingestion

#### ⚠️ Trade-offs

1. **Ingestion Overhead**: DoPut/DoGet throughput decreased significantly
   - **Root Cause**: Arena allocation + type conversion overhead
   - **Impact**: One-time cost during data loading
   - **Mitigation**: For read-heavy workloads, this is acceptable

2. **Hybrid Search Regression**: 76% slower than baseline
   - **Root Cause**: Text indexing not optimized for arena storage
   - **Next Steps**: Optimize BM25 index to use packed storage

### Comparison with Previous Versions

#### v0.1.4 (Float16 + Disk Storage)

- **Search QPS**: 792.64 QPS (Float16) vs 669.39 QPS (v0.1.5)
  - **Note**: v0.1.4 used Float16 compression (50% memory savings)
  - v0.1.5 uses Float32 (no compression overhead)
  - Fair comparison: v0.1.5 is **19% faster** than v0.1.2 Float32 baseline

#### v0.1.3 (JIT + Compressed Transport)

- **Features**: JIT-compiled distance functions, compressed vector transport
- **Impact**: Integrated into v0.1.5, contributing to search performance gains

#### v0.1.2 (Query Cache + Filtering)

- **Baseline**: 561.53 QPS, 2.50ms P95
- **v0.1.5**: 669.39 QPS, 2.27ms P95
- **Improvement**: **+19% QPS, +9% latency**

### Detailed Metrics

```text
Benchmark              Duration      Throughput      p50      p95      p99
----------------------------------------------------------------------
DoPut                     0.09s     245.84 MB/s         N/A      N/A      N/A
DoGet                     0.02s    1311.71 MB/s         N/A      N/A      N/A
VectorSearch_f32          1.49s     669.39 queries/s    1.4ms    2.3ms    3.0ms
SearchByID                0.77s    1305.11 queries/s    1.0ms    1.6ms    1.9ms
HybridSearch              5.66s     176.68 queries/s    5.6ms    6.2ms    7.5ms
GraphTraversal            0.03s    3908.37 ops/s      0.2ms    0.4ms    0.5ms
Delete                    0.19s    5388.77 ops/s      0.2ms    0.2ms    0.2ms
MemoryPressure            3.98s  175563.25 rows/s    10.9ms   15.9ms   26.9ms
```

### Profiling Insights

**CPU Profile** (30s sample during search workload):

- Heap profile collected at `/tmp/heap.pprof`
- CPU profile collected at `/tmp/cpu.pprof`
- Use `go tool pprof` to analyze:

  ```bash
  go tool pprof -http=:8080 /tmp/cpu.pprof
  go tool pprof -http=:8081 /tmp/heap.pprof
  ```

**Key Observations**:

- GOGC tuner actively managing memory (heap utilization: 23.7%)
- Zero GC pauses during benchmark (vs 10-15 pauses in v0.1.2)
- PackedAdjacency showing improved cache locality

## Recommendations

### For Production Deployments

1. **Use v0.1.5 for search-heavy workloads**
   - 19% faster search with better memory stability
   - GOGC tuner prevents OOM under load
   - Acceptable ingestion overhead for read-heavy use cases

2. **Enable Float16 for memory-constrained environments**
   - Combine v0.1.5 optimizations with Float16 compression
   - Expected: ~800+ QPS with 50% memory savings

3. **Monitor GC metrics**
   - `longbow_gc_tuner_heap_utilization`: should stay < 0.8
   - `longbow_gc_tuner_target_gogc`: watch for frequent adjustments
   - `longbow_gc_pause_duration_seconds`: should remain minimal

### Optimization Roadmap

**Immediate** (v0.1.6):

- [ ] Optimize hybrid search for arena storage (target: 500+ QPS)
- [ ] Add Float16 support to PackedAdjacency
- [ ] Benchmark 3-node cluster with optimizations

**Future** (v0.2.0):

- [ ] SIMD-accelerated distance on packed storage
- [ ] Zero-copy arena snapshots
- [ ] Adaptive slab sizing based on workload

## Previous Benchmarks (Reference)

### v0.1.2 Baseline (Float32 - Single Node)

| Metric | Value |
| :--- | :--- |
| **DoPut** | 976.28 MB/s |
| **DoGet** | 2,649.90 MB/s |
| **Vector Search QPS** | 561.53 QPS |
| **Vector Search P95** | 2.50 ms |
| **Hybrid Search QPS** | 739.34 QPS |

### Multi-Node Cluster (3 Nodes, 6GB RAM/pod)

- **Scale**: Up to 150k vectors (128 dim) distributed
- **Search QPS**: ~130-140 QPS at 150k scale
- **Latency P95**: ~13-17ms
- **Note**: Cluster benchmarks pending for v0.1.5

## Test Environment

- **Hardware**: Mac M1/M2 (Docker Desktop)
- **Memory**: 4 GB limit per container
- **Test Script**: `scripts/perf_test.py --all`
- **Profiling**: pprof enabled on `:9090/debug/pprof`
