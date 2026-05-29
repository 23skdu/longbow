# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-05-29

## v0.2.1-rc6 — QPS Fix (2026-05-29)

> [!IMPORTANT]
> **Int16/Uint16 Regression Fixed**: The int16/uint16 distance functions used `int64` accumulators (`int64 MUL` — 3-4 cycle latency on ARM64), while uint8/int8 used `float64 accumulators` (`float64 FMUL` — 1 cycle latency, FMA-fused). Switched int16/uint16 to `float64` accumulators, matching the uint8/int8 pattern. Results: **32x latency improvement** for int16 at count=5000.

### Performance Impact (Local CPU, M3)

| Type | Dim | Count | Metric | Before Fix | After Fix | Improvement |
|------|-----|-------|--------|-----------|-----------|-------------|
| int16 | 128 | 5,000 | p50 latency | 17.02 ms | **0.53 ms** | **32.1x faster** |
| int16 | 384 | 1,000 | p50 latency | 4.50 ms | **1.02 ms** | **4.4x faster** |
| uint16 | 384 | 5,000 | p50 latency | 17.89 ms | **1.02 ms** | **17.5x faster** |
| uint8 | 128 | 1,000 | p50 latency | 1.64 ms | **0.83 ms** | **2.0x faster** |

All integer types now consistently use `float64` accumulators, matching the fastest code path.

### QPS Aggregation Bug Fix

> [!IMPORTANT]
> **QPS values from all prior releases were inflated by a bench-tool aggregation bug**. Search modes (dense,hybrid,sparse,filtered,byid) ran as 5 concurrent goroutines, each computing QPS = 1000/avgLatency under contention from the other 4 modes. This produced QPS values ~5x higher than actual sustained throughput. The fix (`86b56fb7`) runs modes sequentially with total wall-clock QPS. **Latency (p50/p95/p99) was always accurate** since it's measured per-query.

| Type | Local CPU (M3) — Corrected QPS | Ancalagon CPU — Corrected QPS |
|------|-------------------------------|-------------------------------|
| float16 (128d, 1k) | 2,995 QPS (0.30ms) | *(pending)* |
| float32 | *(pending)* | *(pending)* |
| int8 | *(pending)* | *(pending)* |
| int16 | *(pending)* | *(pending)* |

*Full corrected results from the current benchmark matrix run will replace this table.*

### Key Stability Improvements

1. **Int16/Uint16 Distance Kernel**: Switched from `int64` to `float64` accumulators in 6 functions (`euclideanInt16Unrolled4x`, `dotInt16Unrolled4x`, `euclideanUint16Unrolled4x`, `dotUint16Unrolled4x`, `cosineDistanceInt16Unrolled4x`, `cosineDistanceUint16Unrolled4x`). This eliminates the 3-4 cycle `int64 MUL` bottleneck on ARM64 NEON, leveraging the fast 1-cycle `float64 FMUL` pipe.

2. **Benchmark Script Fix**: Resolved `-search-modes all` expansion bug in `scripts/unified_benchmark.py` — the literal string `"all"` was passed to bench-tool instead of expanding to actual mode names.

3. **QPS Aggregation Fix** (`86b56fb7`): Search modes now run sequentially (not concurrently). QPS computed as `queries / totalElapsed` from wall-clock time, giving accurate sustained throughput.

---

---

## v0.2.0-rc2 Release Candidate - Final Hardening (2026-05-05)

> [!IMPORTANT]
> **QPS values in this section are inflated ~5x by the bench-tool concurrent-mode bug** (discovered and fixed in `86b56fb7`). **Latency (p50/p95/p99) values are accurate.** These results are preserved for historical reference of what the buggy tool reported.

### Search Performance Breakdown (dim=128, count=5000) [INFLATED QPS]

| Mode                | Target (v0.1.9) | **Reported (buggy QPS)** | Platform                | Status (latency)  |
| ------------------- | --------------- | ------------------------ | ----------------------- | ----------------- |
| **Dense Search**    | > 20,000 QPS    | **30,576 QPS**           | Local CPU (M3)          | **INFLATED**      |
| **Dense Search**    | > 20,000 QPS    | **29,268 QPS**           | Local Metal (M3)        | **INFLATED**      |
| **Dense Search**    | > 20,000 QPS    | **29,223 QPS**           | Remote CPU (Ancalagon)  | **INFLATED**      |
| **Dense Search**    | > 20,000 QPS    | **30,013 QPS**           | Remote CUDA (Ancalagon) | **INFLATED**      |
| **Temporal Search** | > 12,000 QPS    | **29,389 QPS**           | Local CPU (M3)          | **INFLATED**      |
| **Temporal Search** | > 12,000 QPS    | **29,817 QPS**           | Local Metal (M3)        | **INFLATED**      |
| **Temporal Search** | > 12,000 QPS    | **19,886 QPS**           | Remote CPU (Ancalagon)  | **INFLATED**      |
| **Temporal Search** | > 12,000 QPS    | **20,096 QPS**           | Remote CUDA (Ancalagon) | **INFLATED**      |
| **Sparse Search**   | > 4,000 QPS     | **59,400 QPS**           | Local Metal (M3)        | **INFLATED**      |
| **GraphRAG Search** | > 3,000 QPS     | **47,960 QPS**           | Local Metal (M3)        | **INFLATED**      |
| **Geospatial**      | > 5,000 QPS     | **36,617 QPS**           | Local Metal (M3)        | **INFLATED**      |

*Corrected QPS values will replace this table once the current benchmark run completes.*

### Latency Metrics (Local M3, dim=128, count=5000)

| Search Mode  | p50 (ms) | p95 (ms) | p99 (ms) |
| ------------ | -------- | -------- | -------- |
| Dense        | 0.228    | 0.493    | 0.757    |
| Sparse       | 0.129    | 0.250    | 0.372    |
| GraphRAG     | 0.156    | 0.276    | 0.338    |
| Temporal     | 0.246    | 0.493    | 0.756    |
| LearnedIndex | 2.039    | 2.731    | 2.821    |

### Ingestion Performance (vec/s)

| Platform     | Mode | float32 (128d) | Target  | Status         |
| ------------ | ---- | -------------- | ------- | -------------- |
| Darwin arm64 | CPU  | **459,418**    | 150,000 | **OK (+206%)** |
| Linux x86_64 | CPU  | **371,689**    | 150,000 | **OK (+147%)** |

---

## Target Baselines (v0.2.2, Corrected QPS)

> [!NOTE]
> All QPS targets revised downward from v0.1.9 era because the original measurements were inflated ~5x by the concurrent-mode bug. Latency targets are unchanged.

- **Dense Search (Float32, 384d, 5k)**: > 4,000 QPS (p50 < 1.0ms)
- **Hybrid Search**: > 4,000 QPS
- **Sparse Search**: > 4,000 QPS
- **Filtered Search**: > 4,000 QPS
- **ByID Search**: > 4,000 QPS
- **Temporal Search**: > 2,500 QPS (p50 < 1.0ms)
- **GraphRAG Search**: > 3,000 QPS
- **Geospatial**: > 3,000 QPS
- **LearnedIndex**: > 500 QPS
- **Ingestion (Bulk, float32 128d)**: > 150,000 vec/s

---

## Hardware

- **Local**: Apple Silicon M3, 18GB memory
- **Remote (ancalagon)**: NVIDIA RTX 4060 Laptop GPU, 8GB VRAM, 22GB RAM, 16 cores (AMD64 Linux)

### Benchmark Matrix Coverage

- **Platforms:** CPU, Metal (local), CUDA (remote ancalagon)
- **Data Types:** float16, float32, float64, int8, int16, int32, int64, uint8, uint16, uint32, uint64, complex64, complex128, turboquant2, turboquant4, turboquant8
- **Dimensions:** 128, 384, 768, 1024, 3072
- **Counts:** 500, 1000, 5000, 15000, 50000, 100000
- **Search Modes:** dense, hybrid, sparse, filtered, byid, graphrag, geo, temporal, learned_index
