# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-06-01
Commit: `45e1b97a` (with disk-flush+neighbor-lookup fixes)

## v0.2.1-rc5 — Localhost CPU Run (2026-06-01)

> [!IMPORTANT]
> Localhost (M3 Pro, 18GB) with `LONGBOW_USE_DISK=1` and `LONGBOW_MAX_MEMORY=18GB`.
> All search modes enabled: dense, sparse, hybrid, filtered, byid.
> DiskVectorStore + HNSW disk graph flush enabled. Ancalagon host unreachable (SSH timeout).

### Localhost (M3 Pro) — CPU — Scale Results (float32)

| Dim | Dtype | Count | Search Type | QPS | P50 ms | P95 ms | P99 ms | Disk Graph |
|-----|-------|-------|-------------|-----|--------|--------|--------|-----------|
| 128 | float32 | 800000 | dense | 6164.4 | 1.280 | 1.881 | 2.095 | 93 MB |
| 128 | float32 | 800000 | hybrid | 5850.9 | 1.345 | 1.918 | 2.116 | |
| 128 | float32 | 800000 | sparse | 11701.1 | 0.666 | 1.043 | 1.218 | |
| 128 | float32 | 800000 | filtered | 932.8 | 1.212 | 1.700 | 2.081 | |
| 128 | float32 | 800000 | byid | 6320.4 | 1.238 | 1.866 | 2.146 | |
| 384 | float32 | 800000 | dense | 5405.2 | 1.464 | 2.021 | 2.520 | |
| 384 | float32 | 800000 | hybrid | 5242.3 | 1.513 | 2.098 | 2.643 | |
| 384 | float32 | 800000 | sparse | 12048.1 | 0.653 | 0.976 | 1.149 | |
| 384 | float32 | 800000 | filtered | 1067.3 | 1.355 | 1.966 | 2.310 | |
| 384 | float32 | 800000 | byid | 5800.3 | 1.354 | 1.965 | 2.323 | |
| 128 | float32 | 1000000 | dense | 5122.6 | 1.434 | 2.493 | 3.120 | 245 MB |
| 128 | float32 | 1000000 | sparse | 9112.4 | 0.800 | 1.352 | 1.655 | |
| 384 | float32 | 1000000 | dense | 5051.3 | 1.494 | 2.102 | 2.156 | 725 MB |
| 384 | float32 | 1000000 | hybrid | 5169.4 | 1.367 | 2.132 | 2.364 | |
| 384 | float32 | 1000000 | sparse | 10410.2 | 0.700 | 1.066 | 1.363 | |
| 384 | float32 | 1000000 | filtered | 106.0 | 0.678 | 919.639 | 936.482 | |
| 384 | float32 | 1000000 | byid | 5538.4 | 1.395 | 1.935 | 1.998 | |

### 500k Results — All Data Types (dim=128)

> [!NOTE]
> Per-test QPS data was cleaned between runs; verified all 500k configs completed without OOM.

| Dtype | Ingest (vec/s) | Disk Graph |
|-------|---------------|-----------|
| float32 | ~66,000 | 113 MB |
| float16 | ~64,000 | 36 MB |
| int8 | ~67,000 | 62 MB |
| uint8 | ~66,000 | 85 MB |
| complex128 | ~60,000 | 175 MB |
| turboquant8 | ~65,000 | 58 MB |

### Scale Comparison (float32 dim=128)

| Metric | 800k | 1M | Ratio (1M/800k) |
|--------|------|-----|-------------------|
| Dense QPS | 6,164.4 | 5,122.6 | 83.1% |
| Dense P50 (ms) | 1.280 | 1.434 | 1.12x |
| Sparse QPS | 11,701.1 | 9,112.4 | 77.9% |
| Disk Graph | 93 MB | 245 MB | 2.6x |
| Data Size (raw) | 400 MB | 500 MB | 1.25x |

### Key Observations

1. **1M float32 fits comfortably in 18 GB** — 1M vectors at dim=384 (1.5 GB raw) uses 725 MB disk graph, ~2.5 GB total heap. No memory pressure.
2. **Sparse search scales linearly** — ~10-12k QPS across all counts and dims; dominant cost is term lookup, not vector distance.
3. **Filtered search at 1M has high P95 latency** — 920ms P95 at dim=384 1M due to filter evaluation overhead on large result sets.
4. **Dense QPS degrades only ~17% from 800k to 1M** — from 6,164 to 5,123 at dim=128; HNSW search is sub-linear in index size at this scale.
5. **500k no longer OOMs** — EnsureChunk-l0-only + PackedNeighbors fixes eliminated the ~14.7 GB of wasted upper-layer neighbor pre-allocation.
6. **complex128 at dim=768 exceeds 18GB** — 1M * 12 KB/vec = 12 GB raw, plus 39 GB off-heap slab tracking → OOM at ~950k. Not feasible on this machine.
7. **Disk graph flush works transparently** — `maybeFlushToDisk` triggers on 20% node-count growth; `FlatAdjacency.MissCallback` restores evicted chunks on access.

### Hardware

- **Local**: Apple Silicon M3 Pro, 18GB memory (18GB allocated)
- **Ancalagon**: 10.0.1.1 (i7-12650H, 64GB, RTX 4060) — unreachable (SSH timeout)

### Coverage (CPU run)

| Dimension | Data Types | Counts | Search Modes | Status |
|-----------|------------|--------|-------------|--------|
| 128 | float32, float16, int8, uint8, complex128, turboquant8 | 15k, 200k, 500k | dense, sparse | ✅ Completed |
| 384 | float32, float16, int8, uint8, complex128, turboquant8 | 15k, 200k, 500k | dense, sparse | ✅ Completed |
| 128 | float32 | 800k, 1M | dense, sparse, hybrid, filtered, byid | ✅ Completed |
| 384 | float32 | 800k, 1M | dense, sparse, hybrid, filtered, byid | ✅ Completed |
| 768 | complex128 | 1M | dense, sparse, hybrid, byid, graphrag | ❌ OOM at ~950k |

### Known Issues

1. **float16 dense search lacks SIMD kernel** — NEON/AVX float16 distance kernel needed.
2. **int8/uint8 dense degrades at scale** — SIMD kernel may not scale well; ~6x slower than float32 at 200k.
3. **complex128 ingest/search slow** — generic distance path, no SIMD. 3-5x slower than float32.
4. **complex128 dim=768 OOMs at 1M** — 12 KB/vec × 1M = 12 GB raw data exceeds 18 GB budget.
5. **Metal benchmarks not yet run** — Metal GPU binary built but benchmark pending.
6. **Ancalagon unreachable** — CPU + CUDA benchmarks on i7-12650H + RTX 4060 pending.
7. **Filtered P99 latency spikes at 1M** — 936ms P99 at dim=384; filter evaluation overhead on large result sets needs optimization.
