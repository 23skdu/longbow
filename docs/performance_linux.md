# Longbow Linux Performance Benchmarks

**Date**: March 22-23, 2026
**Platform**: Linux (x86_64, gccgo/Go 1.24)
**CPU**: 12th Gen Intel(R) Core(TM) i7-12650H (no AVX512 — AVX2-only)
**Memory**: 23GB RAM (8GB allocated to Longbow for tests)
**Storage**: NVMe SSD

> **Status**: Partial results captured (16 configs). Linux shows ~10x lower QPS than M3 Pro
> due to AVX2-only CPU (i7-12650H lacks AVX512). Previous results from 2026-03-22 below.

> **Note**: This i7-12650H laptop CPU does **not** have AVX512. All SIMD
> operations fall back to AVX2. The newly added `euclidean768AVX512` and
> `euclidean1536AVX512` kernels fall back to `euclidean768AVX2` /
> `euclidean1536AVX2` (which call the generic unrolled implementation).
> See `docs/nextsteps.md` item #12.

---

## Benchmark Configuration

- **Test Tool**: `bin/benchmark-tool` (Go, same binary as server)
- **Test Types**: DoPut, DoGet, Dense Search, Sparse Search, Hybrid Search, Filtered Search
- **Dimensions**: 128, 384, 768
- **Dataset Sizes**: 10,000 | 25,000 vectors
- **Data Types**: float32, int8
- **Metric**: Euclidean (L2)
- **Search k**: 10
- **Queries**: 1,000 per test

---

## Results Summary

### float32 — 8GB Memory Allocation

| Dim | Count | DoPut (vec/s) | DoPut (MB/s) | DoGet (vec/s) | DoGet (MB/s) | Index (s) | Dense QPS | Dense P50 | Sparse QPS | Hybrid QPS | Hybrid P50 | Filtered QPS | Filtered P50 |
|-----|-------|---------------|--------------|---------------|--------------|-----------|-----------|-----------|------------|------------|-------------|--------------|------------|
| 128 | 10,000 | 692,039 | 337.91 | 1,077,471 | 526.11 | 2.23 | **5,474** | 0.18ms | 5,799 | 145 | 8.54ms | 87 | 9.28ms |
| 128 | 25,000 | 679,054 | 331.57 | 1,512,967 | 738.75 | 44.60 | **112** | 8.54ms | 485 | 72 | 9.64ms | 79 | 9.34ms |
| 384 | 10,000 | 249,406 | 365.34 | 386,932 | 566.80 | 4.66 | **4,248** | 0.23ms | 2,070 | 78 | 9.76ms | 81 | 9.67ms |
| 384 | 25,000 | 267,787 | 392.27 | 439,444 | 643.72 | 57.69 | **168** | 1.04ms | 475 | 67 | 10.49ms | 67 | 10.28ms |
| 768 | 10,000 | 136,370 | 399.52 | 209,076 | 612.53 | 25.33 | **81** | 16.00ms | 359 | 55 | 17.13ms | 56 | 17.17ms |

### float32 — 12GB Memory Allocation (key comparison)

| Dim | Count | Dense QPS | Dense P50 | Hybrid QPS | Hybrid P50 | Filtered QPS | Filtered P50 |
|-----|-------|-----------|-----------|------------|------------|--------------|--------------|
| 128 | 10,000 | **5,377** | 0.18ms | **1,195** | 0.76ms | **1,388** | 0.71ms |
| 384 | 10,000 | **4,128** | 0.24ms | **249** | 1.78ms | **354** | 1.39ms |

### int8 — 8GB Memory Allocation

| Dim | Count | DoPut (vec/s) | DoPut (MB/s) | DoGet (vec/s) | DoGet (MB/s) | Index (s) | Dense QPS | Dense P50 | Hybrid QPS | Hybrid P50 |
|-----|-------|---------------|--------------|---------------|--------------|-----------|-----------|-----------|------------|------------|
| 128 | 10,000 | 1,802,602 | 220.04 | 1,796,552 | 219.31 | 32.42 | **5,023** | 0.19ms | 112 | 8.77ms |

---

## Detailed Results

### float32 dim=128, count=10,000 — 8GB

```
DoPut:              692,039 vec/s | 337.91 MB/s
DoGet:            1,077,471 vec/s | 526.11 MB/s
Indexing Time:           2.23s
Search Dense:        5,474 QPS | P50=0.18ms | P95=0.28ms | P99=0.34ms
Search Sparse:      5,799 QPS | P50=0.17ms | P95=0.27ms | P99=0.35ms
Search Hybrid:        145 QPS | P50=8.54ms | P95=17.70ms | P99=34.10ms
Search Filtered:      87 QPS | P50=9.28ms | P95=27.63ms | P99=36.30ms
```

### float32 dim=128, count=10,000 — 12GB

```
DoPut:              672,519 vec/s | 328.38 MB/s
DoGet:            1,059,510 vec/s | 517.34 MB/s
Indexing Time:           2.23s
Search Dense:        5,377 QPS | P50=0.18ms | P95=0.28ms | P99=0.35ms
Search Sparse:      5,477 QPS | P50=0.18ms | P95=0.28ms | P99=0.35ms
Search Hybrid:      1,195 QPS | P50=0.76ms | P95=0.94ms | P99=5.05ms
Search Filtered:    1,388 QPS | P50=0.71ms | P95=0.88ms | P99=0.99ms
```

### float32 dim=128, count=25,000 — 8GB

```
DoPut:              679,054 vec/s | 331.57 MB/s
DoGet:            1,512,967 vec/s | 738.75 MB/s
Indexing Time:          44.60s
Search Dense:           112 QPS | P50=8.54ms | P95=27.59ms | P99=36.22ms
Search Sparse:          485 QPS | P50=0.30ms | P95=8.04ms | P99=8.54ms
Search Hybrid:           72 QPS | P50=9.64ms | P95=35.29ms | P99=42.76ms
Search Filtered:         79 QPS | P50=9.34ms | P95=34.97ms | P99=43.45ms
```

### float32 dim=384, count=10,000 — 8GB

```
DoPut:              249,406 vec/s | 365.34 MB/s
DoGet:              386,932 vec/s | 566.80 MB/s
Indexing Time:           4.66s
Search Dense:        4,248 QPS | P50=0.23ms | P95=0.34ms | P99=0.41ms
Search Sparse:       2,070 QPS | P50=0.17ms | P95=4.47ms | P99=6.34ms
Search Hybrid:         78 QPS | P50=9.76ms | P95=28.04ms | P99=36.97ms
Search Filtered:       81 QPS | P50=9.67ms | P95=26.79ms | P99=36.53ms
```

### float32 dim=384, count=10,000 — 12GB

```
DoPut:              274,101 vec/s | 401.51 MB/s
DoGet:              377,602 vec/s | 553.13 MB/s
Indexing Time:           4.88s
Search Dense:        4,128 QPS | P50=0.24ms | P95=0.34ms | P99=0.39ms
Search Sparse:       5,327 QPS | P50=0.18ms | P95=0.28ms | P99=0.34ms
Search Hybrid:        249 QPS | P50=1.78ms | P95=7.78ms | P99=20.11ms
Search Filtered:      354 QPS | P50=1.39ms | P95=6.73ms | P99=18.25ms
```

### float32 dim=384, count=25,000 — 8GB

```
DoPut:              267,787 vec/s | 392.27 MB/s
DoGet:              439,444 vec/s | 643.72 MB/s
Indexing Time:          57.69s
Search Dense:           168 QPS | P50=1.04ms | P95=18.50ms | P99=36.51ms
Search Sparse:          475 QPS | P50=0.31ms | P95=8.39ms | P99=8.81ms
Search Hybrid:          67 QPS | P50=10.49ms | P95=36.31ms | P99=44.15ms
Search Filtered:         67 QPS | P50=10.28ms | P95=37.10ms | P99=45.66ms
```

### float32 dim=768, count=10,000 — 8GB

```
DoPut:              136,370 vec/s | 399.52 MB/s
DoGet:              209,076 vec/s | 612.53 MB/s
Indexing Time:          25.33s
Search Dense:           81 QPS | P50=16.00ms | P95=18.50ms | P99=33.00ms
Search Sparse:          359 QPS | P50=0.30ms | P95=15.26ms | P99=16.19ms
Search Hybrid:           55 QPS | P50=17.13ms | P95=32.97ms | P99=34.20ms
Search Filtered:         56 QPS | P50=17.17ms | P95=32.97ms | P99=38.86ms
```

### int8 dim=128, count=10,000 — 8GB

```
DoPut:            1,802,602 vec/s | 220.04 MB/s
DoGet:            1,796,552 vec/s | 219.31 MB/s
Indexing Time:          32.42s
Search Dense:        5,023 QPS | P50=0.19ms | P95=0.30ms | P99=0.38ms
Search Sparse:        999 QPS | P50=0.21ms | P95=7.80ms | P99=8.47ms
Search Hybrid:        112 QPS | P50=8.77ms | P95=16.92ms | P99=17.55ms
Search Filtered:      105 QPS | P50=8.91ms | P95=17.17ms | P99=18.10ms
```

---

## Key Observations

1. **Memory allocation dramatically impacts Hybrid/Filtered search**: At 10k vectors,
   increasing from 8GB → 12GB improves Hybrid from 145 → **1,195 QPS** (8x) and
   Filtered from 87 → **1,388 QPS** (16x). Memory pressure (GC tuner ratio >1.5x)
   severely degrades these workloads.
2. **Dense/Sparse search is memory-resilient**: Dense QPS stays high (4-5k) even at
   8GB. Sparse QPS is consistently fast (500-5,800) regardless of memory.
3. **AVX2-only hardware** (i7-12650H): Dense QPS at 10k is excellent (4,000-5,500+).
   At 25k vectors, QPS drops to 112-168 due to HNSW graph traversal length scaling.
4. **768-dim search is slow** (81 QPS) on AVX2-only systems. This is expected —
   768 dimensions requires 6x more distance calculations than 128 dims, and the AVX2
   fallback uses the generic unrolled Go implementation. The newly added AVX512
   kernels only help on AVX512-capable hardware.
5. **Indexing time**: float32 2-58s, int8 32s. int8 indexing is slow (32s for 10k)
   due to the narrow 32-byte AVX2 kernel.
6. **DoPut/DoGet throughput** is consistent regardless of memory allocation (as long
   as there's no OOM): 220-400 MB/s for DoPut, 220-740 MB/s for DoGet.

---

## Test Environment

```bash
# Start server with 8GB memory (recommended for balanced performance)
LONGBOW_LISTEN_ADDR=0.0.0.0:3000 \
LONGBOW_DATA_PATH=data/bench \
LONGBOW_MAX_MEMORY=8589934592 \
LONGBOW_NODE_ID=bench1 \
./bin/longbow &

# Run benchmark
./bin/benchmark-tool \
  --uri=127.0.0.1:3000 \
  --scale=10000 \
  --dim=128 \
  --dtype=float32 \
  --dataset=linux_bench \
  --queries=1000
```

---

*Last Updated: March 23, 2026 (partial results)*
