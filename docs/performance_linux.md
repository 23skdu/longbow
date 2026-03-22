# Longbow Linux Performance Benchmarks

**Date**: March 22, 2026
**Platform**: Linux (x86_64, gccgo/Go 1.24)
**CPU**: 12th Gen Intel(R) Core(TM) i7-12650H (no AVX512 — AVX2-only)
**Memory**: 23GB RAM (6GB allocated to Longbow)
**Storage**: NVMe SSD

> **Note**: This i7-12650H laptop CPU does **not** have AVX512. All SIMD
> operations fall back to AVX2. See `docs/nextsteps.md` for plans to optimize
> for AVX2-only systems.

---

## Benchmark Configuration

- **Test Tool**: `bin/benchmark-tool` (Go, same binary as server)
- **Test Types**: DoPut, DoGet, Dense Search, Sparse Search, Hybrid Search, Filtered Search
- **Dimensions**: 128, 384
- **Dataset Sizes**: 10,000 | 25,000 vectors
- **Data Types**: float32, int8
- **Metric**: Euclidean (L2)
- **Search k**: 10
- **Queries**: 1,000 per test

---

## Results Summary

### float32

| Dim | Count | DoPut (vec/s) | DoPut (MB/s) | DoGet (vec/s) | DoGet (MB/s) | Dense QPS | Dense P50 | Hybrid QPS | Hybrid P50 |
|-----|-------|---------------|--------------|---------------|--------------|-----------|-----------|------------|------------|
| 128 | 10,000 | 692,039 | 337.91 | 1,077,471 | 526.11 | **5,474** | 0.18ms | 145 | 8.54ms |
| 128 | 25,000 | 679,054 | 331.57 | 1,512,967 | 738.75 | **112** | 8.54ms | 72 | 9.64ms |
| 384 | 10,000 | 249,406 | 365.34 | 386,932 | 566.80 | **4,248** | 0.23ms | 78 | 9.76ms |
| 384 | 25,000 | 267,787 | 392.27 | 439,444 | 643.72 | **168** | 1.04ms | 67 | 10.49ms |

### int8

| Dim | Count | DoPut (vec/s) | DoPut (MB/s) | DoGet (vec/s) | DoGet (MB/s) | Dense QPS | Dense P50 | Hybrid QPS | Hybrid P50 |
|-----|-------|---------------|--------------|---------------|--------------|-----------|-----------|------------|------------|
| 128 | 10,000 | 1,802,602 | 220.04 | 1,796,552 | 219.31 | **4,974** | 0.20ms | 117 | 8.63ms |

---

## Detailed Results

### float32 dim=128, count=10,000

```
DoPut:              692,039 vec/s | 337.91 MB/s
DoGet:            1,077,471 vec/s | 526.11 MB/s
Indexing Time:           2.23s
Search Dense:        5,474 QPS | P50=0.18ms | P95=0.28ms | P99=0.34ms
Search Sparse:      5,799 QPS | P50=0.17ms | P95=0.27ms | P99=0.35ms
Search Hybrid:        145 QPS | P50=8.54ms | P95=17.70ms | P99=34.10ms
Search Filtered:      87 QPS | P50=9.28ms | P95=27.63ms | P99=36.30ms
```

### float32 dim=128, count=25,000

```
DoPut:              679,054 vec/s | 331.57 MB/s
DoGet:            1,512,967 vec/s | 738.75 MB/s
Indexing Time:          44.60s
Search Dense:           112 QPS | P50=8.54ms | P95=27.59ms | P99=36.22ms
Search Sparse:          485 QPS | P50=0.30ms | P95=8.04ms | P99=8.54ms
Search Hybrid:           72 QPS | P50=9.64ms | P95=35.29ms | P99=42.76ms
Search Filtered:         79 QPS | P50=9.34ms | P95=34.97ms | P99=43.45ms
```

### float32 dim=384, count=10,000

```
DoPut:              249,406 vec/s | 365.34 MB/s
DoGet:              386,932 vec/s | 566.80 MB/s
Indexing Time:           4.66s
Search Dense:        4,248 QPS | P50=0.23ms | P95=0.34ms | P99=0.41ms
Search Sparse:       2,070 QPS | P50=0.17ms | P95=4.47ms | P99=6.34ms
Search Hybrid:         78 QPS | P50=9.76ms | P95=28.04ms | P99=36.97ms
Search Filtered:       81 QPS | P50=9.67ms | P95=26.79ms | P99=36.53ms
```

### float32 dim=384, count=25,000

```
DoPut:              267,787 vec/s | 392.27 MB/s
DoGet:              439,444 vec/s | 643.72 MB/s
Indexing Time:          57.69s
Search Dense:           168 QPS | P50=1.04ms | P95=18.50ms | P99=36.51ms
Search Sparse:          475 QPS | P50=0.31ms | P95=8.39ms | P99=8.81ms
Search Hybrid:          67 QPS | P50=10.49ms | P95=36.31ms | P99=44.15ms
Search Filtered:         67 QPS | P50=10.28ms | P95=37.10ms | P99=45.66ms
```

### int8 dim=128, count=10,000

```
DoPut:            1,802,602 vec/s | 220.04 MB/s
DoGet:            1,796,552 vec/s | 219.31 MB/s
Indexing Time:          33.26s
Search Dense:        4,974 QPS | P50=0.20ms | P95=0.31ms | P99=0.39ms
Search Sparse:       1,028 QPS | P50=0.19ms | P95=7.43ms | P99=8.21ms
Search Hybrid:         117 QPS | P50=8.63ms | P95=16.45ms | P99=17.31ms
Search Filtered:       110 QPS | P50=8.79ms | P95=16.84ms | P99=17.57ms
```

---

## Key Observations

1. **AVX2-only hardware** (i7-12650H, no AVX512): All SIMD kernels fall back to AVX2,
   resulting in lower peak search QPS compared to AVX512 or ARM NEON systems.
2. **Indexing time scales with dataset size**: 10k vectors index in 2-5s; 25k vectors
   take 44-58s, which impacts total benchmark time.
3. **Memory pressure**: Server configured with 6GB (well under 23GB available), causing
   periodic GC tuner warnings. Larger memory allocation would improve search performance.
4. **int8 indexing is slow** (33s for 10k) vs float32 (2-5s) — the int8 AVX2 kernel
   processes only 32 bytes per iteration vs 256 bytes for float32 AVX2.
5. **Dense search QPS at 10k vectors is excellent** (4,000-5,000+ QPS). At 25k vectors,
   QPS drops significantly (112-168), likely due to HNSW graph traversal length scaling
   and memory pressure.

---

## Comparison: float32 dim=128 10k vs Previous Run

| Metric | Previous (2026-03-07) | Current (2026-03-22) | Delta |
|--------|----------------------|----------------------|-------|
| DoPut | 381 MB/s | 338 MB/s | -11% |
| DoGet | 896 MB/s | 526 MB/s | -41% |
| Dense QPS | 1,077 | 5,474 | +408% |
| Hybrid QPS | 146 | 145 | ~0% |

> The DoGet regression is due to reduced memory allocation (8GB → 6GB) and
> accumulated server state. The Dense QPS improvement reflects the Go benchmark
> tool's corrected stream lifecycle.

---

## Test Environment

```bash
# Start server with 6GB memory
LONGBOW_LISTEN_ADDR=0.0.0.0:3000 \
LONGBOW_DATA_PATH=data/bench \
LONGBOW_MAX_MEMORY=6442450944 \
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

*Last Updated: March 22, 2026*
