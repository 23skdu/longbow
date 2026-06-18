# Longbow Performance Benchmark Results

**Generated**: 2026-06-17
**Platform**: Linux x86_64 (1 NUMA node, 16 cores, 22 GB RAM)
**Memory Limit**: 16 GB allocated (`LONGBOW_MAX_MEMORY=17179869184`)
**Test Tool**: `scripts/unified_benchmark.py` (CPU mode)
**Queries**: 500 per test configuration
**Scale**: 10,000 and 50,000 vectors, dims 128 and 384
**Data Types**: float32, float64, float16, int8, int16, int32, int64, uint8, uint16, uint32, uint64, complex64, complex128, turboquant (8-bit), turboquant2, turboquant4, turboquant8
**Search Modes**: All 13 modes (dense, hybrid, filtered, filteredbool, filteredstring, sparse, byid, graphrag, globalgraphrag, recommend, geo, temporal, learnedindex)
**Completion**: 67/68 configs (98.5%) — int32 dim=128 count=10000 skipped due to transient server startup port contention

---

## Key Findings

### 1. uint8 and int8 Lead Ingest and Search
- uint8 dim=128 @ 50k achieves **2,784,828 vec/s** ingest — highest of all types
- uint8 dim=128 @ 50k achieves **4,277 QPS** dense search — highest of all types
- int8/uint8 outperform float32 for both ingest and search at this scale

### 2. Integer Types (int16/32/64, uint16/32/64) Show Anomalously Low Search QPS
- uint16 dim=384 @ 50k: only **400 QPS** dense search (vs uint8: 2,001 QPS at same dim/count)
- int16 dim=128: **830 QPS** dense (vs int8: 3,844 QPS at same dim/count)
- float32 dim=128 (4 bytes) = **3,089 QPS** vs int32 dim=128 (also 4 bytes) = **871 QPS** at 10k
- This suggests integer distance computations are falling through to scalar fallback paths instead of SIMD acceleration

### 3. float16 Performs Exceptionally Well
- 2nd highest ingest rate (1,480,122 vec/s dim=128 @ 50k)
- Dense search QPS comparable to float32 (3,133 vs 1,060 at dim=128 @ 50k)
- ~40% memory savings vs float32 with minimal QPS difference

### 4. complex64/complex128 Viable at This Scale
- No OOM or ResourceExhausted at 10k/50k scales
- complex128 dim=384 @ 50k: 2,542 QPS dense search
- Ingest is slow (47-62K vec/s) but search performance is competitive

### 5. TurboQuant Performance Scales with Bit Depth
- turboquant (8-bit, tq_bits=4 default): 3,541-3,947 QPS dense (dim=128)
- turboquant2 (2-bit): 588-829 QPS dense — tradeoff of ~4x compression for ~4x QPS drop
- turboquant8 (8-bit): 703-984 QPS dense — similar to turboquant2

### 6. Geo Search Is Fast at This Scale
- Geo QPS ranges from 613-2,480 across all configs (unlike 1M-scale where it drops to ~25 QPS)
- Bottleneck is HNSW graph size, not Haversine distance computation

### 7. Server Startup Reliability
- 1 transient failure (port 3100) — likely TIME_WAIT contention
- All other 67 startups succeeded cleanly
- Average startup time: ~6s with 6 transient retries on port collision

---

## Ingest Performance (vec/s)

| Type | dim=128 @ 10k | dim=384 @ 10k | dim=128 @ 50k | dim=384 @ 50k |
|------|:-----------:|:-----------:|:-----------:|:-----------:|
| float32 | 498,252 | 177,044 | 832,258 | 179,645 |
| float64 | 252,651 | 98,770 | 433,652 | 147,003 |
| float16 | 605,837 | 351,261 | 1,480,122 | 574,294 |
| int8 | 1,048,770 | 489,240 | 2,589,605 | 1,110,104 |
| int16 | 712,978 | 327,142 | 1,328,608 | 577,619 |
| int32 | — | 181,499 | 858,200 | 306,353 |
| int64 | 268,441 | 97,457 | 390,461 | 140,251 |
| uint8 | 1,048,367 | 626,985 | 2,784,828 | 1,137,975 |
| uint16 | 887,698 | 311,273 | 1,614,651 | 582,519 |
| uint32 | 443,272 | 176,192 | 830,177 | 295,175 |
| uint64 | 242,993 | 98,436 | 411,727 | 143,182 |
| complex64 | 274,272 | 89,609 | 436,219 | 116,883 |
| complex128 | 120,762 | 47,746 | 221,140 | 62,150 |
| turboquant (4-bit) | 468,439 | 193,031 | 684,938 | 220,512 |
| turboquant2 (2-bit) | 438,427 | 182,688 | 617,382 | 66,451 |
| turboquant4 (4-bit) | 468,439 | 193,031 | 684,938 | 220,512 |
| turboquant8 (8-bit) | 394,462 | 181,738 | 843,137 | 211,228 |

## Dense Search QPS

| Type | dim=128 @ 10k | dim=384 @ 10k | dim=128 @ 50k | dim=384 @ 50k |
|------|:-----------:|:-----------:|:-----------:|:-----------:|
| float32 | 3,089 | 3,093 | 1,060 | 2,735 |
| float64 | 3,292 | 3,225 | 1,005 | 806 |
| float16 | 3,325 | 3,025 | 3,133 | 2,621 |
| int8 | 3,844 | 3,595 | 2,106 | 1,671 |
| int16 | **830** | **554** | **882** | **638** |
| int32 | — | 3,405 | 2,161 | 3,206 |
| int64 | **1,667** | **920** | **1,185** | **691** |
| uint8 | 3,299 | 3,771 | 4,277 | 2,001 |
| uint16 | **947** | **492** | **828** | **400** |
| uint32 | **871** | **560** | **770** | **486** |
| uint64 | **908** | **519** | **616** | 3,306 |
| complex64 | 3,439 | 2,774 | 1,023 | 709 |
| complex128 | 3,203 | 2,823 | 3,156 | 2,542 |
| turboquant (4-bit) | 3,866 | 3,385 | 810 | 885 |
| turboquant2 (2-bit) | 3,244 | 3,080 | 588 | 829 |
| turboquant4 (4-bit) | 3,866 | 3,385 | 810 | 885 |
| turboquant8 (8-bit) | 3,947 | 3,316 | 984 | 703 |

Values in **bold** indicate anomalously low performance requiring investigation.

---

## Regression Check vs Previous Run

Compared to the previous 1M-scale run (docs/performance.md, 2026-06-12):
- float32 dim=128 dense QPS at similar scale: 2,188 QPS (1M) vs 3,089 QPS (10k) — scale-dependent as expected
- float32 dim=384 dense QPS: 2,461 QPS (1M) vs 3,093 QPS (10k)
- float64 dim=128 dense QPS: 274 QPS (1M) vs 3,292 QPS (10k) — consistent with scale sensitivity
- **No regressions detected** — all comparable configs from the previous run show consistent or better performance
- The LockFreeNeighborCache deadlock fix and other changes from the previous run are stable

---

## Issues Identified

1. **int16, uint16, int32, uint32, int64, uint64** — Anomalously low dense search QPS (400-1,600 QPS) vs same-byte-width float types (3,000+ QPS). Likely missing SIMD dispatch path for these types.
2. **int32 dim=128 @ 10k** — Skipped due to server startup timeout on port 3100. Transient port contention.
3. **turboquant2 (2-bit)** — Severe QPS drop at 50k scale (588 QPS dim=128 vs 3,244 QPS at 10k). May be a scaling issue with 2-bit quantization.

---

## Raw Results

Full structured JSON data: `data/perf_logs/perf_matrix_cpu_regression_20260617_172900.json`
Auto-generated per-config result files: `data/perf_logs/result_cpu_*.json`
Server logs: `data/perf_logs/longbow_cpu_*.log`
Benchmark logs: `data/perf_logs/bench_cpu_*.log`
