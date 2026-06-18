# Recommended Next Steps (Updated 2026-06-17)

Based on the 50k-vector regression benchmark (dim 128+384, all 17 dtypes, 13 search modes, 500 queries, 16 GB limit).

---

## Priority 1: Fix Integer SIMD Dispatch for int16/32/64 and uint16/32/64

**Problem**: Search QPS for integer types (int16, int32, int64, uint16, uint32, uint64) is 2-10x lower than same-byte-width float types. For example:
- int32 dim=128 (4 bytes): 871 QPS vs float32 (4 bytes): 3,089 QPS
- uint16 dim=128 (2 bytes): 947 QPS vs float16 (2 bytes): 3,325 QPS
- int64 dim=128 (8 bytes): 1,667 QPS vs float64 (8 bytes): 3,292 QPS

**Root cause**: Integer distance computation likely falls through to scalar Go code instead of SIMD-accelerated paths. The dispatch table in `internal/vector/dispatch.go` may not have entries for integer L2/cosine/ip kernels.

**Fix**: Add SIMD dispatch entries for integer types:
- `cosineInt16AVX2` / `l2SquaredInt16AVX2` for int16/uint16
- `cosineInt32AVX2` / `l2SquaredInt32AVX2` for int32/uint32
- `cosineInt64AVX2` / `l2SquaredInt64AVX2` for int64/uint64
- Wire all into `dispatch.go` with proper type detection

**Expected gain**: 3-5x improvement for integer search QPS.

## Priority 2: Benchmark Infrastructure — Auto-Resume

**Problem**: The benchmark script does not resume after crashes or port failures. The int32 dim=128 count=10000 config was skipped due to a transient port issue with no retry mechanism.

**Fix**:
- Add `--retry-failed` flag to re-attempt failed/skipped configs
- Add exponential backoff for server startup port contention
- Track port usage to avoid TIME_WAIT collisions

## Priority 3: Investigate turboquant2 Scaling Regression

**Problem**: turboquant2 (2-bit) shows severe QPS drop at 50k scale (588 QPS dim=128) vs 10k (3,244 QPS). This 5.5x drop is much larger than other types (~2x). Likely an HNSW recall issue with 2-bit quantization at larger graph sizes.

**Investigate**:
- Check recall@10 for turboquant2 at 10k vs 50k
- Verify tq_bits parameter is correctly passed at scale
- Consider adaptive bit-depth for turboquant based on graph size

## Priority 4: Add Memory Estimation Pre-Flight Check

**Problem**: The 16 GB memory limit was adequate for all types at 10k/50k scale, but the previous 500k run showed complex64/complex128 OOM. A pre-flight estimation would prevent wasted time.

**Implementation**:
- Add `--estimate-memory` flag to unified_benchmark.py
- Calculate estimated peak RSS per (dtype, dim, count) tuple
- Skip configs that would exceed the memory limit

## Priority 5: Complete Benchmark at 500k Scale

**Problem**: Only 11/17 types completed at 500k scale. Larger types (complex64, complex128) hit ResourceExhausted under 16 GB.

**Strategy**: Run in batches:
| Batch | Dtypes | Est. Time | Memory |
|-------|--------|-----------|--------|
| Small | float32, float16, int8, uint8, turboquant2 | ~30 min | fits 16 GB |
| Medium | float64, int16, uint16, int32, uint32, turboquant4 | ~1 h | fits 16 GB |
| Large | int64, uint64, complex64, complex128, turboquant, turboquant8 | ~2 h | needs 24+ GB or --use-disk |

## Priority 6: Server Startup Reliability

**Problem**: 1/68 configs failed due to server startup timeout (port 3100). The script retries for 120s but eventually gives up.

**Fix**:
- Add socket SO_REUSEADDR/TIMEWAIT handling in Go server
- Add retry with port randomization in the benchmark script
- Reduce startup timeout or add more retry attempts

---

## Completed in This Run (2026-06-17)

| Item | Status |
|------|--------|
| 50k-scale benchmark across all 17 dtypes | ✅ 67/68 configs |
| All 13 search modes verified | ✅ Non-zero QPS on all modes |
| 16 GB memory limit validation | ✅ No OOM at 10k/50k scale |
| Integer type SIMD regression identified | 🔴 int16/32/64, uint16/32/64 affected |
| Complete result matrix generated | ✅ `perf_matrix_cpu_regression_20260617_172900.json` |
