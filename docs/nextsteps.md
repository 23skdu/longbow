# Recommended Next Steps (Updated 2026-08-29)

Based on the 50k-vector regression benchmark (dim 128+384, all 17 dtypes, 13 search modes, 500 queries, 16 GB limit).

---

## Priority 1: Investigate turboquant2 Scaling Regression

**Problem**: turboquant2 (2-bit) shows severe QPS drop at 50k scale (588 QPS dim=128) vs 10k (3,244 QPS). This 5.5x drop is much larger than other types (~2x). Likely an HNSW recall issue with 2-bit quantization at larger graph sizes.

**Investigate**:
- Check recall@10 for turboquant2 at 10k vs 50k
- Verify tq_bits parameter is correctly passed at scale
- Consider adaptive bit-depth for turboquant based on graph size

## Priority 2: Complete Benchmark at 500k Scale

**Problem**: Only 11/17 types completed at 500k scale. Larger types (complex64, complex128) hit ResourceExhausted under 16 GB.

**Strategy**: Run in batches:
| Batch | Dtypes | Est. Time | Memory |
|-------|--------|-----------|--------|
| Small | float32, float16, int8, uint8, turboquant2 | ~30 min | fits 16 GB |
| Medium | float64, int16, uint16, int32, uint32, turboquant4 | ~1 h | fits 16 GB |
| Large | int64, uint64, complex64, complex128, turboquant, turboquant8 | ~2 h | needs 24+ GB or --use-disk |

---

## Completed (2026-08-29)

| Item | Status |
|------|--------|
| 50k-scale benchmark across all 17 dtypes | ✅ 67/68 configs |
| All 13 search modes verified | ✅ Non-zero QPS on all modes |
| 16 GB memory limit validation | ✅ No OOM at 10k/50k scale |
| Integer type SIMD regression identified | ✅ Fixed — int16/32/64, uint16/32/64 dispatched via AVX2 |
| Benchmark auto-resume and retry | ✅ `--resume` + `--max-retries` + checkpoint persistence |
| Memory estimation pre-flight check | ✅ `--estimate-memory` flag with per-config RSS estimation |
| Server startup reliability | ✅ SO_REUSEADDR, exponential backoff, port randomization, fast-exit |
| Complete result matrix generated | ✅ `perf_matrix_cpu_regression_20260617_172900.json` |
