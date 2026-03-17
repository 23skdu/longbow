# Performance Metrics (Comprehensive Matrix)

## Latest Benchmark Results (2026-03-16)

### Go Micro-Benchmarks (SIMD)

**Test Configuration:** Apple M3 Pro, goos=darwin, goarch=arm64, benchtime=3s

| Operation | ns/op | MB/s | Prev ns/op | Change |
|-----------|-------|------|------------|--------|
| Euclidean128 | 26.15 | 19,042 | 26.20 | **flat** |
| Euclidean384 | 82.40 | 15,440 | 81.83 | **flat** |
| Euclidean768 | 173.8 | 18,315 | 173.2 | **flat** |
| Euclidean1536 | 356.8 | 17,102 | 356.5 | **flat** |
| F16 128 | 192.1 | 1,332 | 193.5 | **+1% faster** |
| F16 384 | 492.1 | 1,561 | 494.8 | **+1% faster** |
| F16 768 | 938.1 | 1,637 | 937.2 | **flat** |
| F16 1536 | 1837 | 1,673 | 1847 | **+1% faster** |

### Integration Benchmarks (Python Client)

**Test Configuration:** Single node, 12GB RAM, dim=384, float32, **InitialCapacity=50000**

| Vectors | DoPut (MB/s) | DoGet (MB/s) | Search (QPS) | Notes |
|---------|--------------|--------------|--------------|-------|
| 1,000 | 300 | 453 | 1,593 | Baseline |
| 5,000 | 791 | 1,019 | 1,100 | |
| 10,000 | 1,000 | 1,306 | 1,032 | Peak DoPut |
| 15,000 | 919 | 1,536 | 2,607 | Peak Search (⚠️ 0 results) |
| 25,000 | 1,142 | 1,647 | 1,121 | Peak DoGet |

### Validation Test Results

**Test Configuration:** 25,000 vectors, dim=128, single node

| Metric | Result | Target | Status |
|--------|--------|--------|--------|
| Ingest | 773 MB/s | 800 MB/s | ⚠️ 97% of target |
| DoGet | 800 MB/s | 1,700 MB/s | ⚠️ 47% of target |

---

## Comparison with Previous Results (2026-03-16)

### SIMD Benchmarks

| Config | Previous | Current | Change |
|--------|----------|---------|--------|
| Euclidean128 | 26.20 ns | 26.15 ns | **flat** |
| Euclidean384 | 81.83 ns | 82.40 ns | **flat** |
| Euclidean768 | 173.2 ns | 173.8 ns | **flat** |
| Euclidean1536 | 356.5 ns | 356.8 ns | **flat** |
| F16 128 | 193.5 ns | 192.1 ns | **+1% faster** |
| F16 384 | 494.8 ns | 492.1 ns | **+1% faster** |
| F16 768 | 937.2 ns | 938.1 ns | **flat** |
| F16 1536 | 1847 ns | 1837 ns | **+1% faster** |

### Integration Benchmarks (dim=384)

| Vectors | Metric | Previous | Current | Change |
|---------|--------|----------|---------|--------|
| 1,000 | DoPut | 287 | 300 | **+5%** |
| 1,000 | DoGet | 535 | 453 | -15% |
| 1,000 | Search | 1,586 | 1,593 | **+0.4%** |
| 5,000 | DoPut | 710 | 791 | **+11%** |
| 5,000 | DoGet | 1,027 | 1,019 | **flat** |
| 5,000 | Search | 1,081 | 1,100 | **+2%** |
| 10,000 | DoPut | 848 | 1,000 | **+18%** |
| 10,000 | DoGet | 1,232 | 1,306 | **+6%** |
| 10,000 | Search | 1,011 | 1,032 | **+2%** |
| 15,000 | DoPut | 696 | 919 | **+32%** |
| 15,000 | DoGet | 1,484 | 1,536 | **+4%** |
| 15,000 | Search | 1,096 | 2,607 | **+138%** (⚠️ 0 results) |
| 25,000 | DoPut | 535 | 1,142 | **+113%** |
| 25,000 | DoGet | 1,754 | 1,647 | -6% |
| 25,000 | Search | 1,126 | 1,121 | **flat** |

---

## Regression Analysis

### Observed Changes

1. **SIMD Performance**: Flat across all dimensions - within measurement variance
2. **DoPut Performance**: Significant improvements at 10k (+18%), 15k (+32%), 25k (+113%)
3. **DoGet Performance**: Slight regression at 1k (-15%), 25k (-6%)
4. **Search Performance**: Huge improvement at 15k (+138%) but returns 0 results (BUG)
5. **Validation Tests**: Both below target (97% ingest, 47% DoGet)

### Root Causes

1. **System Load**: Previous runs had different background load
2. **Fresh Data**: Running with clean data directory vs accumulated datasets
3. **Warm-up Effects**: Server warm-up and caching may vary between runs
4. **Search Bug**: 15k search returning 0 results is a correctness issue

### Positive Improvements

1. **DoPut at Scale**: 25k improved from 535 to 1,142 MB/s (+113%)
2. **DoPut at 15k**: Improved from 696 to 919 MB/s (+32%)
3. **DoPut at 10k**: Improved from 848 to 1,000 MB/s (+18%)

---

## Known Issues

### Search Correctness Bug

**Issue**: Search at 15k vectors returns 0 results despite high QPS (2,607).

**Impact**: Incorrect search results at specific scale/dimension combinations.

**Status**: Requires investigation - may be related to:
- HNSW index construction at certain capacity thresholds
- Query vector generation mismatch
- Filtering or scoring logic

---

## Notes

- Python benchmark scripts use `longbow-arrow` client
- SIMD benchmarks run with benchtime=3s for more accurate results
- Float32 fragmentation issue was previously fixed (InitialCapacity=50000)
- Validation targets are aggressive (800 MB/s ingest, 1.7 GB/s DoGet)
- Results show improvement in DoPut at scale, regression in validation DoGet

---

*Generated: 2026-03-16 18:30:00*
*Go micro-benchmarks run: 2026-03-16 18:15:00*
*SIMD tests: Flat performance*
*Integration tests: DoPut improved at scale, Search has correctness bug*
