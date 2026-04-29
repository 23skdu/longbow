# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-04-28

## Current Benchmark Results (2026-04-28)

### Platform Configuration
- **Memory**: 18GB allocated to longbow
- **Test Configuration**: dim=128, count=500, 500 queries

### Results Summary

| Metric | Local CPU | Local Metal | Remote CPU (ancalagon) | Remote CUDA (ancalagon) |
|--------|----------|-------------|----------------------|------------------------|
| **DoPut (vec/s)** | 281,491 | 231,839 | 227,294 | 241,875 |
| **DoGet (vec/s)** | 438,212 | 501,567 | 231,851 | 400,580 |
| **Search Dense (QPS)** | 4,868 | 4,966 | 2,685 | 2,747 |
| **Search Sparse (QPS)** | 13,825 | 13,912 | 6,356 | 6,522 |
| **Search ByID (QPS)** | 6,279 | 6,292 | 2,933 | 3,359 |
| **Search Temporal (QPS)** | 5,803 | 5,779 | 3,753 | **4,192** |
| **Search Geo (QPS)** | 6,115 | 6,160 | 2,520 | 2,882 |
| **Search GraphRAG (QPS)** | 1,464 | 1,498 | 869 | 939 |
| **Search Recommend (QPS)** | 5,728 | 5,800 | 2,710 | 3,130 |
| **p50 Dense (ms)** | 0.19 | 0.19 | 0.35 | 0.35 |

### Key Observations
1. **Temporal search now working**: Fixed timestamp type assertion, now returning 3,700-5,800 QPS
2. **Local (Metal) shows best performance**: 4,966 dense QPS (vs 2,747 CUDA)
3. **Sparse remains fastest**: 13,825 QPS local CPU
4. **Ancalagon (RTX 4060) lower QPS**: Due to x86 CPU vs Apple Silicon, but CUDA shows improvement in some ops

### Hardware
- **Local**: Apple Silicon M3, 18GB memory
- **Remote (ancalagon)**: NVIDIA RTX 4060 Laptop GPU, 8GB VRAM, 22GB RAM, 16 cores

## v0.1.9 Baseline (2026-04-26)

### Benchmark Matrix Coverage
- **Platforms:** CPU, Metal (local), CUDA (remote ancalagon)
- **Data Types:** float16, float32, float64, int8, int16, int32, int64, uint8, uint16, uint32, uint64, complex64, complex128, turboquant2, turboquant4, turboquant8
- **Dimensions:** 128, 384, 768, 1024, 3072
- **Counts:** 500, 1000, 5000, 15000, 50000, 100000
- **Search Types (via alpha-values):** dense (alpha=1.0), hybrid (alpha=0.5), graph (alpha=0.0)
- **Search Modes:** dense, hybrid, sparse, filtered, byid, graphrag, geo, temporal, learned_index
- **Memory Allocation:** 18GB for longbow testing

### Ingest Performance (vec/s) - CPU, 10K vectors, dim=128

| Platform | Mode | float32 | float64 | int8 |
|---------|------|--------|--------|-----|
| Darwin arm64 | CPU | 1,219,915 | ~800K | ~900K |

### Search Performance (QPS) - CPU, 10K vectors, dim=128

| Mode | QPS | p50 ms | p95 ms | p99 ms |
|------|-----|--------|--------|--------|
| Dense | 3,947 | 0.23 | 0.38 | 0.57 |
| Hybrid | 3,929 | 0.23 | 0.42 | 0.59 |
| Sparse | 4,015 | 0.22 | 0.40 | 0.57 |
| Filtered | 3,937 | 0.23 | 0.32 | 0.63 |
| ByID | 3,900 | 0.23 | 0.41 | 0.58 |

### Remote CUDA Benchmark Results (ancalagon, Linux x86_64)
- **Status:** Tests queued for parallel execution with local benchmarks
- **Expected Impact:** 5-10x speedup for >1M vectors on GPU
- **Monitoring:** pprof data collection, log error monitoring enabled

### pprof Data Collection
- Enabled for all benchmark runs
- Profiles captured: cpu, memory, goroutine, threadcreate, block, mutex
- Storage: ./profiles/ directory with timestamped files

### Log Monitoring
- All benchmark runs monitored for errors
- Log level: DEBUG for detailed tracing
- Error patterns tracked and reported

## v0.1.8 Baseline (2026-04-17)

### Ingest Performance (vec/s)

| | (500, 128) | (500, 384) | (500, 768) | (500, 1024) | (500, 3072) | (1000, 128) |
|:-------------------------------------|-------------:|-------------:|-------------:|--------------:|--------------:|--------------:|
| ('Darwin arm64', 'cpu', 'float16') | 351,494 | 265,440 | 141,123 | 186,704 | 107,989 | nan |
| ('Darwin arm64', 'cpu', 'float32') | 275,036 | 175,558 | 141,304 | 100,721 | 63,231.4 | nan |
| ('Darwin arm64', 'cpu', 'float64') | 233,375 | 170,177 | 114,736 | 88,528.9 | 36,024.6 | nan |
| ('Darwin arm64', 'cpu', 'int8') | 345,383 | 323,136 | 265,657 | 224,010 | 175,096 | nan |
| ('Darwin arm64', 'metal', 'float16') | 386,125 | 275,111 | 206,065 | 177,791 | 119,318 | nan |
| ('Darwin arm64', 'metal', 'float32') | 211,532 | 195,523 | 155,660 | 135,535 | 63,251.4 | nan |
| ('Darwin arm64', 'metal', 'float64') | 247,842 | 149,661 | 86,032.6 | 91,986.4 | 37,726.1 | nan |
| ('Darwin arm64', 'metal', 'int8') | 364,221 | 279,681 | 254,415 | 243,719 | 243,719 | 176,336 |
| ('Linux x86_64', 'cpu', 'float32') | 78,500 | 134,738 | nan | nan | nan | 357,775 |
| ('Linux x86_64', 'cpu', 'float64') | 169,560 | 82,564.2 | nan | nan | nan | nan |

(End of file)
