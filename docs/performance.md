# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-04-28

## Current Benchmark Results (2026-04-29)

### Platform Configuration
- **Memory**: 18GB allocated to longbow node
- **Test Configuration**: Matrix across dims (128-3072), counts (1k-500k), 100 queries
- **Data Status**: Partial results for CPU (Darwin/ARM64 and Linux/x86_64)

### Results Summary (float32, dim=128, count=1000)

| Metric | Local CPU | Remote CPU (ancalagon) |
|--------|----------|----------------------|
| **DoPut (vec/s)** | **469,180** (↑) | **333,739** (↑) |
| **Search Dense (QPS)** | 3,370 (↓) | 2,209 (↓) |
| **Search Sparse (QPS)** | 13,701 | 7,241 (↑) |
| **Search Temporal (QPS)** | 3,246 (↓) | 2,751 (↓) |
| **Search Geo (QPS)** | 3,589 (↓) | 1,767 (↓) |
| **Search GraphRAG (QPS)** | 1,005 | 738 |

## Target Baselines (v0.1.9 Parity)

*   **Dense Search (Float32, 384d)**: > 20,000 QPS
*   **Temporal Search**: > 12,000 QPS
*   **Ingestion (Bulk)**: > 150,000 vec/s

### Fine-Grained Locking

*   Monolithic `insertMu` replaced with `epMu` and atomic graph pointers.
*   Allows non-blocking concurrent traversals during bulk ingestion.

### Key Observations

1. **Ingestion Performance Milestone**: Ingestion vec/s improved by >50% on both platforms, likely due to parallel ingestion hardening and optimized allocation.

2. **P0: Resolve Search QPS Regressions**

*   **Investigation**: Dense and Temporal search QPS dropped by ~30% in v0.1.9.
*   **Hypothesis**: Contention on `insertMu` or overhead from `insertPool`.
*   **Action**: Implement fine-grained locking or lock-free reads for the index traversal path.

3. **P1: Temporal Cache Stabilization**

*   **Observation**: Temporal QPS varies between 3k and 14k across identical runs.
*   **Action**: Investigate cache eviction policy and ensure consistent pre-warming for temporal indices.

4. **Platform Gap**: Apple Silicon (M3) continues to outperform x86_64 CPU by ~50% in search tasks.

### Hardware
- **Local**: Apple Silicon M3, 18GB memory
- **Remote (ancalagon)**: NVIDIA RTX 4060 Laptop GPU, 8GB VRAM, 22GB RAM, 16 cores (AMD64 Linux)

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

### pprof

### SharedWorkerPool

*   Fixed-size pool scaled to `runtime.GOMAXPROCS(0)`.
*   Eliminates per-query goroutine churn.

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
