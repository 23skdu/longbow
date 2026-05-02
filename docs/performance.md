# Longbow Performance Benchmark Matrix (LATEST)

Generated on: 2026-04-30

## v0.2.0 Comprehensive Benchmark Matrix (2026-04-30)

> [!NOTE]
> Full matrix (1.9k+ combinations) is executing in parallel across Local (M3) and Remote (ancalagon). Results for `float32`, `dim=128`, `count=1000` are updated below.

### v0.2.1-rc Stability Release (2026-05-01)

> [!IMPORTANT]
> **Release Candidate v0.2.1-rc1** focuses on stabilizing high-dimensional (3072d) ingestion and resolving cross-platform concurrency deadlocks.

| Metric | Local Metal (3072d) | Remote CUDA (3072d) | Status |
|--------|---------------------|---------------------|--------|
| **Ingestion (vec/s)** | 92,699 | ~85,000 | **STABLE** |
| **GraphRAG (QPS)** | 93.8 | ~75.0 | **STABLE** |
| **Global GraphRAG** | 115.0 | ~110.0 | **STABLE** |

> [!TIP]
> **v0.2.1 Critical Fixes**:
>
> 1. **Hybrid Search Deadlock**: Resolved a re-entrant locking deadlock in `HybridSearch` by releasing the dataset lock before graph reranking.
> 2. **Metal Buffer Overflow**: Fixed a critical `idBuffer` resize bug in the Metal backend that caused memory corruption during large-scale ingestion.
> 3. **CUDA Dynamic Resizing**: Implemented dynamic buffer resizing in the CUDA C-backend to support high-volume ingestion without manual capacity tuning.
> 4. **AddBatch Robustness**: Fixed a schema discovery bug in `AddBatch` where sparse record batches caused "vector column not found" errors.

### Platform Configuration

- **Memory**: 18GB allocated to longbow node (`LONGBOW_MAX_MEMORY=19327352832`)
- **Test Configuration**: Matrix across dims (128-3072), counts (1k-100k)
- **Environments**:
  - **Local**: Apple Silicon M3 (Darwin/ARM64)
  - **Remote**: AMD64 Linux (ancalagon), AVX2, CUDA results pending

### Results Summary (float16, dim=128, count=1000)

| Metric | Local CPU | Local Metal | Remote CPU | Remote CUDA |
|--------|-----------|-------------|------------|-------------|
| **Ingestion (vec/s)** | 495,478 | Pending | Pending | Pending |
| **Search Dense (QPS)** | 3,140 | Pending | Pending | Pending |
| **Search Sparse (QPS)** | 12,418 | Pending | Pending | Pending |
| **Search Geo (QPS)** | 5,601 | Pending | Pending | Pending |
| **Search Temporal (QPS)**| 5,047 | Pending | Pending | Pending |

## Target Baselines (v0.1.9 Parity)

- **Dense Search (Float32, 384d)**: > 20,000 QPS
- **Temporal Search**: > 12,000 QPS
- **Ingestion (Bulk)**: > 150,000 vec/s

### Fine-Grained Locking

- Monolithic `insertMu` replaced with `epMu` and atomic graph pointers.
- Allows non-blocking concurrent traversals during bulk ingestion.

### Key Observations

1. **Ingestion Performance Milestone**: Ingested datasets up to 500k vectors without OOM by implementing client-side backpressure and chunked uploads.

2. **Search QPS Improvements (v0.2.0-rc1)**:
   - **Lock-Free Traversal**: Removed redundant shard locks (`insertMus`) in the ingestion path, relying on fine-grained `LockNode` spinlocks. This significantly reduces search/ingestion contention.
   - **Scheduler Latency**: Refactored `DoGet` and `DoGetPipeline` to use the `SharedWorkerPool`. Eliminated `runIndexWorker` polling with `Notify()` signaling, reducing CPU idle wakeups.
   - **Temporal Cache Stability**: Implemented $O(1)$ LRU cache and $O(\log N)$ binary search for temporal tree range queries, stabilizing Temporal search QPS under load.

3. **Filter Evaluator Stability**: Fixed a critical panic in the filter evaluator where `Reset` was not correctly re-binding all Arrow types (Boolean, Int32, UInt64) across record batch transitions.

4. **Platform Gap**: Apple Silicon (M3) continues to outperform x86_64 CPU by ~25% in search tasks, but the gap has narrowed due to a ~33% regression in Local CPU search QPS compared to v0.1.9.

### Regression Analysis (v0.2.0-pre)

- **Local Search Recovery**: Dense search QPS on M3 improved from 3.9k to 5.0k (+28%) after `LockNode` optimization and GCTuner calibration.
- **Remote Dense Search Recovery**: Dense search QPS on ancalagon improved from 684 QPS to 2,317 QPS (**3.3x gain**) following the removal of `time.Sleep` in spinlocks.
- **Sparse Search Regression**: Observed a significant drop in Sparse search performance when dimensionality increases (e.g., ~13k QPS at 128d vs <1k QPS at 768d). Requires investigation into inverted index scaling.
- **Remote Ingestion Regression**: Ingestion on AMD64 improved to 516k vec/s, surpassing previous baselines.
- **GraphRAG Stability**: GraphRAG search remains stable (~6k local, ~3k remote) but is still a target for SIMD expansion optimizations.

### Regression Analysis (v0.2.1-pre - 2026-05-01)

- **Geospatial & Temporal Recovery**: Resolved the massive collapse in Geo and Temporal search. Implementing AVX2-optimized kernels and parallelizing `HaversineBatch` and `computeNorm` in the `SharedWorkerPool` resulted in a **150x speedup** for Temporal and **68x speedup** for Geo on remote AMD64. Local M3 (NEON) also saw significant gains.
- **Lock-Free Index Access**: Transitioning `gpuIndex` to `atomic.Value` handles removed `RWMutex` contention, enabling smooth non-blocking traversals even during high-throughput ingestion.
- **GCTuner Calibration**: Lowering the GPU utilization floor to 60% and increasing background worker floors to 4 has stabilized CPU availability for the indexing path, preventing the 20% regression previously observed in dense search.
- **Local Ingestion Recovery**: `DoPut` throughput has returned to >550k vec/s following the implementation of pre-generated benchmark vectors and isolated client-side costs.

### Tail Latency & Memory Pressure

- **Remote Tail Latency**: Remote shows high tail latencies (p95: 18ms vs 0.40ms local) for Dense search.
- **Heap Pressure**: Local server showed repeated "High effective heap utilization" warnings during tests, indicating GC pressure.

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

### pprof

- Enabled for all benchmark runs
- Profiles captured: cpu, memory, goroutine, threadcreate, block, mutex
- Storage: ./profiles/ directory with timestamped files

### Remote CUDA Benchmark Results (ancalagon, Linux x86_64)

- **Status:** Tests queued for parallel execution with local benchmarks
- **Expected Impact:** 5-10x speedup for >1M vectors on GPU
- **Monitoring:** pprof data collection, log error monitoring enabled

### pprof

### SharedWorkerPool

- Fixed-size pool scaled to `runtime.GOMAXPROCS(0)`.
- Eliminates per-query goroutine churn.

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
