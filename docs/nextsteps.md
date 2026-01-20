# 10-Part Plan: Optimizing 384d Ingestion Throughput

**Goal**: Achieve > 800 MB/s ingestion throughput for all data types (especially Float32) at 384 dimensions.
**Current State**: Float32 @ 384d is ~608 MB/s. Int8 @ 384d is ~551 MB/s.

## 1. Deep Profiling & Instability Investigation

**Observation**: 384d ingestion throughput ranges from 115 MB/s to 435 MB/s for identical workloads.
**Action**: Capture profiles during "fast" vs "slow" periods.
**Goal**: Identify the intermittent blocker (likely GC pauses, WAL sync spikes, or Background Index Compaction).
**Method**: Continuous profiling during a 5-minute soak test.

## 2. Client-Side Batch Size Optimization

**Action**: Benchmark ingestion with varying client-side batch sizes (1k vs 10k).
**Observation**: Initial tests showed 1k batch > 10k batch (unexpected). This suggests large batches trigger the bottleneck (e.g. large allocation GC pressure).
**Goal**: Find the "Goldilocks" batch size that maximizes throughput without triggering stability issues.

## 3. Server-Side Batch Aggregation Tuning

**Action**: Tune `DoPut` internal buffer thresholds (`maxBatchBytes` / `maxBatchRows`) in `store_actions.go`.
**Goal**: Ensure server aggregates enough small batches before hitting the WAL/Index to amortize lock acquisition and I/O costs.

## 4. Ingestion Worker Scaling

**Action**: Increase `NumIngestionWorkers` for 384d datasets.
**Goal**: 384d vectors require more "compute per MB" than 1536d vectors (more headers, more indexing ops per byte). Increasing parallelism can utilize available CPU cores to process these smaller units faster.

## 5. Zero-Copy Pathway Audit

**Action**: Verify `Float32` and `Int` vectors at 384d follow the "Zero-Copy" ingestion path in `store_actions.go`.
**Goal**: Ensure `GraphData.SetVector` (or equivalent) is not allocating intermediate slices. Use `benchstat` to measure allocation rate.

## 6. SIMD Distance Optimization for 384d

**Action**: Analyze `DistCosine` / `DistEuclidean` performance for `dim=384`.
**Goal**: Check for alignment issues. 384 is divisible by 16 (AVX-512) and 8 (AVX2), but "tail" processing might be less efficient than for 128d or 1024d. Optimize the SIMD loop for this specific fixed dimension.

## 7. WAL Async Write Tuning

**Action**: Increase `persistenceQueue` depth for high-throughput small-vector scenarios.
**Goal**: PREVENT backpressure from the WAL writer. If WAL cannot keep up with > 100k writes/s (even if throughput < 800MB/s), it will block ingestion.

## 8. Memory Allocator & GC Tuning

**Action**: Analyze `mallocgc` pressure. Set `GOGC` aggressively or optimize vector slice pooling.
**Goal**: Small vectors generate high object churn. Reducing GC frequency or reusing buffers via `sync.Pool` for 384d vectors can yield significant gains.

## 9. Index Construction Parameter Tuning

**Action**: Experiment with lower `efConstruction` during bulk load, or parallelize `Insert` further.
**Goal**: HNSW construction is O(N log N). If construction is the bottleneck, temporarily relaxing graph quality (to be repaired later) or optimizing neighbor selection can boost speed.

## 10. Pre-allocation Strategy

**Action**: Ensure `GraphData` internal arrays are pre-allocated for the expected dataset size (e.g. 50k) to avoid resizing copying costs during the initial load.
**Goal**: Eliminate `grow` pauses during the benchmark.
