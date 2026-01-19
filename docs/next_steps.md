# Next Steps: Performance & Stability Roadmap

**Date**: 2026-01-18
**Context**: Post-release stabilization and scaling for High-Dimensional Embeddings (OpenAI 1536d/3072d).

## Phase 1: High-Dimensional Optimization (Immediate)

1. **High-Dim SIMD Tuning**:
    * **Action**: Optimize AVX512/NEON kernels for 1536/3072d vectors (loop unrolling, cache prefetching) to handle strided access efficiently.
2. **SQ8 Recall Validation**:
    * **Action**: Verify recall/loss of SQ8 vs Float32 on normalized high-dim vectors (1536d) to ensure accuracy is within acceptable limits.
3. **Parallel Index Building**:
    * **Action**: Move graph linking (HNSW insert) to async workers to unblock the main ingestion loop and utilize all cores.
4. **Ingestion Buffer Pooling**:
    * **Action**: Implement `sync.Pool` for SQ8 encoding buffers and scratch space to reduce GC pressure during high-throughput ingestion.
5. **JIT/PGO Optimization**:
    * **Action**: Enable Profile Guided Optimization (PGO) for the hot distance calculation paths to allow the Go compiler to inline and optimize based on real workloads.

6. **Investigate 3072d Float32 Degradation**:
    * **Finding**: `float32` @ 3072d drops to 15 QPS (vs 220 QPS for `float16`).
    * **Action**: Profile L2/L3 cache misses. Implement blocked/tiled distance calculation for dimensions > 1024 to maximize L2/L3 cache usage.
7. **Optimize Chunk Allocation (`ensureChunk`)**:
    * **Finding**: Pprof shows `ensureChunk` / `AllocSliceDirty` consuming 65% of heap allocs during high-dim ingest.
    * **Action**: Implement a slab allocator or recycle chunks to reduce GC pressure and allocation overhead.
8. **Ingestion Batching Alignment**:
    * High-dim vectors create large allocation spikes.
    * **Action**: Implement adaptive batch sizing in `DoPut` based on payload size (not just row count) to keep allocs < 10MB per batch.
9. **Memory Pooling for Search**:
    * Query vectors for 3072d require significant scratch space.
    * **Action**: Expand `ArrowSearchContext` pool to handle variable-sized scratch buffers efficiently, preventing GC churn.
10. **Quantization (SQ8) Verification**:
    * Scaling to high dimensions requires compression.
    * **Action**: Validate and tune SQ8 quantization specifically for 1536d distribution (e.g. OpenAI embeddings are normalized, check loss).

## Phase 2: System Stability (Short Term)

1. **Prefetching Tuning**:
    * Hardware prefetchers struggle with strided access in large vectors.
    * **Action**: Tune explicit software prefetch distance in `GetVector` for >1024 dims.
2. **OOM Protection / Backpressure V2**:
    * Observe high memory usage during 3072d ingest.
    * **Action**: Implement "Soft Limit" backpressure that slows ingest *before* hard implementation limit involves flow control.
3. **WAL Compression**:
    * WAL usage scales linearly with dimensions, causing IO bottlenecks.
    * **Action**: Enable logic to compress WAL entries (Snappy/Zstd) for vectors > 1024d.
4. **Tombstone Compaction Policy**:
    * Deletes in high-dim indices leave large holes.
    * **Action**: Implement aggressive background compaction for high-churn high-dim indices.
5. **Startup Time Optimization**:
    * Loading 3072d indices takes 10x longer.
    * **Action**: Parallelize index loading (mmap warm-up) across cores.

## Phase 3: Advanced Scaling (Medium Term)

1. **Panic Recovery & Isolation**:
    * Ensure a single shard panic (e.g. malformed vector) doesn't crash the node.
    * **Action**: Wrap shard operations in recovery middleware.
2. **Product Quantization (PQ)**:
    * SQ8 is good, PQ is better for memory.
    * **Action**: Prioritize PQ implementation for 1536/3072d to allow holding 10M+ vectors on standard hardware.
3. **Disk-ANN / SSD Offloading**:
    * RAM is the limit for 3072d.
    * **Action**: Implement SSD-based vector storage with in-memory graph (Vamana/DiskANN style).
4. **Dynamic Dimension Reduction (Matryoshka)**:
    * OpenAI models support shortening vectors.
    * **Action**: Add native support for slicing vectors during distance calc to support Matryoshka Embedding Learning (MRL).
5. **Continuous Profiling Dashboard**:
    * Observability is key.
    * **Action**: Integrate `pprof` endpoints into a persistent Grafana/Prometheus dashboard for long-term trend analysis.
