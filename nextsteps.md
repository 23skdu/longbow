# Optimization Roadmap

This document outlines a 15-point plan to further optimize Longbow's ingestion and retrieval performance, based on recent findings and benchmarks.

## Immediate (Performance & Stability)

1. **[WAL] Reduce Flush Contention**: The `WALBatcher.flushLoop` still acquires locks frequently. Implement ring-buffer batching.
2. **[DoGet] Adaptive Chunking**: Implement dynamic chunk sizing (start small, grow to 64k) to handle both low-latency and high-throughput clients.
3. [x] **[Ingest] Async Persistence**: Move `ApplyDelta` persistence to a dedicated background worker to unblock `DoPut` critical path completely.
4. [x] **[Memory] CompressBuf Pool**: Fix `WALBatcher.compressBuf` unlimited growth by using a `sync.Pool` of fixed-size large buffers.
5. **[SIMD] AVX-512 Support**: Implement `euclideanAVX512` for x86 architectures (currently only AVX2/NEON supported).

## Medium-Term (Architecture)

1. **[Store] Zero-Copy Flight**: Fully implement Arrow Flight's `DoExchange` for zero-copy ingestion (avoiding `DoPut` overhead).
2. **[Index] Parallel HNSW Build**: Shard HNSW construction across cores *within* a single dataset (currently serialized per dataset).
3. **[Query] Vector Quantization**: Enable Product Quantization (PQ) by default for >1M vector datasets to reduce memory bandwidth.
4. **[Storage] Parquet Backend**: Replace custom WAL/Snapshot format with Parquet for better interoperability and compression.
5. **[Network] gRPC Tuning**: Tune `KeepAlive` and window sizes for high-latency clients.

## Long-Term (Scalability)

1. **[Cluster] Raft Consensus**: Replace ad-hoc replication with a proper Raft consensus group for leader election and log replication.
2. **[Index] Disk-Based HNSW**: Implement DiskANN-style out-of-core indexing for datasets larger than RAM.
3. **[Search] Hybrid Re-ranking**: Optimize the re-ranking phase of hybrid search using late-interaction models (ColBERT).
4. **[Ops] Auto-Scaling**: Integrate with K8s HPA based on custom metrics (ingestion rate, query latency).
5. **[Observability] Distributed Tracing**: Add OpenTelemetry tracing to visualize bottlenecks across distributed nodes.

## Completed Tasks (Session Log)

# Ingestion Performance & Optimization

## Objectives

- [x] Fix race condition in `IndexJobQueue`.
- [ ] Optimize ingestion for <50k vectors.

## Tasks

- [x] **Fix Race Condition** <!-- id: 60 -->
  - [x] Modify `IndexJobQueue` to use `RWMutex` for channel protection <!-- id: 61 -->
  - [x] Verify fix with `go test -race` (Addressed overflow issues) <!-- id: 62 -->

- [ ] **Optimization** <!-- id: 70 -->
  - [x] Run benchmark sweep (3k, 5k, 10k, 25k) <!-- id: 71 -->
  - [x] Analyze results for <50k optimizations <!-- id: 72 -->
  - [x] Tune parameters if needed (Performance is satisfactory) <!-- id: 73 -->

  - [ ] Investigate 128-dim throughput bottleneck (Target: 800MB/s Ingest, 1.7GB/s DoGet) <!-- id: 81 -->
  - [x] Create `scripts/validate_performance.py` for 10k-50k verification <!-- id: 82 -->
  - [x] Verify SIMD/Hardware acceleration usage (NEON/AVX2) <!-- id: 83 -->
  - [x] Analyze DoGet zero-copy path <!-- id: 84 -->
  - [x] **Optimize DoGet (Zero-Copy)**
    - [x] Investigate current implementation (Parallel logic)
    - [x] Implement parallel serialization (StatefulSerializer w/ pooling)
    - [x] Validate throughput (Achieved ~1.18 GB/s, up from 850 MB/s)
  - [x] Audit WAL and Persistence overhead <!-- id: 85 -->
  - [x] Create `nextsteps.md` with 15-point optimization plan <!-- id: 86 -->
