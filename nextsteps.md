# Optimization Roadmap

This document outlines the optimization plan for Longbow, prioritized by immediate stability/correctness and
performance targets.

## Immediate (Critical Fixes & Stability)

1. **[Stability] DoPut Connection Refused**: Debug and resolve "Connection refused" errors and server crashes
   during concurrent `DoPut` operations (Python client).
2. **[Interop] DoGet Serialization Error**: Fix "Header-type of flatbuffer-encoded Message is not RecordBatch" error
   in Python client (likely serialization mismatch).
3. **[Perf] Restore Throughput**:
    - **[DoGet]** Optimize `StatefulSerializer` to remove per-batch `RecordWriter` overhead (Target: >1.5 GB/s).
    - **[DoPut]** Optimize `IndexJobQueue` backpressure (remove spin-wait) and cache `DiskStore` checks
      (Target: >800 MB/s).
4. **[WAL] Reduce Flush Contention**: Implement ring-buffer batching to reduce lock contention in `WALBatcher.flushLoop`.
5. **[DoGet] Adaptive Chunking**: Implement dynamic chunk sizing (start small, grow to 64k) to handle both
   low-latency and high-throughput clients.
6. **[Perf] 128-dim Throughput**: Investigate and resolve specific throughput bottleneck for 128-dimensional vectors.

## Medium-Term (Architecture)

1. **[Store] Zero-Copy Flight**: Fully implement Arrow Flight's `DoExchange` for zero-copy ingestion
   (avoiding `DoPut` overhead).
2. **[Index] Parallel HNSW Build**: Shard HNSW construction across cores *within* a single dataset
   (currently serialized per dataset).
3. **[Query] Vector Quantization**: Enable Product Quantization (PQ) by default for >1M vector datasets to
   reduce memory bandwidth.
4. **[Storage] Parquet Backend**: Replace custom WAL/Snapshot format with Parquet for better interoperability
   and compression.
5. **[Network] gRPC Tuning**: Tune `KeepAlive` and window sizes for high-latency clients.

## Long-Term (Scalability)

1. **[Cluster] Raft Consensus**: Replace ad-hoc replication with a proper Raft consensus group for leader election
   and log replication.
2. **[Index] Disk-Based HNSW**: Implement DiskANN-style out-of-core indexing for datasets larger than RAM.
3. **[Search] Hybrid Re-ranking**: Optimize the re-ranking phase of hybrid search using late-interaction models (ColBERT).
4. **[Ops] Auto-Scaling**: Integrate with K8s HPA based on custom metrics (ingestion rate, query latency).
5. **[Observability] Distributed Tracing**: Add OpenTelemetry tracing to visualize bottlenecks across distributed nodes.
