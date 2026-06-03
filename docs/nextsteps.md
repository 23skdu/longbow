# Observations & Next Steps

Based on the recent benchmark tests run on 500k vectors at dimension 384 using disk-based storage with `io_uring`:

## Observations

1. **Ingest Performance is Bottlenecked by Disk I/O:** Both `float32` and `turboquant8` ingested at approximately ~51.3k vectors/sec across CPU and CUDA modes. The disk usage remained identical (732.4 MB), suggesting that the `io_uring` disk-writing path is the primary bottleneck during ingestion, negating any computational advantage CUDA or quantization might provide.
2. **CUDA Provides Marginal Search Improvements:** CUDA queries showed small improvements in dense/hybrid search QPS over CPU modes. However, for features like Geo and Temporal search, the CUDA modes occasionally showed slightly worse performance. The data transfer overhead between host and device might be overshadowing the compute speedup for this dataset size (500k).
3. **TurboQuant Overhead:** `turboquant8` performed slightly slower than `float32` on both QPS and latency across most modes. Since the disk usage didn't shrink (suggesting the disk storage didn't properly compact the quantized vectors or is storing them identically), the runtime decompression/decoding overhead of TurboQuant results in a net negative for performance.
4. **Filtered and Geo Searches Have High P99 Latency:** Filtered search hit ~340ms P99 latencies, and Geo search hovered around ~300ms P99 latencies. Sparse search is phenomenally fast (~2ms P99).

## Recommendations & Advanced Temporal Indexing Plan

1. **TurboQuant Storage Engine Fixed:** The disk-storage layer was modified to serialize the compressed `turboquant8` byte stream directly to `io_uring` and `mmap` rather than re-inflating vectors. This provides the expected I/O and memory bandwidth benefits on ingestion and disk footprint.
2. **Geo Search Optimization Addressed:** The `HybridSearch` implementation has been updated to use the Quadtree index for *pre-filtering* candidates into an `AllowedSet` bitmap instead of brute-force calculating Haversine distance during HNSW graph traversal.
3. **CUDA Kernel Dispatching:** Memory pinning (`mlock`) was added to the host-to-device mmap allocator to prevent paging during CUDA transfers, improving the host-device bandwidth bottleneck.
4. **TurboQuant Flakiness Resolved:** Fixed a bug where a hardcoded float subtraction (`val -= 0.1`) was corrupting residual calculations during decoding. QJL corrections now correctly apply the calculated residual adjustments, restoring Cosine Similarity test accuracy.

### Advanced Temporal Indexing Strategy (Planned)

To handle massive-scale temporal/time-series vector workloads (where users frequently query ranges like `last 30 days` or specific date bounds), we need to replace the naive metadata filtering approach.

**Proposed Architecture:**
- **Time-Bucketed HNSW Shards:** Partition the HNSW index chronologically (e.g., daily or hourly shards). Querying a specific time window simply queries the specific shards bounded by the time range.
- **Segment Trees / Interval Trees:** For datasets requiring sub-shard accuracy, integrate a high-performance Segment Tree in Go that stores `(StartTime, EndTime) -> vector_id` pairs.
- **Pre-Filtering Bitsets:** Similar to the updated Geo index, querying the Segment Tree will yield an ultra-fast RoaringBitmap of allowed `vector_ids`, which is then passed as an `AllowedSet` into the HNSW algorithm.
- **LSM-Tree for Hot Data:** Maintain an active memory-mapped LSM-tree (Log-Structured Merge-tree) for the most recent temporal vectors (the "Hot" set), as they receive the most writes and queries. Once a time bucket expires, merge it into immutable disk-backed shards.
5. **Async IO Uring WAL Stability Resolved:** Fixed critical race conditions in the `io_uring` WAL storage backend by eliminating concurrent CQE polling from synchronous writes and preventing empty buffer flushing, restoring stability during high-concurrency.
