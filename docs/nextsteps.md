# Longbow Next Steps - Release Candidate 0.2.1-rc4

This document tracks all outstanding items and performance/feature suggestions specifically prioritized for the current release candidate (`0.2.1-rc4`).

## Prioritized Action Items for 0.2.1-rc4

### Critical Priority (Security & Stability)
- **Monitor Arena offset truncation (>4GB)**: TemporalEntry arena is limited by design. Continuous monitoring of arena growth is required in production. (`temporal_search.go:347,367`)
- **Review `ivf_flat.go:347` vector map size bounds**: Add explicit checks if IVF-FLAT is expected to handle >4B vectors (`uint32(len(ivf.vectors))`).
- **Review `arrow_hnsw_persistence.go:208` version conversion**: Add bounds check if version numbers could exceed MaxInt64.

### High Priority (Performance Core)
1. **CUDA Optimization for Turboquant**: Optimize the CUDA kernels for `turboquant` dot product and L2 routines to align Nvidia performance with Apple Silicon Metal. Implement shared memory caching for lookup tables.
2. **NUMA-Aware Arena Allocator**: Update `internal/store/arena_pool.go` to support NUMA-node local allocations and pin search goroutines to specific CPU cores. This will prevent cross-socket latency on high-core AMD64 servers (e.g., `ancalagon`).
3. **Adaptive Batching for GPU Ingestion**: Enhance adaptive batching (`internal/store/adaptive_batching.go`) to query the active GPU backend for optimal block sizes, coalescing small inserts to perfectly align with GPU warp/thread block sizes.
4. **Query Result Merger Pre-allocation**: Update `internal/store/result_merger.go` to pre-calculate required capacity (`limit * shards`) and pre-allocate merge arrays from a `sync.Pool` to achieve zero-allocation result merging.

### Medium Priority (Scale & Reliability)
5. **Autoscaling Ingestion Workers**: Dynamically spawn or reap worker goroutines in `internal/store/ingestion_worker.go` based on the ingest vs. QPS ratio via a feedback loop, stabilizing throughput under varying loads.
6. **Disk-backed Learned Index Caching**: Implement a tiered LRU/LFU cache in RAM/NVMe specifically for learned index leaf nodes (`disk_backed_learned_index.go`) to eliminate latency hits on cold reads.
7. **Asynchronous Bitmap Pool Refill**: Add a background goroutine to proactively pre-allocate and refill the `bitmap_pool` before exhaustion, preventing severe P95 latency spikes during heavy parallel filtering.
8. **Distributed Vector Clock Compaction**: Implement a lock-free background thread in `internal/store/compaction.go` to aggressively merge vector clock deltas, guaranteeing bounded latency for high-frequency temporal searches.

### Low Priority (Future Proofing & Monitoring)
9. **GraphRAG Subgraph Prefetching**: Add an asynchronous prefetcher in `internal/store/graph_analytics.go` using `__builtin_prefetch` (or Go assembly equivalents) to load adjacent nodes into L2/L3 cache ahead of traversal.
10. **SIMD-Accelerated Binary Quantization**: Implement specialized AVX-512 population count (`VPOPCNTDQ`) kernels in `internal/simd/` for Hamming distance and wire them into `internal/store/binary_quantization.go`.
- **Monitor Vector ID truncation**: System designed for uint32 IDs; truncation occurs only at 4.29B vectors. (`temporal_search.go:935,1045,1094,1145`)
- **Monitor BatchIdx truncation**: BatchIdx is bounded by record count; unlikely to exceed uint32. (`sharded_hnsw.go:393,1025,1136`)
- **Monitor locationStore.Len() truncation**: Per-shard vector count unlikely to exceed 4.29B. (`sharded_hnsw.go:1392`)
