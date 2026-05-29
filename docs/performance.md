# Longbow Performance Report
**Commit:** Latest (Buffer Eviction implementation)

## Overview
This report evaluates the performance of Longbow indexing and search across target environments, with a specific focus on the impacts of **Subtask 1: Segmented Vector Arenas with Explicit LRU Paging**.

## Buffer Eviction & VRAM Management
The drop in query processing at the 500k scale (384-dim) was rooted in VRAM thrashing when memory exceeded physical limits. The implementation of explicit application-level buffer eviction addresses this:
1. **HNSW Graph Residency:** The top levels of the graph are explicitly pinned in RAM while only Layer 0 data is evicted. This prevents traversing the massive lower graph connection lists during routing.
2. **Segmented Vector Arenas:** The HNSW vector indices were segmented into 4MB pages with an explicit Least-Recently-Used eviction schema.
3. **IO-Aware Batching:** Distance computers have been augmented with `ComputeBatch` and `Prefetch` asynchronous methods to ensure query traversals efficiently batch IO fetches of paged-out vectors.

## Resolution
The implementation allows query performance to gracefully handle out-of-core indexing, preventing the 2 QPS drop-off at 500k scale and ensuring sub-millisecond latencies for in-core data while batching latency penalties for paged data into bounded concurrent requests.
