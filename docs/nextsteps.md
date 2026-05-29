# Longbow Next Steps

## P0: Buffer Eviction & VRAM Management

To handle 500k scale vectors without thrashing physical memory limits (VRAM/RAM), the following explicit memory paging architecture must be implemented:

- `[x]` **Subtask 1: Segmented Vector Arenas with Explicit LRU Paging**: Break monolithic vector buffers into fixed-size pages (e.g., 4MB/16MB). Wire these into the `GPUPager` (or equivalent CPU memory pager) so that the application—not the OS—controls what stays in RAM and what spills to disk using an explicit Least Recently Used (LRU) policy.
- `[x]` **Subtask 2: HNSW Graph Residency & Hot-Node Pinning**: The upper layers of the HNSW hierarchical graph are traversed on _every_ query. These top layers must be explicitly pinned in memory (never evicted). Only the massive bottom layer (Layer 0) graph connections and vector payloads should be eligible for eviction.
- `[x]` **Subtask 3: IO-Aware Batched Distance Computations**: Modify the search traversal to batch candidate neighbor distance evaluations. Fetching vectors from disk in bulk allows distance evaluations to happen concurrently, minimizing context switches.

## P1: Query Routing and Sub-Graph Expansion

- `[x]` Refine Metal argument buffers for GPU index dispatching.
- `[x]` Expand test coverage on integration with existing VectorStore implementations.

## P0: Regression Analysis & Stability Recommendations

- `[x]` **Subtask 1: Fix Zero-Length Slice Panics During Auto-Sharding Migration**: Resolved an issue where `ShardedHNSW` was instantiating underlying `ArrowHNSW` shards using uninitialized configurations (`Dims = 0`). This misconfiguration caused zero-length slices to be passed to the distance computation logic, resulting in rapid index out-of-bounds panics at scale (15k+ vectors). The `setDims` function now correctly mirrors dynamic vector dimensionality directly back into the core index configuration, ensuring that `Clone()` propagates safe array bounds to all migrated shards.
- `[x]` **Subtask 2: Fix Auto-Sharding Migration Aborts & Memory Leaks**: Resolved an issue where `AddBatch` errors due to missing vectors would abort the entire auto-sharding migration. It now logs a warning and skips missing vectors to ensure the migration completes. Also fixed a memory leak where `Retain()` on duplicate dataset records was not properly paired with `Release()`.
- `[ ]` **Subtask 3: Rebase & Dependabot resolution**: Cleanly handle incoming PRs to minimize downstream pipeline issues.
