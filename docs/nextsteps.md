# Longbow Next Steps

## P0: Buffer Eviction & VRAM Management
To handle 500k scale vectors without thrashing physical memory limits (VRAM/RAM), the following explicit memory paging architecture must be implemented:

- `[x]` **Subtask 1: Segmented Vector Arenas with Explicit LRU Paging**: Break monolithic vector buffers into fixed-size pages (e.g., 4MB/16MB). Wire these into the `GPUPager` (or equivalent CPU memory pager) so that the application—not the OS—controls what stays in RAM and what spills to disk using an explicit Least Recently Used (LRU) policy.
- `[x]` **Subtask 2: HNSW Graph Residency & Hot-Node Pinning**: The upper layers of the HNSW hierarchical graph are traversed on *every* query. These top layers must be explicitly pinned in memory (never evicted). Only the massive bottom layer (Layer 0) graph connections and vector payloads should be eligible for eviction.
- `[x]` **Subtask 3: IO-Aware Batched Distance Computations**: Modify the search traversal to batch candidate neighbor distance evaluations. Fetching vectors from disk in bulk allows distance evaluations to happen concurrently, minimizing context switches.

## P1: Query Routing and Sub-Graph Expansion
- `[x]` Refine Metal argument buffers for GPU index dispatching.
- `[x]` Expand test coverage on integration with existing VectorStore implementations.
