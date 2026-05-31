# Longbow Next Steps

## P0: Unindexed Search Mode Complexity & GraphRAG Optimization

The current `graphrag`, `temporal`, and `geo_spatial` search implementations suffer from exponential $O(N^3)$ computational scaling at massive vector bounds (> 400k) due to falling back on brute-force multi-hop traversals rather than utilizing the core indexing engine. To prevent multi-hour timeouts and OOM crashes, the following plan must be executed to collapse query latency:

- `[x]` **Subtask 1: HNSW Index Passthrough**: Refactored `temporal`, `geo_spatial`, and `graphrag` search execution to natively query the HNSW index via `TemporalPredicate`, `SlidingWindowPredicate`, and `GeoPredicate`. Per-hop neighbor lookup now runs at $O(\log N)$ instead of $O(N)$ linear scan. Back-pointer `ds *Dataset` wired into both `TemporalIndex` and `GeoIndex`.
- `[x]` **Subtask 2: GraphRAG Beam Search**: Replaced unconditional BFS in `RankWithGraph` and `RankWithGraphDistributed` with a Beam Search that prunes the BFS frontier to the top `BeamWidth=100` nodes (by decayed similarity score) after each hop. Worst-case complexity collapses from $O(N^3)$ to $O(B^2 \cdot \text{depth})$ where $B=100$. `Traverse` also migrated.
- `[x]` **Subtask 3: Explicit Edge Materialization (Adjacency Lists)**: Added `adjList [][]Edge` and `bwdAdjList [][]Edge` to `GraphStore`, populated atomically in `AddEdge` and `FromArrowBatch` under `adjMu` write lock. `RankWithGraph`, `RankWithGraphDistributed`, and `Traverse` now index directly into these slices under a single `adjMu.RLock()`, replacing per-edge `LockFreeMap.Get()` calls with $O(1)$ pointer dereferences.

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

## P0: Remote CUDA/Metal Benchmarks

GPU benchmarks (Metal on local, CUDA on ancalagon) have not been run for this release. Historical baselines (v0.2.0-rc2) are invalid due to the QPS aggregation bug. Corrected QPS must be established from the current benchmark run.

## P1: Full Benchmark Matrix

The complete benchmark matrix (16 types × 5 dims × 7 counts × 4 platforms × 5+ search modes) is currently running for CPU on both hosts. Metal and CUDA runs are scheduled after CPU completes.

## P1: Review All Integer Distance Kernels

The `int64` accumulator pattern was only present in int16/uint16 kernels but other integer types (int32, uint32, int64, uint64) should be benchmarked at count=5000+ to verify they don't exhibit similar regression patterns. All integer distance kernels should use `float64` accumulators as the standard pattern on ARM64.

## P1: Correct Performance Baselines in docs/performance.md

All QPS targets were inflated ~5x by the bug. The `performance.md` header now flags this, and targets have been reset to estimated corrected values. Once the benchmark run completes, update with actual numbers from result JSON files.

## P1: Full Benchmark Matrix

The complete benchmark matrix (14 types × 5 dims × 7 counts × 4 platforms) should be run to validate across all configurations. The int16/uint16 fix demonstrated that kernel-level changes can produce 32x latency improvements; the full matrix will reveal if other types have similar optimization opportunities.

## P1: Review All Integer Distance Kernels

The `int64` accumulator pattern was only present in int16/uint16 kernels but other integer types (int32, uint32, int64, uint64) should be benchmarked at count=5000+ to verify they don't exhibit similar regression patterns. All integer distance kernels should use `float64` accumulators as the standard pattern on ARM64.
