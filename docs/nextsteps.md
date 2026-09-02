# Longbow Next Steps & Roadmap

## Release 0.2.3-rc1 Status & Verification

All identified P0 blockers, architectural race conditions, memory leaks, and persistence recovery hazards have been remediated, verified with `go vet ./...`, audited with `gosec` (0 issues), and validated under Go's race detector (`go test -race`).

---

### P0 Blockers Remediation Matrix (Resolved)

| # | Item | Subsystem | File & Lines | Resolution & Verification |
|---|---|---|---|---|
| **P0.1** | **Snapshot-WAL Recovery Inversion Data Loss** | Persistence / Recovery | [`internal/store/store_persistence.go`](file:///home/rsd/REPOS/longbow/internal/store/store_persistence.go) | **Resolved:** Reordered `InitPersistence` to load baseline snapshots (`engine.LoadSnapshots`) before replaying incremental WAL deltas (`engine.ReplayWAL`), ensuring consistent record ordering and index location mapping. |
| **P0.2** | **`IVFHNSWCompositeIndex.ApplyDelta` Non-Reentrant Deadlock** | Index / Concurrency | [`internal/store/index/ivf_hnsw_composite.go`](file:///home/rsd/REPOS/longbow/internal/store/index/ivf_hnsw_composite.go) | **Resolved:** Extracted `addLocked` and `fetchVectorLocked`; `ApplyDelta` calls them directly while holding `idx.mu.Lock()`, eliminating non-reentrant mutex self-deadlock. Verified with unit tests. |
| **P0.3** | **Arrow RecordBatch Memory Leak in `DoPut` Slicing** | Memory / Allocator | [`internal/store/store_actions.go`](file:///home/rsd/REPOS/longbow/internal/store/store_actions.go) | **Resolved:** Removed redundant `subRec.Retain()` in `DoPut` slicing branch, and added deferred cleanup in `flush()` to guarantee all batches in `batch` are released even on error paths. |
| **P0.4** | **`IVFHNSWCompositeIndex.AddBatch` Vector ID Collision & Data Race** | Index / Data Integrity | [`internal/store/index/ivf_hnsw_composite.go`](file:///home/rsd/REPOS/longbow/internal/store/index/ivf_hnsw_composite.go) | **Resolved:** Protected ID generation and batch insertion under `idx.mu.Lock()` with `addLocked`. Verified with concurrent multi-goroutine `-race` test without duplicate IDs. |
| **P0.5** | **TurboQuant Recursive Reconstruction Workspace Buffer Aliasing** | Quantization / Accuracy | [`internal/store/index/turboquant.go`](file:///home/rsd/REPOS/longbow/internal/store/index/turboquant.go) | **Resolved:** Expanded workspace buffer to $3 \times 2^{\lceil \log_2 d \rceil}$ and isolated work, recon, and depth-partitioned recursion stack slices ($e.pow2 - n$) to prevent parent/child buffer corruption. |
| **P0.6** | **Retained RecordBatch Leak on Snapshot Serialization Failure** | Persistence / Memory | [`internal/store/store_persistence_methods.go`](file:///home/rsd/REPOS/longbow/internal/store/store_persistence_methods.go) | **Resolved:** Added explicit `item.Cleanup()` invocations on early error paths (`SnapshotGraph`, `ExportGraph`, and `yield(item)` failures) in `storeSnapshotSource.Iterate`. |
| **P0.7** | **Peer Search Unchecked Type Assertions & Panic Crash Risk** | Distributed / Flight | [`internal/store/global_search.go`](file:///home/rsd/REPOS/longbow/internal/store/global_search.go) | **Resolved:** Added `extractSearchResultsFromRecord` with safe type switches for `Uint64`, `Uint32`, `Int64`, `Float32`, `Float64` and added `recover()` handlers in all peer query goroutines. |
| **P0.8** | **TurboQuant Non-Standard Bit-Depth Silent 0-Distance Failure** | SIMD / Distance | [`internal/simd/turboquant.go`](file:///home/rsd/REPOS/longbow/internal/simd/turboquant.go) | **Resolved:** Implemented bit-level angle unpacking and polar reconstruction in `TurboQuantDistanceGeneric` for arbitrary bit depths, plus parameter validation in `NewTurboQuantEncoder`. |

---

## Roadmap Initiatives for v0.3.0

The following long-term benchmarking and multi-node scaling items are scheduled for v0.3.0:

| # | Initiative | Description |
|---|---|---|
| 1 | **TurboQuant2 50k Recall@10 Validation** | Comprehensive 50k Recall@10 validation tests with adaptive bit-depth and widened search parameters. |
| 2 | **500k Scale Benchmark Suite Execution** | Complete benchmark runs for remaining vector types (`int64`, `uint64`, `complex64`, `complex128`, `turboquant`) using `--use-disk` memory budgeting. |
| 3 | **Multi-Node RDMA Integration Validation** | Live cluster verification of RoCE/InfiniBand Flight RDMA transport. |
