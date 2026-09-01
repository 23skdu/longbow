# Longbow Next Steps & Roadmap

## # Release 0.2.3-rc1 Completion & Release Status

All identified architectural blockers, reliability gaps, and security issues for **0.2.3-rc1** have been resolved, verified with `go vet ./...`, audited with `gosec ./...` (0 issues), and validated through unit test suites.

---

### Priority 0.1: Engine Durability & Memory Safety (Completed)

| # | Item | File & Line Range | Resolution |
|---|---|---|---|
| **0.1.1** | **Non-Blocking Snapshotting (Decouple Write Lock from Disk I/O)** | [`internal/storage/engine.go`](file:///home/rsd/REPOS/longbow/internal/storage/engine.go) | **Resolved:** Disk I/O and Parquet compression write to `tempDir` asynchronously before acquiring write lock only for the atomic swap and WAL reset. |
| **0.1.2** | **WAL Replay Goroutine Leak on Error** | [`internal/storage/wal_replay.go`](file:///home/rsd/REPOS/longbow/internal/storage/wal_replay.go) | **Resolved:** Introduced `context.Context` cancellation across reader, decoder, and reorder routines so all workers exit cleanly on early error or completion. |
| **0.1.3** | **Zstd Decoder Goroutine Leak in Replay** | [`internal/storage/wal_replay.go`](file:///home/rsd/REPOS/longbow/internal/storage/wal_replay.go) | **Resolved:** Explicitly calls `decoder.Close()` after decompression to prevent worker goroutine leaks. |
| **0.1.4** | **WAL IPC Decode Error Visibility & Metrics** | [`internal/storage/wal_replay.go`](file:///home/rsd/REPOS/longbow/internal/storage/wal_replay.go) | **Resolved:** Emits structured error logs and propagates decode errors upstream through channel error payloads. |
| **0.1.5** | **Implement `BackupManager.Restore` & `RestoreFromBackup`** | [`internal/store/backup.go`](file:///home/rsd/REPOS/longbow/internal/store/backup.go) | **Resolved:** Retains backup payload in `BackupManager`, verifies checksum integrity in `Restore`, and reconstructs dataset tables with Arrow IPC reader in `RestoreFromBackup`. |
| **0.1.6** | **Metadata Buffer Pool Recycling** | [`internal/store/store.go`](file:///home/rsd/REPOS/longbow/internal/store/store.go) | **Resolved:** Implemented `putPooledMetadataBuffer(buf []byte)` to recycle metadata byte slices back to `vs.metadataPool`. |

---

### Priority 0.2: Index Concurrency & Graph Correctness (Completed)

| # | Item | File & Line Range | Resolution |
|---|---|---|---|
| **0.2.1** | **`IVFFlat.AddByRecord` ID Collision Race Condition** | [`internal/store/index/ivf_flat.go`](file:///home/rsd/REPOS/longbow/internal/store/index/ivf_flat.go) | **Resolved:** Added monotonic `nextID uint64` inside single critical section `addLocked` to eliminate vector ID collisions. |
| **0.2.2** | **`HNSWPluggableAdapter.Search` Graph Traversal** | [`internal/store/index/pluggable_index_adapters.go`](file:///home/rsd/REPOS/longbow/internal/store/index/pluggable_index_adapters.go) | **Resolved:** Connected adapter to underlying `h.hnsw.SearchVectors` graph search when initialized. |
| **0.2.3** | **IVF-HNSW Composite Delta Sync Vector Fetching** | [`internal/store/index/ivf_hnsw_composite.go`](file:///home/rsd/REPOS/longbow/internal/store/index/ivf_hnsw_composite.go) | **Resolved:** Implemented `SetDataProvider` and `fetchVector` resolving Arrow vectors from backing dataset batch chunks. |
| **0.2.4** | **Index State & Delta Sync Stubs Completion** | [`internal/store/index/pluggable_index_adapters.go`](file:///home/rsd/REPOS/longbow/internal/store/index/pluggable_index_adapters.go), [`adaptive_index.go`](file:///home/rsd/REPOS/longbow/internal/store/index/adaptive_index.go), [`ivf_pq_index.go`](file:///home/rsd/REPOS/longbow/internal/store/index/ivf_pq_index.go) | **Resolved:** Implemented gob-based `ExportState`, `ImportState`, `ExportDelta`, and `ApplyDelta` across `BruteForceIndex`, `IVFPQIndex`, and `PluggableInternalAdapter`. |

---

### Priority 0.3: Ingestion Latency & GC Stability (Completed)

| # | Item | File & Line Range | Resolution |
|---|---|---|---|
| **0.3.1** | **Eliminate Inline Stop-The-World GC in `DoPut`** | [`internal/store/store_actions.go`](file:///home/rsd/REPOS/longbow/internal/store/store_actions.go) | **Resolved:** Removed synchronous `runtime.GC()`, `debug.FreeOSMemory()`, and `100ms` sleep; admission errors return backpressure status directly to clients. |

---

### Priority 0.4: Search Engine & Index Efficiency (Completed)

| # | Item | File & Line Range | Resolution |
|---|---|---|---|
| **0.4.1** | **Deduplicate BM25 Indexing in `indexTextColumns`** | [`internal/store/store_hybrid.go`](file:///home/rsd/REPOS/longbow/internal/store/store_hybrid.go) | **Resolved:** Prioritizes `BM25ArenaIndex` and falls back to `BM25Index` without double-indexing. |
| **0.4.2** | **`HybridSearchWithBitmap` BM25 Fallback** | [`internal/store/hybrid_search.go`](file:///home/rsd/REPOS/longbow/internal/store/hybrid_search.go) | **Resolved:** Added fallback to `ds.BM25Index.SearchBM25` when `ds.BM25ArenaIndex` is nil. |
| **0.4.3** | **Wire CDC Subscriber Registration** | [`internal/store/store.go`](file:///home/rsd/REPOS/longbow/internal/store/store.go) | **Resolved:** Implemented `RegisterCDCSubscriber` and `UnregisterCDCSubscriber` using store-level `cdcSubscribers` dispatcher map. |

---

## # Roadmap Initiatives for v0.3.0

The following long-term benchmarking and multi-node scaling items are scheduled for v0.3.0:

| # | Initiative | Description |
|---|---|---|
| 1 | **TurboQuant2 50k Recall@10 Validation** | Comprehensive 50k Recall@10 validation tests with adaptive bit-depth and widened search parameters. |
| 2 | **500k Scale Benchmark Suite Execution** | Complete benchmark runs for remaining vector types (`int64`, `uint64`, `complex64`, `complex128`, `turboquant`) using `--use-disk` memory budgeting. |
| 3 | **Multi-Node RDMA Integration Validation** | Live cluster verification of RoCE/InfiniBand Flight RDMA transport. |

---

## # Completed & Verified Milestones Archive

1. **`GetSchema` Implementation** (`internal/store/store_query.go:157-173`)
2. **`WriteToWAL` Uninitialized Guard** (`internal/storage/engine.go:113-115`)
3. **Snapshot Fsync Durability** (`internal/storage/engine.go:353, 397, 449`)
4. **Atomic Snapshot Directory Swap** (`internal/storage/engine.go:238-257`)
5. **Bounded WAL Replay Buffer** (`internal/storage/wal_replay.go:382, 432`)
6. **`StdWAL.Sync()` & Descriptor Reuse** (`internal/storage/wal_interface.go:23-110`)
7. **LockFreeHNSW Layer-0 BFS Search** (`internal/store/index/hnsw_lockfree.go:120-192`)
8. **DiskANN Deterministic Start Node** (`internal/store/index/diskann.go:289-297`)
9. **DiskANN Neighbor Pruning** (`internal/store/index/diskann.go:253-270`)
10. **WebSocket Server Shutdown** (`cmd/longbow/main.go:887-893`)
11. **MQ Exporter Shutdown** (`cmd/longbow/main.go:896-902`)
12. **RDMA Server Listener Execution** (`cmd/longbow/main.go:867-876`)
13. **Snapshot Error Aggregation** (`internal/storage/engine.go:500, 525, 532, 549`)
14. **Snapshot Partial Load Failure Detection** (`internal/storage/engine.go:548-550`)
15. **`AsyncFsyncer` Counter Tracking** (`internal/storage/async_fsync.go:271-274`)
16. **`VerifyBackup` & `Restore` Thread Safety** (`internal/store/backup.go:175-370`)
17. **DiskANN Full Buffer Reads** (`internal/store/index/diskann.go:540, 561, 588`)
18. **SIMD Details Type Assertion Safety** (`cmd/longbow/main.go:350-354`)
19. **Listener Type Assertion Safety** (`cmd/longbow/main.go:801-804, 821-824`)
20. **DiskANN Resource Cleanup** (`internal/store/index/diskann.go:609-617`)
21. **IVFFlat Resource Cleanup & AddByRecord Race Prevention** (`internal/store/index/ivf_flat.go:37-160`)
22. **`GetTombstones` Defensive Copy** (`internal/store/dataset.go:343-351`)
23. **`ArrowHNSW.Close()` DiskGraph Teardown** (`internal/store/index/arrow_hnsw_persistence.go:368-373`)
24. **`ArrowHNSW.Close()` RepairAgent Teardown** (`internal/store/index/arrow_hnsw_persistence.go:359-362`)
25. **`worker_pool` Panic Recovery** (`internal/store/index/worker_pool.go:148-158`)

