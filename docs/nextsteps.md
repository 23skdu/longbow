# P0 Blockers for 0.2.3 Release

## Critical (data loss / crash / silent corruption)

| # | Item | File:Line | Impact |
|---|---|---|---|
| 1 | `BackupManager.Restore` is a no-op | `internal/store/backup.go:222-236` | Backup restoration completely broken — validates then returns nil |
| 2 | `GetSchema` returns `nil, nil` | `internal/store/store_query.go:157-159` | gRPC nil-pointer crash when any client calls GetSchema |
| 3 | `WriteToWAL` silently returns nil when WAL uninitialized | `internal/storage/engine.go:112-114` | Data loss — caller believes data is persisted |
| 4 | Snapshot file writes lack fsync | `internal/storage/engine.go:307-440` | Crash between f.Close() and Rename leaves corrupt snapshots |
| 5 | Snapshot directory swap is non-atomic | `internal/storage/engine.go:239` | RemoveAll + Rename window leaves no snapshot on crash |
| 6 | WAL replay reorder buffer unbounded | `internal/storage/wal_replay.go:377-449` | Corrupted WAL with gaps OOMs the process |
| 7 | WAL IPC decode failures silently dropped | `internal/storage/wal_replay.go:334-367` | Corrupted entries vanish without any error/metric |
| 8 | `StdWAL.Sync()` is a no-op | `internal/storage/wal_interface.go:92-95` | Callers believe data is synced — durability guarantee is false |
| 9 | `StdWAL.Write()` no fsync before close | `internal/storage/wal_interface.go:38-86` | Data may not reach stable storage before file close |
| 10 | LockFreeHNSW Search returns only 1 result | `internal/store/index/hnsw_lockfree.go:122` | Layer-0 BFS unimplemented — k>1 always returns 1 neighbor |
| 11 | DiskANN non-deterministic start node | `internal/store/index/diskann.go:272-276` | Go map iteration is random — search results vary per call |
| 12 | DiskANN `insertIntoGraph` never prunes neighbors | `internal/store/index/diskann.go:239-255` | Neighbor lists grow unbounded beyond MaxDegree |
| 13 | WebSocket server never stopped on shutdown | `cmd/longbow/main.go:431` | Listener and connections leak on graceful shutdown |
| 14 | MQ Exporter never stopped on shutdown | `cmd/longbow/main.go:454` | Buffered messages lost, goroutines leak |
| 15 | RDMA listener discarded, never served | `cmd/longbow/main.go:853`, `rdma_server.go:93` | Dead code — feature advertised but non-functional |

## High (broken features / race conditions)

| # | Item | File:Line | Impact |
|---|---|---|---|
| 16 | `HybridSearchWithBitmap` missing BM25Index fallback | `internal/store/hybrid_search.go:319-350` | Sparse search silently empty when BM25ArenaIndex is nil |
| 17 | `LoadSnapshots` swallows `.pq`/`.config` read errors | `internal/storage/engine.go:494-503` | Dataset loads without PQ codebook or index config |
| 18 | `LoadSnapshots` returns nil despite loadErrors | `internal/storage/engine.go:516-520` | Caller cannot detect partial snapshot load failure |
| 19 | Snapshot holds write lock for entire duration | `internal/storage/engine.go:197-291` | All writes blocked for seconds/minutes during snapshot |
| 20 | AsyncFsyncer `dirtyBytes` counter lost on multi-flush | `internal/storage/async_fsync.go:260-275` | Data durability window larger than configured |
| 21 | Replay goroutines leak on error | `internal/storage/wal_replay.go:100-105` | Blocked decoders hang on channel send after main loop breaks |
| 22 | Zstd decoder leak in replay | `internal/storage/wal_replay.go:296` | zstd.NewReader goroutines leak per compressed block |
| 23 | `RegisterCDCSubscriber`/`UnregisterCDCSubscriber` stubs | `internal/store/store.go:983-991` | CDC event system non-functional |
| 24 | `getPooledMetadataBuffer` never returns buffers | `internal/store/store.go:327-335` | Pool drains permanently — falls through to make() |
| 25 | `VerifyBackup` reads map without lock | `internal/store/backup.go:174` | Concurrent map read/write race |
| 26 | DiskANN `Load()` uses `f.Read()` not `io.ReadFull()` | `internal/store/index/diskann.go:519,540,567` | Silent data corruption on partial reads |
| 27 | IVFFlat `AddByRecord` ID collision | `internal/store/index/ivf_flat.go:135-142` | Concurrent calls read same len(), overwrite vectors |
| 28 | HNSWPluggableAdapter Search is brute-force O(n) | `internal/store/index/pluggable_index_adapters.go:79-115` | "HNSW" index does linear scan — no graph traversal |
| 29 | IVF-HNSW `fetchVector` always returns error | `internal/store/index/ivf_hnsw_composite.go:554-558` | Delta sync completely broken |
| 30 | SimdDetails unchecked type assertions | `cmd/longbow/main.go:353-356` | Panic if map key missing or wrong type |
| 31 | Listener type assertions unchecked | `cmd/longbow/main.go:795,811` | Panic if listener type differs |

## Medium (reliability gaps)

| # | Item | File:Line | Impact |
|---|---|---|---|
| 32 | Learned Index + Temporal Index discarded | `cmd/longbow/main.go:480,516` | Wasted ~400 MB, features appear supported but not wired |
| 33 | `PluggableInternalAdapter` ~15 stub methods | `internal/store/index/pluggable_index_adapters.go:519-600` | ExportState/ImportState/ExportDelta silently no-op |
| 34 | `BruteForceIndex` export/import no-ops | `internal/store/index/adaptive_index.go:330-358` | Backup/restore silently fails for BF-indexed datasets |
| 35 | IVFFlat/IVFOPQ ExportDelta no-op | `internal/store/index/ivf_pq_index.go:675`, `ivf_opq_index.go:675` | Incremental replication misses all changes |
| 36 | DiskANN `Close()` empty — memory leak | `internal/store/index/diskann.go:588-590` | GC cannot reclaim vectors/graph until reference dropped |
| 37 | IVFFlat `Close()` empty — memory leak | `internal/store/index/ivf_flat.go:507-509` | Same issue |
| 38 | `GetTombstones` returns mutable internal map | `internal/store/dataset.go:344-346` | Concurrent mutation without lock |
| 39 | `indexTextColumns` double-indexes both BM25 stores | `internal/store/store_hybrid.go:111-121` | 2x memory and indexing time |
| 40 | Emergency GC on ingestion throttle | `internal/store/store_actions.go:886-888` | Full stop-the-world pause under load |
| 41 | `ArrowHNSW.Close()` doesn't close DiskGraph | `internal/store/index/arrow_hnsw_persistence.go:354-377` | mmap file descriptor leak |
| 42 | `ArrowHNSW.Close()` doesn't stop RepairAgent | `internal/store/index/arrow_hnsw_persistence.go:354-377` | Goroutine accesses nil data pointer |
| 43 | `worker_pool` panics silently swallowed | `internal/store/worker_pool.go:148-153` | Task panics disappear — debugging impossible |

---

# Priority 1: turboquant2 Scaling Regression

| Field | Detail |
|---|---|
| Status | **OPEN — NOT FIXED** |
| Location | `internal/vector/turboquant.go`, `internal/simd/turboquant_asm_amd64.s` |

## What the Code Shows

| Metric | Normal quant | turboquant2 |
|---|---|---|
| Compressed bytes | 1 byte | **0.25 bytes** |
| Bit-range pairs | 256 | **4** |
| Quantized range | 65,536 levels | **256 levels** |
| Scale factor | 1.0× | **~3.2× smaller** |

## Known Issues

1. **efSearch cap at 600 is counterproductive**: `turboquant.go:83` caps `Params.EfSearch` at `max(600, 40)` for TQ2, which may limit recall for fine-grained queries where the quantized range is only 256 levels.
2. **No recall@10 validation for TQ2**: All 67 completed benchmark configs exclude turboquant2. Only a 100-vector post-SIMD-fix run includes TQ2.
3. **No adaptive bit-depth**: No mechanism to dynamically select 8-bit vs 2-bit based on data distribution.

## Recommendation

- [ ] Run TQ2 at full 50k scale with recall@10 validation
- [ ] Widen efSearch cap or make configurable per-vector-type
- [ ] Add `--query-mode hyperplane` for TQ2 to stress 4 bit-range-pair limitation

---

# Priority 2: Complete Benchmark at 500k Scale

| Field | Detail |
|---|---|
| Status | **INCOMPLETE — 11/17 types done** |
| Location | `cmd/bench-tool/main.go:717`, `tests/system/test_unified_benchmark.py` |

## Remaining Types (6)

| Type | Vectors | Memory Required | Status |
|---|---|---|---|
| int64 | 500k × 8B = 3.8 GB | ~19 GB | Not run |
| uint64 | 500k × 8B = 3.8 GB | ~19 GB | Not run |
| complex64 | 500k × 8B = 3.8 GB | ~19 GB | Not run |
| complex128 | 500k × 16B = 7.6 GB | ~23 GB | Not run |
| turboquant | 500k × 1B = 0.4 GB | ~16 GB | Not run |
| turboquant8 | 500k × 1B = 0.4 GB | ~16 GB | Not run |

System has 22 GB RAM. `complex128` needs ~23 GB — likely OOM without aggressive disk routing.

## Recommendation

- [ ] Run `int64`, `uint64`, `complex64` with `--use-disk --memory-budget-bytes 8000000000`
- [ ] Run `complex128` with `--use-disk --memory-budget-bytes 6000000000`
- [ ] Run `turboquant`, `turboquant8` with `--use-disk` (low memory, should succeed)
- [ ] Result JSON files must **never** be committed to git
