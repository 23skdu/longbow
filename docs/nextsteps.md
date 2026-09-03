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

| # | Initiative | Status | Description |
|---|---|:---:|---|
| 1 | **TurboQuant2 50k Recall@10 Validation** | Planned | Comprehensive 50k Recall@10 validation tests with adaptive bit-depth and widened search parameters. |
| 2 | **500k Scale Benchmark Suite Execution** | **Completed** | Full matrix evaluation of 16 datatypes across 10k, 25k, 200k, and 500k scales on both CPU (AVX2) and GPU (NVIDIA RTX 4060 CUDA). Results published to [`docs/performance.md`](performance.md). |
| 3 | **Multi-Node RDMA Integration Validation** | Planned | Live cluster verification of RoCE/InfiniBand Flight RDMA transport. |

---

## Architectural & Performance Optimizations Identified from Comprehensive Benchmark Runs

The comprehensive 128-configuration benchmark across both CPU and CUDA hardware highlighted key architectural opportunities for the v0.3.0 pipeline:

### 1. `GCTuner` Emergency Rate Limiter & Cooldown
- **Status**: **Implemented & Verified** (unit tests & `FuzzGCTuner_EmergencyRateLimiter` passed).
- **Observation**: During high memory pressure (`ratio > 0.92`), `GCTuner.tune()` fired `runtime.GC()` and `debug.FreeOSMemory()` unconditionally on every 500ms tick. On large off-heap graphs (>18 GB), sweeping takes multiple seconds, inducing continuous Stop-The-World (STW) pauses and consuming ~300% CPU purely running GC scans.
- **Resolution**: Introduced adaptive emergency GC rate limiter (minimum 10s cooldown between forced `runtime.GC()` and `FreeOSMemory()` cycles) and 5s diagnostic rate-limiting in [`internal/memory/gc_tuner.go`](../internal/memory/gc_tuner.go), eliminating GC scanning livelock under high memory pressure.

### 2. Fast-Fail Readiness Polling on Admission Block (`ResourceExhausted`)
- **Status**: **Implemented & Verified** (unit tests & `FuzzCheckReadiness_StatusParsing` passed).
- **Observation**: When vector and index allocations exceed `LONGBOW_MAX_MEMORY`, `CanAdmitSearch()` rejects incoming queries with `codes.ResourceExhausted`. In [`cmd/bench-tool/main.go`](../cmd/bench-tool/main.go), `waitForIndexingComplete` polled `check_readiness` for the full 4-hour timeout because the readiness status remained `BUSY`.
- **Resolution**: Updated [`internal/store/store_actions.go`](../internal/store/store_actions.go) to return `status: "RESOURCE_EXHAUSTED"` on admission memory blocks, and updated [`cmd/bench-tool/main.go`](../cmd/bench-tool/main.go) to immediately fast-fail when receiving `RESOURCE_EXHAUSTED` or consecutive memory limits.

### 3. Automatic Spill-to-Disk Paging for 500k+ Uncompressed High-Dimension Vectors
- **Status**: In Pipeline (v0.3.0)
- **Observation**: At 500,000 vectors with 384 dimensions, uncompressed 64-bit and 128-bit types (`float64`, `complex64`, `complex128`, `int64`) consume 1.54 GB – 3.07 GB raw vectors, expanding into ~20–24 GB during multi-layer HNSW graph construction. On hosts with ≤24 GB RAM, this triggers kernel swap paging.
- **Action Item**: Automatically engage `LONGBOW_USE_DISK=1` or memory-mapped partition backing when estimated vector allocation exceeds 70% of available physical memory.

### 4. TurboQuant Default for High-Scale Memory Efficiency
- **Status**: Documented & Recommended
- **Observation**: Performance data from both CPU and CUDA runs demonstrated that TurboQuant (2-bit, 4-bit, 8-bit) provides 8x to 32x memory compression (only 16–64 MB raw buffer at 500k vectors) while matching or exceeding unquantized search performance (3,200–3,400 QPS on CPU, 3,200–3,660 QPS on GPU) with ~2.5ms median latency.
- **Action Item**: Standardize on TurboQuant as the default recommended vector storage mode for 500k+ vector deployments on memory-constrained infrastructure.

### 5. Asynchronous Pinned-Host CUDA Memory Transfers
- **Status**: **Implemented & Verified** (unit tests & `FuzzCUDA_PinnedBufferPool` passed).
- **Observation**: In GPU benchmarks, synchronous host-to-device vector copies (`cudaMemcpy`) on unpinned Go heap buffers introduce latency overhead on high-dimension queries (e.g. `complex128` 384d).
- **Resolution**: Implemented `cudaHostAlloc`, `cudaFreeHost`, and `cudaMemcpyAsync` in [`internal/gpu/cuda/cuda_backend_linux.h`](../internal/gpu/cuda/cuda_backend_linux.h) and [`internal/gpu/cuda/cgo_linux.go`](../internal/gpu/cuda/cgo_linux.go). Added `PinnedHostPool` and integrated double-buffered asynchronous stream copies on `handle.streams[0]` in [`internal/gpu/cuda/cuda_index.go`](../internal/gpu/cuda/cuda_index.go).


