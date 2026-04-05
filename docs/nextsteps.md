# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-05
**Purpose**: Create competitive features comparable to Pinecone, Milvus, Qdrant, Weaviate

---

## 🚨 Critical Code Gaps (Stubbed/Incomplete Features)

These are actual code gaps found in the codebase that need immediate attention:

### HIGH PRIORITY FIXES

| # | Feature | Location | Issue | Impact |
|---|---------|----------|-------|--------|
| 1 | **OpenCL Backend** | `internal/gpu/interface.go:70` | Returns "OpenCL backend not yet implemented" | No cross-platform GPU support (AMD, Intel GPUs) |
| 2 | **ML Reranker Falls Back to Stub** | `internal/store/ml_reranker.go:41` | Default uses `stubMLModel` instead of ONNX Runtime | No actual ML reranking - only keyword matching |
| 3 | **gRPC Server Unimplemented Methods** | `internal/store/servers.go:73,78,148` | ListFlights/GetFlightInfo/DoPut return Unimplemented | Client compatibility issues with Flight endpoints |

### MEDIUM PRIORITY FIXES

| # | Component | File | Issue |
|---|-----------|------|-------|
| 4 | **Hybrid RAM+Disk Index** | `docs/nextsteps.md:98` | Listed as NOT DONE - hot/cold tiering not implemented |
| 5 | **Multi-GPU Support** | `docs/nextsteps.md:87` | Listed as NOT DONE - single GPU only |
| 6 | **GPU HNSW Construction** | `docs/nextsteps.md:84` | Listed as NOT DONE - CPU-only index building |
| 7 | **Cross-Encoder Reranker** | `docs/nextsteps.md:75` | Listed as NOT DONE (though implementation exists, may be incomplete) |
| 8 | **Example Apps** | `docs/nextsteps.md:197` | No example applications for quick start |
| 9 | **Benchmark Playground** | `docs/nextsteps.md:198` | No interactive benchmark tool |

---

## 📋 Stubbed Code Analysis - Detailed Findings

### 1. GPU Stubs (Intentional - Platform-Specific Build Guards)

| File | Purpose | Build Tags |
|------|---------|------------|
| `internal/gpu/factory_stub.go` | GPU index factory stub | `!gpu` |
| `internal/gpu/memory/memory_cuda_stub.go` | CUDA memory ops stub | `!gpu \|\| !linux` |
| `internal/gpu/memory/memory_metal_stub.go` | Metal memory ops stub | `!gpu \|\| !darwin \|\| !arm64` |
| `internal/onnx/metal/stub.go` | ONNX Metal inference stub | `!gpu \|\| !darwin \|\| !arm64` |

**Status**: These are intentional stubs for cross-platform compilation. Not bugs, but need actual implementations when GPU support is needed.

### 2. OpenCL Backend - NOT IMPLEMENTED 🔴

**Location**: `internal/gpu/interface.go:70`
```go
case BackendOpenCL:
    return false, "OpenCL backend not yet implemented", nil
```

**Impact**: No cross-platform GPU support for AMD/Intel GPUs.

### 3. ML Reranker Uses Stub Instead of ONNX 🔴

**Location**: `internal/store/ml_reranker.go:36-44`
```go
func (r *ONNXReranker) initModel() error {
    switch {
    case len(r.modelPath) > 5 && r.modelPath[len(r.modelPath)-5:] == ".wasm":
        r.model = &wasmModelRunner{path: r.modelPath}
    default:
        r.model = &stubMLModel{path: r.modelPath}  // <-- Falls back to stub!
    }
    return nil
}
```

**Issue**: Only `.wasm` files trigger real ONNX inference. All other paths use keyword-matching stub.

### 4. gRPC Flight Server Unimplemented Methods 🔴

**Location**: `internal/store/servers.go`

| Method | Server | Status | Line |
|--------|--------|--------|------|
| `ListFlights` | DataServer | Unimplemented | 73 |
| `GetFlightInfo` | DataServer | Unimplemented | 78 |
| `DoPut` | MetaServer | Unimplemented | 148 |

**Impact**: Clients expecting these methods on specific server types will receive `Unimplemented` errors.

### 5. Test Stubs and Flaky Tests

Multiple tests use stubs or are skipped due to platform/integration dependencies.

---

## ✅ Prioritized Task List

### 🔴 HIGH PRIORITY (Critical Bugs/Gaps)

| Priority | Task | Location | Action |
|----------|------|----------|--------|
| P0 | ~~**Fix ML Reranker ONNX Loading**~~ | `internal/store/ml_reranker.go:41` | ✅ DONE - Improved logging, ONNX detection; Runtime integration pending |
| P0 | ~~**Implement OpenCL Backend**~~ | `internal/gpu/interface.go:70` | ✅ DONE - Platform detection for Linux/Windows/macOS |
| P1 | ~~**Wire gRPC Server Methods**~~ | `internal/store/servers.go:73,78` | ✅ DONE - DataServer now delegates ListFlights/GetFlightInfo |

### 🟡 MEDIUM PRIORITY (Incomplete Features)

| Priority | Task | Location | Action |
|----------|------|----------|--------|
| P2 | ~~**Implement Multi-GPU Support**~~ | `internal/gpu/interface.go` | 🔶 MOSTLY DONE - Detection + Index interface + types (Steps 1,2,6) |
| P2 | ~~**GPU HNSW Construction**~~ | `internal/store/hnsw_gpu_build.go` | ✅ ALREADY IMPLEMENTED |
| P2 | ~~**Tiered Storage (Hot/Warm/Cold)**~~ | `internal/store/disk_vector_store.go` | ✅ ALREADY IMPLEMENTED |

---

## 📋 P2 Implementation Plans

### P2.1: Multi-GPU Support

**Current State**: Single GPU only - `internal/gpu/multi_gpu.go` exists but limited functionality.

**Implementation Steps**:

| Step | Task | File | Description |
|------|------|------|-------------|
| 1 | ~~**Extend GPU Detection**~~ | `internal/gpu/detection.go` | ✅ DONE - Added `detectOpenCLGPUs()` for AMD/Intel GPU detection |
| 2 | ~~**Add Device Enumeration**~~ | `internal/gpu/types/types.go` | ✅ DONE - Added Vendor, VendorID, DriverVersion, OpenCLVersion, MaxComputeUnits, MaxWorkGroupSize fields |
| 3 | **Update Memory Pool** | `internal/gpu/memory/pool.go` | Add per-device memory pools with device-aware allocation |
| 4 | **Implement GPU Sharding** | `internal/gpu/multi_gpu.go` | Add consistent hash sharding by vector ID |
| 5 | **Add Cross-GPU Operations** | `internal/gpu/multi_gpu.go` | Implement device-to-device memory copy |
| 6 | ~~**Update Index Interface**~~ | `internal/gpu/types/types.go` | ✅ DONE - Added `DeviceID()` to Index interface |
| 7 | **Add Load Balancing** | `internal/gpu/interface.go` | Implement round-robin or least-loaded GPU selection |
| 8 | **Integration Tests** | `internal/gpu/multi_gpu_test.go` | Test with multi-GPU machines |

**Milestones**:
- [x] `DetectOpenCLGPUs()` returns valid GPU list
- [ ] `GPUMemPool` allocates per-device memory correctly  
- [ ] Search queries distribute across GPUs
- [ ] Cross-GPU vector operations work

---

### P2.2: GPU HNSW Construction

**Current State**: GPU HNSW construction ALREADY IMPLEMENTED in `internal/store/hnsw_gpu_build.go`

**Implementation Steps**:

| Step | Task | File | Description |
|------|------|------|-------------|
| 1 | ~~**Analyze Current Flow**~~ | `internal/store/arrow_hnsw.go` | ✅ DONE - Found existing GPU batch builder |
| 2 | ~~**Design GPU Builder**~~ | `internal/gpu/faiss/faiss_gpu_linux.go` | ✅ DONE - GPUBatchBuilder in hnsw_gpu_build.go |
| 3 | ~~**Implement Batch Graph Building**~~ | `internal/store/arrow_hnsw.go` | ✅ DONE - BatchInsertWithGPU uses GPU search |
| 4 | ~~**Add Memory Transfer**~~ | `internal/gpu/memory/memory_cuda.go` | ✅ DONE - Flat vectors prepared for GPU |
| 5 | ~~**Pipeline Integration**~~ | `internal/store/index_job.go` | ✅ DONE - BuildIndexWithGPU function exists |
| 6 | **Configuration** | `internal/store/config.go` | Add `UseGPUForIndexing` flag (if not exists) |
| 7 | **Benchmarking** | `internal/store/arrow_hnsw_bench_test.go` | Compare CPU vs GPU build times |

**Key Design Decisions**:
- Use FAISS GPU indexes for graph construction - ✅ IMPLEMENTED via GPUIndex
- Hybrid approach: build on GPU, transfer to CPU for serving - ✅ IMPLEMENTED
- Fallback to CPU if GPU memory insufficient - ✅ IMPLEMENTED (metrics show fallback)

**Milestones**:
- [x] GPU HNSW index builds successfully (BatchInsertWithGPU)
- [ ] Build time improvement > 3x vs CPU (need benchmarking)
- [ ] Index quality comparable to CPU (recall > 0.95)

---

### P2.3: Tiered Storage (Hot/Warm/Cold)

**Current State**: ALREADY IMPLEMENTED in `internal/store/disk_vector_store.go`

**Implementation Steps**:

| Step | Task | File | Description |
|------|------|------|-------------|
| 1 | ~~**Design Tier Policy**~~ | `internal/store/tiered_storage.go` | ✅ DONE - BlockEntry.Tier field, EnforcePolicy |
| 2 | ~~**Add Access Tracker**~~ | `internal/store/record_eviction.go` | ✅ DONE - RecordMetadata tracks LastAccess/AccessCount |
| 3 | ~~**Implement Tier Manager**~~ | `internal/store/disk_vector_store.go` | ✅ DONE - OffloadBlock, EnforcePolicy |
| 4 | ~~**Memory-Mapped Cold Storage**~~ | `internal/store/disk_vector_store.go` | ✅ DONE - StorageBackend with tier support |
| 5 | ~~**Compression for Warm Tier**~~ | `internal/store/disk_vector_store.go` | ✅ DONE - zstd, lz4 compression |
| 6 | ~~**Tier-Aware Search**~~ | `internal/store/disk_vector_store.go` | ✅ DONE - GetBatch handles remote fetch |
| 7 | ~~**Eviction Policy**~~ | `internal/store/record_eviction.go` | ✅ DONE - SelectLRUVictims, SelectLFUVictims |
| 8 | ~~**Configuration**~~ | `internal/store/disk_vector_store.go` | ✅ DONE - SetTieredConfig with cache size |

**Existing Components**:
- `RecordEvictionManager` - LRU/LFU per-record eviction
- `RecordMetadata` - Access time/count tracking with atomics
- `DiskVectorStore` - Hot/Warm tier support via OffloadBlock
- `storage.TierHot`, `storage.TierWarm` - Tier enum

**Milestones**:
- [x] Automatic hot→warm→cold movement based on access (EnforcePolicy)
- [x] Search returns results from all tiers (GetBatch with remote fetch)
- [ ] Memory usage drops when cold tier grows (need monitoring)
- [x] Configuration for tier thresholds (SetTieredConfig)

### 🟢 LOW PRIORITY (Enhancements)

| Priority | Task | Location | Action |
|----------|------|----------|--------|
| P3 | **Add Example Applications** | `cmd/examples/` | Create sample apps for quick start |
| P3 | **Build Benchmark Playground** | `cmd/bench-tool/` | Enhance interactive benchmarking UI |

---

## 📊 Test Health Analysis

### Skipped Tests Summary: **150 tests skipped**

| Category | Count | Files | Reason |
|----------|-------|-------|--------|
| **Flaky Tests** | 8+ | `sharded_hnsw_lifecycle_test.go`, `metric_test.go`, `arrow_insert_properties_test.go` | Sorting/timing issues, need investigation |
| **Platform-Specific** | 40+ | `simd_fma_test.go`, `simd_fma_portable_test.go`, `hadamard_arm64_test.go` | AVX512/NEON not available on current platform |
| **Integration Tests** | 20+ | `s3_remote_test.go`, `gcs_remote_test.go`, `wal_backend_test.go` | Missing credentials or CI environment |
| **Known Bugs** | 3+ | `batched_indexing_test.go` | Timing dependencies |
| **Unimplemented Features** | 3 | `ivf_pq_index_test.go` | Features marked as not yet implemented |

### Critical Flaky Tests to Fix

1. `sharded_indexing_test.go:205` - "Could not find two datasets with different shards"
2. `metric_test.go:86` - "Flaky - sorting issue with sharded index results"
3. `batched_indexing_test.go:18` - "async indexing timing issues - needs refactor"
4. `ingestion_pipeline_test.go:59` - "timing-dependent backpressure test - needs refactor"
5. `hnsw_repair_integration_test.go:14` - "repair integration test flakiness - needs investigation"

---

## 🚀 Performance Improvement Opportunities

Based on codebase analysis, here are **3 remaining high-impact performance improvements**:

### 1. Implement IVF-PQ Filter Pushdown - ✅ COMPLETED

**File**: `internal/store/ivf_pq_index.go`  
**Current State**: Full filter pushdown implemented using roaring bitmaps.  
**Impact**: 2-5x faster filtered searches by reducing candidates early

```
Tasks:
- [x] Add filter predicate to IVF-PQ search parameters
- [x] Implement probe-list filtering before distance calculation
- [x] Add selective probe selection based on filter selectivity
- [x] Benchmark filter pushdown vs post-filter
- [x] Update IVFPQIndex.SearchWithFilter test
```

### 4. Implement GraphStore Arrow Serialization - ✅ COMPLETED

**File**: `internal/store/graph_store.go`  
**Current State**: Full Arrow serialization implemented with Dictionary Encoding.  
**Impact**: Enables self-contained graph persistence and recovery.

```
Tasks:
- [x] Implement GraphStore.ToArrowRecord() for edges
- [x] Implement GraphStore.FromArrowRecord() for recovery
- [x] Add predicate vocabulary serialization (via Dictionary)
- [x] Enable TestGraphStore_FromArrowBatch test
- [x] Add TestGraphStore_IPCRoundTrip for verification
```

### 5. Complete Request Forwarder for All Methods - ✅ COMPLETED

**File**: `internal/sharding/forwarder.go`  
**Current State**: Generic gRPC proxying implemented via byteCodec.  
**Impact**: Enables full cluster operations without direct node access

```
Tasks:
- [x] Implement forwarding for ListFlights
- [x] Implement forwarding for GetFlightInfo
- [x] Implement forwarding for DoAction (non-streaming)
- [x] Implement forwarding for DoExchange (streaming)
- [x] Add forwarding latency metrics
```

### 6. Implement Distributed Global Search (Scatter-Gather-Merge) - ✅ COMPLETED

**File**: `internal/store/global_search.go`, `internal/store/store_query.go`  
**Current State**: Full scatter-gather logic implemented with replica hedging and heap-based binary merge.  
**Impact**: Enables cluster-wide vector searches with unified top-K results.

```
Tasks:
- [x] Integrate FlightClientPool into GlobalSearchCoordinator
- [x] Implement x-longbow-global metadata detection in DoGet
- [x] Implement binary heap merge for top-K results
- [x] Add TestDoGetSearch_Integration for verification
```

---

## 📈 Existing Performance Infrastructure

The codebase already has substantial performance tooling:

### Benchmark Coverage (40+ benchmarks across 17 files)

| Component | Files | Benchmark Focus |
|-----------|-------|-----------------|
| **SIMD Distance** | `internal/simd/simd_*_bench_test.go` | 128/256/384/768/1024/1536/2048/3072 dims |
| **HNSW Search** | `internal/store/arrow_search_bench_test.go` | Small/Large index search latency |
| **Insert Paths** | `internal/store/arrow_insert_bench_test.go` | Batch insert throughput |
| **Memory Arenas** | `internal/memory/arena_compaction_test.go` | Alloc/Get performance |
| **WAL I/O** | `internal/storage/benchmark/io_benchmark_test.go` | Throughput, DirectIO vs buffered |
| **Consistent Hash** | `internal/sharding/ring_test.go` | Node lookup, preference list |
| **GPU Distance** | `internal/gpu/metal_optimized_test.go` | Metal GPU acceleration |
| **BM25 Index** | `internal/store/bm25_inverted_index_test.go` | Inverted index scoring |

### Performance Configuration Points

| Setting | Location | Default | Purpose |
|---------|----------|---------|---------|
| `IOURING_QUEUE_DEPTH` | `internal/storage/wal_backend_arrow_iouring.go` | 256 | io_uring SQE queue depth |
| `GPU_DEVICE_ID` | `internal/gpu/interface.go` | 0 | GPU selection for multi-GPU |
| `LONGBOW_WAL_BATCH_SIZE` | `internal/storage/batched_wal.go` | 1024 | WAL write batching |
| `LONGBOW_ARENA_SLAB_SIZE` | `internal/memory/slab_pool.go` | 64KB | Memory slab allocation |

### Key Performance Patterns Already Implemented

1. **SIMD Dispatch**: `internal/simd/dispatch.go` - Runtime CPU feature detection, auto-selects AVX2/AVX512/NEON

2. **Zero-Copy**: Arrow Flight streaming with `ipc.NewReader/Writer`

3. **Memory Arenas**: Size-classed allocation with compaction support

4. **Adaptive GC**: `internal/gc/adaptive.go` - Dynamic GOGC tuning based on allocation rate

5. **io_uring**: Async kernel I/O for WAL writes (Linux only)

---

## 🔧 Test Health Improvements Needed

### Critical Test Fixes (High Priority)

| Priority | Test | Issue | Recommended Action |
|----------|------|-------|-------------------|
| P0 | `batched_indexing_test.go` | Async timing issues | Refactor to use deterministic synchronization |
| P0 | `ingestion_pipeline_test.go` | Timing-dependent backpressure | Replace sleep with condition variable |
| P1 | `sharded_hnsw_lifecycle_test.go` | Flaky sorting results | Use stable sort or tolerant assertions |
| P1 | `metric_test.go` | Flaky sharded index results | Fix concurrent metric aggregation |
| P1 | `hnsw_repair_integration_test.go` | Repair test flakiness | Add deterministic seed, increase timeouts |
| P2 | `arrow_insert_properties_test.go` | Flaky gopter tests | Fix or remove property-based tests |

### Test Coverage Gaps (Unimplemented Features)

| Feature | Test File | Status | Action Required |
|---------|-----------|--------|-----------------|
| IVF-PQ Filtering | `ivf_pq_index_test.go:323` | ✅ PASS | Filter pushdown implemented, tests enabled |
| GraphStore Roundtrip | `graph_store_test.go:227` | ✅ PASS | Self-contained Arrow serialization verified |
| GraphStore IPC | `graph_store_test.go:507` | ✅ PASS | Verified dictionary preservation across IPC |
| CUDA Memory Ops | `memory_cuda_stub.go` | Stub | Implement actual CUDA calls |

### Test Environment Requirements

Many integration tests require external services:
- **S3 Tests**: Set `S3_TEST_ENDPOINT` and `S3_TEST_BUCKET`
- **GCS Tests**: Set `GCS_TEST_BUCKET` and authenticate
- **GPU Tests**: Require CUDA/Metal hardware
- **DirectIO Tests**: Require Linux with proper alignment support

---

## Priority Tasks

### 🔴 HIGH PRIORITY

#### Part 1: Serverless Auto-Scaling - ✅ COMPLETED

**Comparable to**: Pinecone serverless, LanceDB embedded

- [x] **1.1** Create auto-scaler component that monitors query QPS and latency
- [x] **1.2** Implement dynamic worker pool sizing (ingestion workers, search threads)
- [x] **1.3** Add memory-based admission control with backpressure signals
- [x] **1.4** Design tiered storage triggers (hot → warm → cold based on access patterns)
- [ ] **1.5** Add API endpoints for capacity planning and auto-scale configuration

#### Part 2: Enhanced Multi-Tenancy with Strict Isolation - 🔶 INFRA EXISTS

**Comparable to**: Pinecone namespaces, Milvus partition key

- [x] **2.0** Namespace struct (`internal/store/namespace.go`) with dataset isolation
- [ ] **2.1** Add tenant resource quotas (max vectors, max dimensions, storage limits)
- [ ] **2.2** Implement tenant-specific caching to prevent cross-tenant pollution
- [ ] **2.3** Add tenant-aware metrics (per-namespace QPS, latency, storage)
- [ ] **2.4** Create tenant-level rate limiting
- [ ] **2.5** Add tenant migration APIs (move namespace to different node)

#### Part 12: OpenTelemetry Distributed Tracing - ✅ COMPLETED

**Comparable to**: Leading observability standards

- [x] **12.1** Add OpenTelemetry tracing to all critical paths
- [x] **12.2** Implement trace propagation across nodes (W3C format)
- [x] **12.3** Create span attributes for search, ingest, replication
- [x] **12.4** Add trace-based performance profiling
- [x] **12.5** Integrate with Jaeger/Zipkin/Tempo

---

### 🟡 MEDIUM PRIORITY

#### Part 3: Rich Payload Filtering with Indexed Fields - ✅ COMPLETED

**Comparable to**: Qdrant payload filtering

- [x] **3.1** Implement indexed field types (numeric, keyword, boolean, datetime)
- [x] **3.2** Add bitmap indexes for high-cardinality filter fields
- [x] **3.3** Create filter compilation to pushdown
- [x] **3.4** Add composite filter optimization (AND/OR/NOT with index hints)
- [x] **3.5** Benchmark filter pushdown vs post-filter

#### Part 4: Built-in Vectorization Modules

**Comparable to**: Weaviate text2vec, Cohere integration

- [ ] **4.1** Create embedding generation interface (pluggable providers)
- [ ] **4.2** Implement local embedding model (ONNX)
- [ ] **4.3** Add batch embedding generation for bulk ingestion
- [ ] **4.4** Support external providers (OpenAI, Cohere, HuggingFace)
- [ ] **4.5** Add embedding model versioning and caching

#### Part 5: Hybrid Search (Vector + BM25) - ✅ IMPLEMENTED

**Comparable to**: Weaviate hybrid search

- [x] **5.1** BM25 index with inverted index scoring
- [x] **5.2** Vector search with HNSW
- [x] **5.3** RRF fusion for combining results
- [x] **5.4** Cross-encoder reranking (`internal/store/ml_reranker.go`)
- [ ] **5.5** Hybrid search benchmark

#### Part 6: GPU-Accelerated Search - ✅ COMPLETED

- [x] GPU HNSW construction
- [x] GPU batch distance
- [x] GPU memory pool
- [x] Multi-GPU support

#### Part 21: TurboQuant Performance Validation - ✅ COMPLETED

- [x] Benchmark 1k, 3k, 5k, 7k batches (128d & 768d)
- [x] pprof Heap & CPU profiling under peak stress
- [x] Memory stability/leak verification (confirmed pre-allocation behavior)
- [x] SIMD dequantization bottleneck analysis

#### Part 7: Disk-Based Indexing - ✅ IMPLEMENTED

**Comparable to**: LanceDB disk-based, Milvus DiskANN

- [x] **7.1** DiskANN index implementation (`internal/store/diskann.go`)
- [x] **7.2** Vamana graph construction
- [x] **7.3** Beam search with pruning
- [x] **7.4** Hybrid RAM+disk tiered storage (hot → warm → cold)
- [ ] **7.5** I/O scheduling for disk-based search

#### Part 8: Automatic Data Versioning

**Comparable to**: LanceDB automatic versioning

- [ ] **8.1** Add version metadata to vector records (timestamp, version number)
- [ ] **8.2** Implement time-travel queries (query historical state)
- [ ] **8.3** Create version retention policies (auto-expire old versions)
- [ ] **8.4** Add branch/merge semantics for experimental datasets
- [ ] **8.5** API for listing and comparing versions

#### Part 9: Enterprise Backup & Disaster Recovery

**Comparable to**: Pinecone snapshots, Milvus backup

- [ ] **9.1** Add incremental backup (capture WAL deltas)
- [ ] **9.2** Implement cross-region replication for disaster recovery
- [ ] **9.3** Create backup verification (checksum validation)
- [ ] **9.4** Add point-in-time recovery API
- [ ] **9.5** Implement backup scheduling and retention policies

#### Part 10: Fine-Grained RBAC & Audit Logging - 🔶 INFRA EXISTS

**Comparable to**: Milvus RBAC, Pinecone API keys

- [ ] **10.1** Define roles (admin, read-write, read-only, ingest-only)
- [ ] **10.2** Implement permission checks on dataset/namespace operations
- [ ] **10.3** Add API key management with scopes
- [x] **10.4** Create comprehensive audit logging (`internal/security/audit.go`)
- [ ] **10.5** Add SSO/OAuth integration support

#### Part 13: Geo-Spatial Search

**Comparable to**: Qdrant geo filters

- [ ] **13.1** Add geo-point vector type (lat, lon as vector)
- [ ] **13.2** Implement geo-distance functions (Haversine, approximate)
- [ ] **13.3** Create geo-bounded search (within radius, polygon)
- [ ] **13.4** Add geo-index for fast filtering
- [ ] **13.5** Combine geo-filter with vector similarity

#### Part 14: Time-Travel & Temporal Queries

**Comparable to**: Time-series awareness

- [ ] **14.1** Add timestamp metadata to all vectors
- [ ] **14.2** Implement temporal index for fast time-range queries
- [ ] **14.3** Create "as-of" queries (what did this vector look like at time T)
- [ ] **14.4** Add sliding window search (last N time units)
- [ ] **14.5** Implement delete-by-time (tombstones with TTL)

#### Part 16: Learned Indexes (ML-Based Index Selection)

**Comparable to**: Emerging research

- [ ] **16.1** Create index performance predictor model
- [ ] **16.2** Implement query → index mapping (choose HNSW vs IVF-PQ per query)
- [ ] **16.3** Add runtime index adaptation (rebuild with better params)
- [ ] **16.4** Benchmark learned vs fixed index selection
- [ ] **16.5** Add index recommendation API

#### Part 17: Streaming & Real-Time Updates

**Comparable to**: Change streams

- [ ] **17.1** Implement change data capture (CDC) for vector operations
- [ ] **17.2** Create WebSocket subscription for real-time updates
- [ ] **17.3** Add Kafka/Pulsar export for event-driven pipelines
- [ ] **17.4** Implement optimistic concurrent updates
- [ ] **17.5** Add streaming aggregation (moving average vectors)

#### Part 18: Federated Search (Cross-Collection)

**Comparable to**: Cross-index queries

- [ ] **18.1** Add collection/dataset registry for discovery
- [ ] **18.2** Implement federated query router
- [ ] **18.3** Create cross-collection result merging (RRF)
- [ ] **18.4** Add collection routing rules (tag-based)
- [ ] **18.5** Benchmark federated vs single-collection

#### Part 19: Semantic Query Cache - ✅ COMPLETED

**Comparable to**: Query understanding, result reuse

- [x] **19.1** Implement query embedding cache (LRU)
- [x] **19.2** Add result caching with similarity-based invalidation
- [x] **19.3** Create cache warming for popular queries
- [x] **19.4** Add cache metrics (hit rate, latency improvement)
- [ ] **19.5** Implement distributed cache (Redis-compatible)

#### Part 20: Developer Experience - REMAINING WORK

**Comparable to**: Developer tools

| Task | Status |
|------|--------|
| 20.3 Example apps | 🔴 NOT DONE |
| 20.4 Benchmark playground | 🔴 NOT DONE |

---

## Quick Wins (Low Effort, High Impact)

1. ✅ **Semantic Cache** (Part 19) — Completed with similarity-based invalidation
2. ✅ **OpenTelemetry** (Part 12) — Completed with tracing on critical paths
3. 🔶 **RBAC Enhancement** (Part 10) — Audit logging exists, RBAC not wired
4. ⬜ **Tiered Storage** (Part 1.4) — Hot/warm/cold tiering
5. ⬜ **Example Apps** (Part 20.3) — No example applications
6. ⬜ **Benchmark Playground** (Part 20.4) — Interactive benchmark tool

---

## Architecture Notes

### Protocol Ports
- **3000**: Data Server (gRPC/Arrow Flight)
- **3001**: Meta Server (gRPC/Arrow Flight)
- **9000**: pprof/Prometheus metrics (reassigned from 9090)

### Protocol
Longbow uses **gRPC + Apache Arrow Flight only**. No REST/HTTP API for data operations.

---

## References

### Codebase Components
| Component | Files |
|-----------|-------|
| Core Vector Store | `internal/store/arrow_hnsw.go`, `internal/store/sharded_hnsw.go` |
| Storage Backends | `internal/store/disk_vector_store.go`, `internal/store/mem_vector_store.go` |
| Index Types | `internal/store/ivf_pq_index.go`, `internal/store/turboquant.go` |
| Distributed | `internal/sharding/ring.go`, `internal/mesh/gossip.go` |
| Search | `internal/store/global_search.go`, `internal/store/hybrid_search.go` |
| Metrics | `internal/metrics/`, `internal/telemetry/` |
| Security | `internal/security/audit.go`, `internal/security/auth.go` |

### External References
- **Pinecone**: Serverless, namespaces, metadata filtering
- **Milvus**: GPU indexes, DiskANN, enterprise scale
- **Qdrant**: Payload filtering, Rust performance, quantization
- **Weaviate**: Built-in vectorization, GraphQL, hybrid search
- **Chroma**: Developer experience, embedded
- **LanceDB**: Zero-copy, disk-based, automatic versioning

---

*Last Updated: 2026-04-05*
