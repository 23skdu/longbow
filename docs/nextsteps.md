# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-03-31
**Purpose**: Create competitive features comparable to Pinecone, Milvus, Qdrant, Weaviate

---

## 🚨 Critical Code Gaps (Stubbed/Incomplete Features)

These are actual code gaps found in the codebase that need immediate attention:

### HIGH PRIORITY FIXES

| # | Component | File | Issue | Impact |
|---|-----------|------|-------|--------|
| 1 | **CUDA Memory Ops** | `internal/gpu/memory/memory_cuda_stub.go` | `freeCUDAMemory()`, `cudaMemcpyHostToDevice()`, `cudaMemcpyDeviceToHost()` all return "not implemented" errors | GPU memory operations fall back to CPU, severe performance penalty on CUDA systems |
| 2 | **IVF-PQ Filter Support** | `internal/store/ivf_pq_index_test.go:323` | Test skipped: "Filter support not yet implemented" | Cannot filter during IVF-PQ search, requires post-filter which degrades performance |
| 3 | **GraphStore Arrow Serialization** | `internal/store/graph_store_test.go:155` | Test skipped: "Arrow serialization for GraphStore not yet implemented" | Graph data cannot be persisted/recovered via Arrow format, only in-memory |
| 4 | **Metal Index Optimized** | `internal/gpu/metal/metal_gpu_optimized.go` | ✅ FIXED: Replaced invalid `float8`/`half8` types with `float4`/`half4`, removed `simd_shuffle_down` usage | Apple Silicon GPU acceleration now functional |
| 5 | **Request Forwarder Gaps** | `internal/sharding/forwarder.go:256` | Many forwarding methods return "not yet implemented" | Some cluster operations cannot be proxied between nodes |
| 6 | **OpenCL Backend** | `internal/gpu/interface.go:70` | Returns "OpenCL backend not yet implemented" | No cross-platform GPU support (AMD, Intel GPUs) |

### MEDIUM PRIORITY FIXES

| # | Component | File | Issue |
|---|-----------|------|-------|
| 7 | **Hybrid RAM+Disk Index** | `docs/nextsteps.md:98` | Listed as NOT DONE - hot/cold tiering not implemented |
| 8 | **Multi-GPU Support** | `docs/nextsteps.md:87` | Listed as NOT DONE - single GPU only |
| 9 | **GPU HNSW Construction** | `docs/nextsteps.md:84` | Listed as NOT DONE - CPU-only index building |
| 10 | **Cross-Encoder Reranker** | `docs/nextsteps.md:75` | Listed as NOT DONE (though implementation exists, may be incomplete) |
| 11 | **Example Apps** | `docs/nextsteps.md:197` | No example applications for quick start |
| 12 | **Benchmark Playground** | `docs/nextsteps.md:198` | No interactive benchmark tool |

---

## 📊 Test Health Analysis

### Skipped Tests Summary: **150 tests skipped**

| Category | Count | Files | Reason |
|----------|-------|-------|--------|
| **Flaky Tests** | 8+ | `sharded_hnsw_lifecycle_test.go`, `metric_test.go`, `arrow_insert_properties_test.go` | Sorting/timing issues, need investigation |
| **Platform-Specific** | 40+ | `simd_fma_test.go`, `simd_fma_portable_test.go`, `hadamard_arm64_test.go` | AVX512/NEON not available on current platform |
| **Integration Tests** | 20+ | `s3_remote_test.go`, `gcs_remote_test.go`, `wal_backend_test.go` | Missing credentials or CI environment |
| **Known Bugs** | 5+ | `metal_optimized_test.go`, `batched_indexing_test.go` | Shader compilation issues, timing dependencies |
| **Unimplemented Features** | 4 | `ivf_pq_index_test.go`, `graph_store_test.go` | Features marked as not yet implemented |

### Critical Flaky Tests to Fix:
1. `sharded_indexing_test.go:205` - "Could not find two datasets with different shards"
2. `metric_test.go:86` - "Flaky - sorting issue with sharded index results"
3. `batched_indexing_test.go:18` - "async indexing timing issues - needs refactor"
4. `ingestion_pipeline_test.go:59` - "timing-dependent backpressure test - needs refactor"
5. `hnsw_repair_integration_test.go:14` - "repair integration test flakiness - needs investigation"

---

## 🚀 Performance Improvement Opportunities

Based on codebase analysis, here are **5 high-impact performance improvements**:

### 1. Implement CUDA Memory Operations (Critical GPU Path)

**File**: `internal/gpu/memory/memory_cuda_stub.go`  
**Current State**: All operations return errors, falling back to CPU  
**Impact**: 10-100x speedup on NVIDIA GPU systems for batch distance calculations

```
Tasks:
- [ ] Implement freeCUDAMemory() using cuMemFree
- [ ] Implement cudaMemcpyHostToDevice() using cuMemcpyHtoD
- [ ] Implement cudaMemcpyDeviceToHost() using cuMemcpyDtoH
- [ ] Add proper error handling with CUDA error codes
- [ ] Add benchmark tests comparing GPU vs CPU paths
```

### 2. Fix Metal Optimized Index Shader Compilation ✅ COMPLETED

**File**: `internal/gpu/metal/metal_gpu_optimized.go`  
**Status**: **FIXED** - Replaced invalid `float8`/`half8` types with `float4`/`half4`, removed problematic `simd_shuffle_down` usage  
**Impact**: Enables 3-5x speedup on Apple Silicon for optimized index operations

```
Completed:
- [x] Fixed Metal shader compilation errors (float8→float4, half8→half4)
- [x] Fixed kernel dispatch for optimized distance calculations  
- [x] Added proper error messages for shader failures
- [x] Enabled MetalIndexOptimized tests (removed skip)
- [x] Added comprehensive unit tests for Metal index
```

### 3. Implement IVF-PQ Filter Pushdown

**File**: `internal/store/ivf_pq_index.go`  
**Current State**: Filter support not implemented, requires post-filter  
**Impact**: 2-5x faster filtered searches by reducing candidates early

```
Tasks:
- [ ] Add filter predicate to IVF-PQ search parameters
- [ ] Implement probe-list filtering before distance calculation
- [ ] Add selective probe selection based on filter selectivity
- [ ] Benchmark filter pushdown vs post-filter
- [ ] Update IVFPQIndex.SearchWithFilter test
```

### 4. Implement GraphStore Arrow Serialization

**File**: `internal/store/graph_store.go`  
**Current State**: No Arrow serialization, only in-memory  
**Impact**: Enables graph persistence, recovery, and zero-copy transfer

```
Tasks:
- [ ] Implement GraphStore.ToArrowRecord() for edges
- [ ] Implement GraphStore.FromArrowRecord() for recovery
- [ ] Add predicate vocabulary serialization
- [ ] Integrate with WAL for durability
- [ ] Enable TestGraphStore_FromArrowBatch test
```

### 5. Complete Request Forwarder for All Methods

**File**: `internal/sharding/forwarder.go`  
**Current State**: Many methods return "not yet implemented"  
**Impact**: Enables full cluster operations without direct node access

```
Tasks:
- [ ] Implement forwarding for ListFlights
- [ ] Implement forwarding for GetFlightInfo
- [ ] Implement forwarding for DoAction (non-streaming)
- [ ] Add retry logic for transient failures
- [ ] Add forwarding latency metrics
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
| IVF-PQ Filtering | `ivf_pq_index_test.go:323` | Skipped | Implement filter pushdown, enable test |
| GraphStore Serialization | `graph_store_test.go:155` | Skipped | Implement Arrow serialization |
| Metal Optimized Index | `metal_optimized_test.go` | ✅ Fixed | Shader compilation issues resolved |
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

#### Part 1: Serverless Auto-Scaling

**Comparable to**: Pinecone serverless, LanceDB embedded

- [ ] **1.1** Create auto-scaler component that monitors query QPS and latency
- [ ] **1.2** Implement dynamic worker pool sizing (ingestion workers, search threads)
- [ ] **1.3** Add memory-based admission control with backpressure signals
- [ ] **1.4** Design tiered storage triggers (hot → warm → cold based on access patterns)
- [ ] **1.5** Add API endpoints for capacity planning and auto-scale configuration

#### Part 2: Enhanced Multi-Tenancy with Strict Isolation

**Comparable to**: Pinecone namespaces, Milvus partition keys

- [ ] **2.1** Add tenant resource quotas (max vectors, max dimensions, storage limits)
- [ ] **2.2** Implement tenant-specific caching to prevent cross-tenant pollution
- [ ] **2.3** Add tenant-aware metrics (per-namespace QPS, latency, storage)
- [ ] **2.4** Create tenant-level rate limiting
- [ ] **2.5** Add tenant migration APIs (move namespace to different node)

#### Part 12: OpenTelemetry Distributed Tracing

**Comparable to**: Leading observability standards

- [ ] **12.1** Add OpenTelemetry tracing to all critical paths
- [ ] **12.2** Implement trace propagation across nodes (W3C format)
- [ ] **12.3** Create span attributes for search, ingest, replication
- [ ] **12.4** Add trace-based performance profiling
- [ ] **12.5** Integrate with Jaeger/Zipkin/Tempo

---

### 🟡 MEDIUM PRIORITY

#### Part 3: Rich Payload Filtering with Indexed Fields

**Comparable to**: Qdrant payload filtering

- [ ] **3.1** Implement indexed field types (numeric, keyword, boolean, datetime)
- [ ] **3.2** Add bitmap indexes for high-cardinality filter fields
- [ ] **3.3** Create filter compilation to pushdown
- [ ] **3.4** Add composite filter optimization (AND/OR/NOT with index hints)
- [ ] **3.5** Benchmark filter pushdown vs post-filter

#### Part 4: Built-in Vectorization Modules

**Comparable to**: Weaviate text2vec, Cohere integration

- [ ] **4.1** Create embedding generation interface (pluggable providers)
- [ ] **4.2** Implement local embedding model (ONNX)
- [ ] **4.3** Add batch embedding generation for bulk ingestion
- [ ] **4.4** Support external providers (OpenAI, Cohere, HuggingFace)
- [ ] **4.5** Add embedding model versioning and caching

#### Part 5: Hybrid Search (Vector + BM25) - REMAINING WORK

**Comparable to**: Weaviate hybrid search

| Task | Status |
|------|--------|
| 5.1 Unified search API | ✅ DONE |
| 5.2 RRF | ✅ DONE |
| 5.3 Auto-tune alpha | ✅ DONE |
| 5.4 Cross-encoder | 🔴 NOT DONE |
| 5.5 Benchmark | 🔴 NOT DONE |

#### Part 6: GPU-Accelerated Search - REMAINING WORK

**Comparable to**: Milvus GPU indexes

| Task | Status |
|------|--------|
| 6.1 GPU HNSW construction | 🔴 NOT DONE |
| 6.2 GPU batch distance | ✅ DONE |
| 6.3 GPU memory pool | ✅ DONE |
| 6.4 Multi-GPU | 🔴 NOT DONE |

#### Part 7: Disk-Based Indexing - REMAINING WORK

**Comparable to**: LanceDB disk-based, Milvus DiskANN

| Task | Status |
|------|--------|
| 7.1 mmap storage | ✅ DONE |
| 7.2 DiskANN | ✅ DONE |
| 7.3 SSD caching | ✅ DONE |
| 7.4 Hybrid RAM+disk | 🔴 NOT DONE |
| 7.5 I/O scheduling | 🔴 NOT DONE |

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

#### Part 10: Fine-Grained RBAC & Audit Logging

**Comparable to**: Milvus RBAC, Pinecone API keys

- [ ] **10.1** Define roles (admin, read-write, read-only, ingest-only)
- [ ] **10.2** Implement permission checks on dataset/namespace operations
- [ ] **10.3** Add API key management with scopes
- [ ] **10.4** Create comprehensive audit logging
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

#### Part 19: Semantic Query Cache

**Comparable to**: Query understanding, result reuse

- [ ] **19.1** Implement query embedding cache (LRU)
- [ ] **19.2** Add result caching with similarity-based invalidation
- [ ] **19.3** Create cache warming for popular queries
- [ ] **19.4** Add cache metrics (hit rate, latency improvement)
- [ ] **19.5** Implement distributed cache (Redis-compatible)

#### Part 20: Developer Experience - REMAINING WORK

| Task | Status |
|------|--------|
| 20.1 gRPC/Flight protocol | ✅ DONE |
| 20.2 Python client | ✅ DONE |
| 20.3 Example apps | 🔴 NOT DONE |
| 20.4 Benchmark playground | 🔴 NOT DONE |

---

## Quick Wins (Low Effort, High Impact)

1. **Semantic Cache** (Part 19) — Cache layer, immediate latency wins
2. **OpenTelemetry** (Part 12) — Existing telemetry package, needs tracing
3. **RBAC Enhancement** (Part 10) — Extend existing auth

---

## Architecture Notes

### Protocol Ports
- **3000**: Data Server (gRPC/Arrow Flight)
- **3001**: Meta Server (gRPC/Arrow Flight)
- **9090**: Prometheus metrics

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

*Last Updated: 2026-03-31*
