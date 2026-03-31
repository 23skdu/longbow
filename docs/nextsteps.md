# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-03-30
**Purpose**: Create competitive features comparable to Pinecone, Milvus, Qdrant, Weaviate

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

*Last Updated: 2026-03-30*
