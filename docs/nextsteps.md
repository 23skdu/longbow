# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-05
**Purpose**: Create competitive features comparable to Pinecone, Milvus, Qdrant, Weaviate

---

## 🎯 Remaining Tasks

### HIGH PRIORITY

#### Part 2: Enhanced Multi-Tenancy with Strict Isolation

**Comparable to**: Pinecone namespaces, Milvus partition key

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 2.1 | ~~Add tenant resource quotas~~ | ✅ DONE | Added MaxVectors, MaxDimensions, MaxStorageBytes to Namespace |
| 2.2 | ~~Implement tenant-specific caching~~ | ✅ DONE | Added NamespaceCacheManager for per-namespace cache isolation |
| 2.3 | ~~Add tenant-aware metrics~~ | ✅ DONE | Added NamespaceQPS, NamespaceLatency, NamespaceStorageBytes, etc. |
| 2.4 | ~~Create tenant-level rate limiting~~ | ✅ DONE | Added RateLimiterManager with per-namespace limits |
| 2.5 | ~~Add tenant migration APIs~~ | ✅ DONE | Added MigrateNamespace, ExportDataset, ImportDataset, CloneDataset |

#### Part 8: Automatic Data Versioning

**Comparable to**: LanceDB automatic versioning

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 8.1 | Add version metadata | 🔶 START | Add timestamp/version to vector records |
| 8.2 | Implement time-travel queries | ⬜ TODO | Query historical state |
| 8.3 | Create version retention policies | ⬜ TODO | Auto-expire old versions |
| 8.4 | Add branch/merge semantics | ⬜ TODO | Experimental dataset branches |
| 8.5 | API for listing versions | ⬜ TODO | Version comparison API |

#### Part 9: Enterprise Backup & Disaster Recovery

**Comparable to**: Pinecone snapshots, Milvus backup

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 9.1 | Add incremental backup | 🔶 START | Capture WAL deltas |
| 9.2 | Cross-region replication | ⬜ TODO | Disaster recovery |
| 9.3 | Backup verification | ⬜ TODO | Checksum validation |
| 9.4 | Point-in-time recovery | ⬜ TODO | Recovery API |
| 9.5 | Backup scheduling | ⬜ TODO | Retention policies |

#### Part 10: Fine-Grained RBAC & Audit Logging

**Comparable to**: Milvus RBAC, Pinecone API keys

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 10.1 | Define roles | ⬜ TODO | admin, read-write, read-only, ingest-only |
| 10.2 | Permission checks | ⬜ TODO | Dataset/namespace operations |
| 10.3 | API key management | ⬜ TODO | Scoped API keys |
| 10.4 | Audit logging | ✅ DONE | `internal/security/audit.go` |
| 10.5 | SSO/OAuth | ⬜ TODO | Not started |

---

### MEDIUM PRIORITY

#### Part 1: Serverless Auto-Scaling

**Comparable to**: Pinecone serverless, LanceDB embedded

- [x] **1.1** Create auto-scaler component that monitors query QPS and latency
- [x] **1.2** Implement dynamic worker pool sizing (ingestion workers, search threads)
- [x] **1.3** Add memory-based admission control with backpressure signals
- [x] **1.4** Design tiered storage triggers (hot → warm → cold based on access patterns)
- [ ] **1.5** Add API endpoints for capacity planning and auto-scale configuration

#### Part 4: Built-in Vectorization Modules

**Comparable to**: Weaviate text2vec, Cohere integration

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 4.1 | ~~Create embedding generation interface~~ | ✅ DONE | Add EmbeddingGenerator interface |
| 4.2 | ~~Implement local embedding model~~ | ✅ DONE | Added localEmbeddingGenerator with stub/onnx/wasm support |
| 4.3 | ~~Add batch embedding generation~~ | ✅ DONE | Batch processing with configurable batch size |
| 4.4 | Support external providers | 🔶 STUB | OpenAI, Cohere, HuggingFace stubs ready for API integration |
| 4.5 | Add embedding model versioning | ⬜ TODO | Caching and model management |

#### Part 7: Disk-Based Indexing

**Comparable to**: LanceDB disk-based, Milvus DiskANN

- [x] **7.1** DiskANN index implementation (`internal/store/diskann.go`)
- [x] **7.2** Vamana graph construction
- [x] **7.3** Beam search with pruning
- [x] **7.4** Hybrid RAM+disk tiered storage (hot → warm → cold)
- [ ] **7.5** I/O scheduling for disk-based search

#### Part 13: Geo-Spatial Search

**Comparable to**: Qdrant geo filters

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 13.1 | Add geo-point vector type | ⬜ TODO | lat, lon as vector |
| 13.2 | Implement geo-distance | ⬜ TODO | Haversine, approximate |
| 13.3 | Geo-bounded search | ⬜ TODO | Within radius, polygon |
| 13.4 | Add geo-index | ⬜ TODO | Fast filtering |
| 13.5 | Combine geo + vector | ⬜ TODO | Hybrid search |

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

---

## Architecture Notes

### Protocol Ports
- **3000**: Data Server (gRPC/Arrow Flight)
- **3001**: Meta Server (gRPC/Arrow Flight)
- **9000**: pprof/Prometheus metrics (reassigned from 9090)

### Protocol
Longbow uses **gRPC + Apache Arrow Flight only**. No REST/HTTP API for data operations.

### Existing Components
| Component | Files |
|-----------|-------|
| Core Vector Store | `internal/store/arrow_hnsw.go`, `internal/store/sharded_hnsw.go` |
| Storage Backends | `internal/store/disk_vector_store.go`, `internal/store/mem_vector_store.go` |
| Index Types | `internal/store/ivf_pq_index.go`, `internal/store/turboquant.go` |
| Distributed | `internal/sharding/ring.go`, `internal/mesh/gossip.go` |
| Search | `internal/store/global_search.go`, `internal/store/hybrid_search.go` |
| Metrics | `internal/metrics/`, `internal/telemetry/` |
| Security | `internal/security/audit.go`, `internal/security/auth.go` |
| Python SDK | `longbowclientsdk/` |
| Docker | `docker-compose.yml`, `Dockerfile*` |

---

*Last Updated: 2026-04-05*
