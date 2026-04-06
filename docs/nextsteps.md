# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-05
**Purpose**: Create competitive features comparable to Pinecone, Milvus, Qdrant, Weaviate

---

## 🎯 Remaining Tasks

### HIGH PRIORITY

#### Part 1: Serverless Auto-Scaling

**Comparable to**: Pinecone serverless, LanceDB embedded

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 1.1 | Create auto-scaler component | ✅ DONE | `internal/autoscale/scaler.go` monitors QPS/latency |
| 1.2 | Dynamic worker pool sizing | ✅ DONE | `AdjustWorkerCounts` for indexing/ingestion workers |
| 1.3 | Memory-based admission control | ✅ DONE | Backpressure signals via `internal/store/admission.go` |
| 1.4 | Tiered storage triggers | ✅ DONE | Hot→warm→cold based on access patterns |
| 1.5 | API endpoints for capacity planning | ✅ DONE | Added `GetCapacityPlan`, `GetAutoScaleConfig`, `SetAutoScaleConfig` to MetaServer DoAction |

---

### MEDIUM PRIORITY

#### Part 4: Built-in Vectorization Modules

**Comparable to**: Weaviate text2vec, Cohere integration

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 4.1 | Create embedding generation interface | ✅ DONE | `EmbeddingGenerator` interface |
| 4.2 | Implement local embedding model | ✅ DONE | `localEmbeddingGenerator` with stub/onnx/wasm |
| 4.3 | Batch embedding generation | ✅ DONE | Batch processing with configurable batch size |
| 4.4 | Support external providers | ✅ DONE | OpenAI (`text-embedding-3-small`), Cohere (`embed-english-v3.0`), HuggingFace (`sentence-transformers/all-MiniLM-L6-v2`) |
| 4.5 | **Embedding model versioning** | ⬜ TODO | Caching and model management |

#### Part 7: Disk-Based Indexing

**Comparable to**: LanceDB disk-based, Milvus DiskANN

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 7.1 | DiskANN index implementation | ✅ DONE | `internal/store/diskann.go` |
| 7.2 | Vamana graph construction | ✅ DONE | Graph construction |
| 7.3 | Beam search with pruning | ✅ DONE | Search with pruning |
| 7.4 | Hybrid RAM+disk tiered storage | ✅ DONE | Hot→warm→cold tiered storage |
| 7.5 | **I/O scheduling for disk-based search** | ⬜ TODO | Optimize disk I/O for search |

#### Part 10: Fine-Grained RBAC & Audit Logging

**Comparable to**: Milvus RBAC, Pinecone API keys

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 10.1 | Define roles | ✅ DONE | `RoleAdmin`, `RoleReadWrite`, `RoleReadOnly`, `RoleIngest` |
| 10.2 | Permission checks | ✅ DONE | `CheckPermission` with namespace/dataset scoping |
| 10.3 | API key management | ✅ DONE | `CreateAPIKey`, `ValidateAPIKey`, `RevokeAPIKey`, `DeleteAPIKey` |
| 10.4 | Audit logging | ✅ DONE | `internal/security/audit.go` |
| 10.5 | **SSO/OAuth** | ⬜ TODO | Not started |

---

### LOW PRIORITY

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

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 14.1 | Add timestamp metadata to all vectors | ⬜ TODO | Vector timestamp tracking |
| 14.2 | Implement temporal index | ⬜ TODO | Fast time-range queries |
| 14.3 | Create "as-of" queries | ⬜ TODO | What did this vector look like at time T |
| 14.4 | Add sliding window search | ⬜ TODO | Last N time units |
| 14.5 | Implement delete-by-time | ⬜ TODO | Tombstones with TTL |

#### Part 16: Learned Indexes (ML-Based Index Selection)

**Comparable to**: Emerging research

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 16.1 | Create index performance predictor | ⬜ TODO | ML model for index selection |
| 16.2 | Implement query → index mapping | ⬜ TODO | Choose HNSW vs IVF-PQ per query |
| 16.3 | Add runtime index adaptation | ⬜ TODO | Rebuild with better params |
| 16.4 | Benchmark learned vs fixed selection | ⬜ TODO | Performance comparison |
| 16.5 | Add index recommendation API | ⬜ TODO | API for index suggestions |

#### Part 17: Streaming & Real-Time Updates

**Comparable to**: Change streams

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 17.1 | Implement CDC for vector ops | ⬜ TODO | Change data capture |
| 17.2 | Create WebSocket subscription | ⬜ TODO | Real-time updates |
| 17.3 | Add Kafka/Pulsar export | ⬜ TODO | Event-driven pipelines |
| 17.4 | Optimistic concurrent updates | ⬜ TODO | Concurrent vector updates |
| 17.5 | Add streaming aggregation | ⬜ TODO | Moving average vectors |

#### Part 18: Federated Search (Cross-Collection)

**Comparable to**: Cross-index queries

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 18.1 | Collection/dataset registry | ⬜ TODO | Discovery |
| 18.2 | Federated query router | ⬜ TODO | Route queries across collections |
| 18.3 | Cross-collection result merging | ⬜ TODO | RRF (Reciprocal Rank Fusion) |
| 18.4 | Add collection routing rules | ⬜ TODO | Tag-based routing |
| 18.5 | Benchmark federated vs single | ⬜ TODO | Performance comparison |

---

## Recently Completed

| Feature | Date | Notes |
|---------|------|-------|
| Part 1.5: Capacity Planning APIs | 2026-04-05 | Added `GetCapacityPlan`, `GetAutoScaleConfig`, `SetAutoScaleConfig` to MetaServer |
| Part 4.4: External Embedding Providers | 2026-04-05 | Full OpenAI, Cohere, HuggingFace integration |
| Build fix: capacity_plan.go | 2026-04-05 | Removed broken stub code, fixed build |

---

## Build Issues Fixed

| Issue | Status |
|-------|--------|
| `capacity_plan.go` - undefined `Action`, `Result`, `FlightService_DoActionServer` | ✅ FIXED |
| Missing capacity planning API endpoints | ✅ FIXED |

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
