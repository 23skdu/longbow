# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-05
**Purpose**: Create competitive features comparable to Pinecone, Milvus, Qdrant, Weaviate

---

## 🎯 Remaining Tasks

### HIGH PRIORITY

_(No high priority items - all completed)_

---

### MEDIUM PRIORITY

_(No medium priority items - all completed)_

---

### LOW PRIORITY

#### Part 16: Learned Indexes (ML-Based Index Selection)

**Comparable to**: Emerging research

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 16.1 | Create index performance predictor | ✅ DONE | ML model for index selection - internal/store/learned_index.go |
| 16.2 | Implement query → index mapping | ✅ DONE | Choose HNSW vs IVF-PQ per query - internal/store/learned_index.go |
| 16.3 | Add runtime index adaptation | ✅ DONE | Rebuild with better params - internal/store/learned_index.go |
| 16.4 | Benchmark learned vs fixed selection | ⬜ TODO | Performance comparison |
| 16.5 | Add index recommendation API | ⬜ TODO | API for index suggestions |

#### Part 17: Streaming & Real-Time Updates

**Comparable to**: Change streams

| Step | Task | Status | Implementation Notes |
|------|------|--------|----------------------|
| 17.1 | Implement CDC for vector ops | ✅ DONE | Change data capture - internal/store/cdc.go with metrics & config |
| 17.2 | Create WebSocket subscription | ✅ DONE | Real-time updates - internal/store/websocket.go |
| 17.3 | Add Kafka/Pulsar export | ✅ DONE | Event-driven pipelines - internal/store/mq_exporter.go |
| 17.4 | Optimistic concurrent updates | ✅ DONE | Concurrent vector updates - internal/store/optimistic_update.go |
| 17.5 | Add streaming aggregation | ✅ DONE | Moving average vectors - internal/store/streaming_aggregation.go |

---

## Recently Completed (2026-04-05)

| Feature | Notes |
|---------|-------|
| Part 1: Serverless Auto-Scaling (1.1-1.5) | Auto-scaler, worker pools, admission control, tiered storage, capacity APIs |
| Part 4.1-4.5: Built-in Vectorization | Embedding interface, local model, batch processing, external providers, model versioning |
| Part 7.1-7.5: Disk-Based Indexing | DiskANN, Vamana graph, beam search, tiered storage, I/O scheduling |
| Part 10.1-10.5: RBAC & Audit | Roles, permissions, API keys, audit logging, SSO/OAuth |
| Part 13.1-13.5: Geo-Spatial Search | Geo-point type, Haversine distance, geo-bounded search, quadtree index, hybrid search |
| Part 14.1-14.5: Time-Travel & Temporal | Timestamp metadata, temporal index, as-of queries, sliding window, delete-by-time |
| Part 18.1-18.5: Federated Search | Collection registry, query router, RRF merging, tag-based routing, benchmark |
| Memory Leak Detection | pprof integration, leak detector, memory snapshots, goroutine tracking |
| Build fix: capacity_plan.go | Removed broken stub code, fixed build |

---

## Architecture Notes

### New Components Added
| Component | File |
|-----------|------|
| Embedding Model Versioning | `internal/store/embedding_generator.go` |
| Disk I/O Scheduler | `internal/store/disk_io_scheduler.go` |
| Geo-Spatial Search | `internal/store/geo_search.go` |
| Temporal Search | `internal/store/temporal_search.go` |
| Federated Search | `internal/store/federated_search.go` |
| Memory Leak Detection | `internal/profiling/memory_leak.go` |
| Change Data Capture (CDC) | `internal/store/cdc.go` |
| WebSocket Server | `internal/store/websocket.go` |
| Message Queue Exporter (Kafka/Pulsar) | `internal/store/mq_exporter.go` |
| Optimistic Concurrent Updates | `internal/store/optimistic_update.go` |
| Streaming Aggregation | `internal/store/streaming_aggregation.go` |
| Learned Index Performance Predictor | `internal/store/learned_index.go` |

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
