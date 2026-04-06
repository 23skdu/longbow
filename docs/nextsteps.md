# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-06
**Purpose**: Create competitive features comparable to Pinecone, Milvus, Qdrant, Weaviate

---

## ✅ COMPLETED FEATURES (All Parts Done)

### Part 16: Learned Indexes (ML-Based Index Selection)
| Step | Task | Status | Implementation |
|------|------|--------|-----------------|
| 16.1 | Index performance predictor | ✅ DONE | internal/store/learned_index.go |
| 16.2 | Query → index mapping | ✅ DONE | internal/store/learned_index.go |
| 16.3 | Runtime index adaptation | ✅ DONE | internal/store/learned_index.go |
| 16.4 | Benchmark learned vs fixed | ✅ DONE | IndexBenchmark struct |
| 16.5 | Index recommendation API | ✅ DONE | IndexRecommendationAPI |

### Part 17: Streaming & Real-Time Updates
| Step | Task | Status | Implementation |
|------|------|--------|-----------------|
| 17.1 | CDC for vector ops | ✅ DONE | internal/store/cdc.go |
| 17.2 | WebSocket subscription | ✅ DONE | internal/store/websocket.go |
| 17.3 | Kafka/Pulsar export | ✅ DONE | internal/store/mq_exporter.go |
| 17.4 | Optimistic concurrent updates | ✅ DONE | internal/store/optimistic_update.go |
| 17.5 | Streaming aggregation | ✅ DONE | internal/store/streaming_aggregation.go |

### Previously Completed
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

---

## 🎯 REMAINING TASKS

### HIGH PRIORITY

#### 1. Wire Features Into Main Server
**Description**: Features exist as library code but aren't instantiated in main.go

| Subtask | Description |
|---------|-------------|
| 1.1 | Add CDC initialization in main.go with config |
| 1.2 | Add WebSocket server initialization |
| 1.3 | Add MQ exporter initialization |
| 1.4 | Add learned index predictor initialization |
| 1.5 | Add gRPC service handlers for new features |

#### 2. Add Missing Test Coverage
**Description**: Several new features lack test files

| Subtask | Description |
|---------|-------------|
| 2.1 | Add tests for federated_search.go |
| 2.2 | Add tests for geo_search.go |
| 2.3 | Add tests for temporal_search.go |
| 2.4 | Add integration tests for CDC |
| 2.5 | Add integration tests for WebSocket |

---

### MEDIUM PRIORITY

#### 3. API Endpoints for New Features
**Description**: Expose new features via gRPC API

| Subtask | Description |
|---------|-------------|
| 3.1 | Add CDC subscription API endpoints |
| 3.2 | Add WebSocket connection API |
| 3.3 | Add index recommendation REST endpoints |
| 3.4 | Add streaming aggregation metrics API |

#### 4. Performance Optimization
**Description**: Optimize newly added features

| Subtask | Description |
|---------|-------------|
| 4.1 | Optimize learned index predictor with actual ML model |
| 4.2 | Add SIMD acceleration for integer distance functions |
| 4.3 | Optimize CDC batching and buffering |
| 4.4 | Add connection pooling for WebSocket |

---

### LOW PRIORITY

#### 5. Documentation Updates
**Description**: Update user-facing documentation

| Subtask | Description |
|---------|-------------|
| 5.1 | Add API documentation for CDC |
| 5.2 | Add usage examples for learned indexes |
| 5.3 | Add migration guide for new features |

#### 6. Monitoring & Observability
**Description**: Add metrics and tracing for new features

| Subtask | Description |
|---------|-------------|
| 6.1 | Add Prometheus metrics for CDC |
| 6.2 | Add tracing for WebSocket operations |
| 6.3 | Add metrics for learned index predictor |
| 6.4 | Add health checks for streaming features |

---

## Architecture Notes

### New Components (Implemented)
| Component | File | Status |
|-----------|------|--------|
| CDC | internal/store/cdc.go | ✅ Wired |
| WebSocket Server | internal/store/websocket.go | ✅ Wired |
| MQ Exporter | internal/store/mq_exporter.go | ✅ Wired |
| Optimistic Updates | internal/store/optimistic_update.go | ✅ Wired |
| Streaming Aggregation | internal/store/streaming_aggregation.go | ✅ Wired |
| Learned Index | internal/store/learned_index.go | ✅ Wired |
| Geo-Spatial Search | internal/store/geo_search.go | ✅ Wired |
| Temporal Search | internal/store/temporal_search.go | ✅ Wired |
| Federated Search | internal/store/federated_search.go | ✅ Wired |
| Memory Leak Detection | internal/profiling/memory_leak.go | ✅ Wired |

### Protocol Ports
- **3000**: Data Server (gRPC/Arrow Flight)
- **3001**: Meta Server (gRPC/Arrow Flight)
- **9000**: pprof/Prometheus metrics

### Protocol
Longbow uses **gRPC + Apache Arrow Flight only**. No REST/HTTP API for data operations.

---

*Last Updated: 2026-04-06*
