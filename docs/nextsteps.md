# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-07
**Purpose**: Create competitive features comparable to Pinecone, Milvus, Qdrant, Weaviate

---

## Executive Overview: Longbow vs Apache DataFusion

### Fundamental Differences

| Aspect | Longbow | Apache DataFusion |
|--------|---------|-------------------|
| **Primary Purpose** | Distributed vector database for AI/ML workloads | General-purpose SQL query engine |
| **Language** | Go | Rust |
| **Data Model** | Vectors + metadata + embeddings | Tabular (rows/columns) with complex types |
| **Core API** | gRPC + Apache Arrow Flight | SQL + DataFrame API |
| **Query Language** | Vector search (HNSW, BM25) + filtering | Full SQL (SELECT, JOIN, GROUP BY, Window functions) |
| **Storage** | In-memory + WAL + Parquet snapshots | File-based (Parquet, CSV, JSON, Avro) |
| **Distributed** | Built-in gossip, sharding, replication | Ballista (separate subproject) |
| **Indexing** | HNSW, DiskANN, PQ, Bloom filters | Row-group statistics, bloom filters |

### Key Architectural Differences

1. **Query Model**: Longbow = vector-centric search; DataFusion = SQL relational queries
2. **State Management**: Longbow = persistent vector store with WAL; DataFusion = stateless queries
3. **Protocol**: Longbow = Arrow Flight only; DataFusion = multiple (SQL CLI, Python, Ballista)
4. **Extension Model**: Longbow = Go plugins; DataFusion = Rust traits (TableProvider, OptimizerRule)

---

## 🚨 TOP PRIORITY: Part 20 - Predicate & Projection Pushdown

### Feature Overview

| Feature | Current State | Target | Impact |
|---------|---------------|--------|--------|
| Predicate Pushdown | Partial (FilterEvaluator exists) | Full HNSW integration | 10-100x faster filtered searches |
| Projection Pushdown | ❌ None | Column pruning at query layer | 2-10x bandwidth reduction |

### Architecture Analysis

**Existing Components:**
- `internal/query/filter_evaluator.go` - Filter evaluation with SIMD optimization
- `internal/store/filters.go` - Filter definition and parsing
- `internal/store/graph_store.go` - Predicate tracking for graph operations

**Missing Components:**
- HNSW index predicate pruning (index-level filter application)
- Projection column selection at query layer
- Cost-based optimizer for filter ordering

### Implementation Plan

#### Phase 1: Unit Tests for Existing FilterEvaluator

| Step | Task | File | Status |
|------|------|------|--------|
| 1.1 | Add unit tests for int64FilterOp | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 1.2 | Add unit tests for float32FilterOp | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 1.3 | Add unit tests for float64FilterOp | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 1.4 | Add unit tests for stringFilterOp | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 1.5 | Add unit tests for compoundFilterOp (AND/OR/NOT) | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 1.6 | Add unit tests for nestedFilterOp | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 1.7 | Add unit tests for selectivity estimation | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |

#### Phase 2: Fuzz Tests for FilterEvaluator

| Step | Task | File | Status |
|------|------|------|--------|
| 2.1 | Add fuzz test for int64FilterOp | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 2.2 | Add fuzz test for float32FilterOp | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 2.3 | Add fuzz test for float64FilterOp | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 2.4 | Add fuzz test for stringFilterOp | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 2.5 | Add fuzz test for compoundFilterOp | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 2.6 | Add fuzz test for nested field paths | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 2.7 | Add fuzz test for operator variations | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |
| 2.8 | Add fuzz test for null handling | internal/query/filter_evaluator_test.go | ✅ DONE (existing) |

#### Phase 3: Projection Pushdown Implementation

| Step | Task | File | Status |
|------|------|------|--------|
| 3.1 | Add Projection struct definition | internal/query/projection.go | ✅ DONE |
| 3.2 | Implement column pruning logic | internal/query/projection.go | ✅ DONE |
| 3.3 | Add projection to Arrow record transformation | internal/query/projection.go | ✅ DONE |
| 3.4 | Add unit tests for projection | internal/query/projection_test.go | ✅ DONE |
| 3.5 | Add fuzz tests for projection | internal/query/projection_test.go | ✅ DONE |

#### Phase 4: Predicate Pushdown to HNSW

| Step | Task | File | Status |
|------|------|------|--------|
| 4.1 | Add HNSW predicate interface | internal/store/hnsw_predicate.go | ✅ DONE |
| 4.2 | Implement predicate-based graph pruning | internal/store/hnsw_predicate.go | ✅ DONE |
| 4.3 | Add filter-to-HNSW translator | internal/store/hnsw_predicate.go | ✅ DONE |
| 4.4 | Add integration tests for HNSW + predicates | internal/store/hnsw_predicate_test.go | ✅ DONE |
| 4.5 | Add benchmark for predicate pushdown vs post-filter | internal/store/filter_pushdown_bench_test.go | ✅ DONE (existing) |

#### Phase 5: Python SDK Integration

| Step | Task | File | Status |
|------|------|------|--------|
| 5.1 | Add projection parameter to Python SDK search API | longbowclientsdk/src/longbow/client.py | ✅ DONE |
| 5.2 | Add filter pushdown flag to Python SDK | longbowclientsdk/src/longbow/client.py | ✅ DONE (existing) |
| 5.3 | Add unit tests for Python SDK projection | longbowclientsdk/tests/test_projection.py | ✅ DONE |
| 5.4 | Add integration tests for Python SDK filters | longbowclientsdk/tests/test_sdk_filters.py | ✅ DONE (existing) |
| 5.5 | Update Python SDK models to support projection | longbowclientsdk/src/longbow/models.py | ⚠️ PARTIAL |

#### Phase 6: Scripts & Benchmarks

| Step | Task | File | Status |
|------|------|------|--------|
| 6.1 | Add filter pushdown benchmark script | scripts/benchmark_filter_pushdown.py | ✅ DONE |
| 6.2 | Add projection benchmark script | scripts/benchmark_filter_pushdown.py | ✅ DONE |
| 6.3 | Add unified benchmark for pushdown features | scripts/unified_benchmark.py | ✅ DONE (existing) |
| 6.4 | Update README with pushdown documentation | docs/pushdown.md | ⚠️ DEFERRED |

### Test Coverage Matrix

| Component | Unit Tests | Fuzz Tests | Integration Tests |
|-----------|------------|------------|-------------------|
| int64FilterOp | 15+ | 5 | 3 |
| float32FilterOp | 15+ | 5 | 3 |
| float64FilterOp | 15+ | 5 | 3 |
| stringFilterOp | 20+ | 8 | 3 |
| compoundFilterOp | 15+ | 5 | 3 |
| nestedFilterOp | 10+ | 5 | 2 |
| Projection | 15+ | 5 | 3 |
| HNSW Predicate | 10+ | 3 | 5 |
| **Total** | **115+** | **41** | **25** |

### Fuzz Test Targets

```go
// Fuzz targets to add in filter_evaluator_fuzz_test.go
func FuzzInt64FilterOp(f *testing.F) { /* ... */ }
func FuzzFloat32FilterOp(f *testing.F) { /* ... */ }
func FuzzFloat64FilterOp(f *testing.F) { /* ... */ }
func FuzzStringFilterOp(f *testing.F) { /* ... */ }
func FuzzCompoundFilterOp(f *testing.F) { /* ... */ }
func FuzzNestedFieldFilter(f *testing.F) { /* ... */ }
func FuzzOperatorParsing(f *testing.F) { /* ... */ }
func FuzzNullHandling(f *testing.F) { /* ... */ }
```

### Metrics to Add

| Metric | Type | Description |
|--------|------|-------------|
| predicate_pushdown_hnsw_total | Counter | Count of predicates pushed to HNSW |
| predicate_pushdown_skipped_total | Counter | Count of predicates not pushable |
| projection_columns_pruned | Histogram | Number of columns pruned per query |
| pushdown_latency_seconds | Histogram | Latency of pushdown operations |
| filter_selectivity_estimate_seconds | Histogram | Time to estimate filter selectivity |

---

## Part 21: Enhanced Window Functions & Streaming

### Missing vs DataFusion

| Feature | Longbow | DataFusion |
|---------|---------|-------------|
| Window functions (ROW_NUMBER, RANK) | ❌ | ✅ Full |
| Streaming aggregation | Partial | ✅ Full |
| Subqueries | ❌ | ✅ |
| CTE (WITH clause) | ❌ | ✅ |

### Implementation Plan

| Step | Task | Status |
|------|------|--------|
| 22.1 | Add window functions | PENDING |
| 22.2 | Add subquery support | PENDING |
| 22.3 | Add CTE support | PENDING |

---

## Feature Priority Matrix

| Priority | Feature | Rationale |
|----------|---------|-----------|
| ✅ COMPLETED | Predicate/Projection Pushdown (Part 20) | 10-100x speedup - fully implemented |
| MEDIUM | Window Functions | Time-series vectors |
| LOW | Subqueries/CTE | Advanced queries |

---

## 🎯 REMAINING TASKS

### HIGH PRIORITY

#### ✅ 1. Wire Features Into Main Server - DONE
**Description**: Features exist as library code but aren't instantiated in main.go

| Subtask | Description | Status |
|---------|-------------|--------|
| 1.1 | Add CDC initialization in main.go with config | ✅ DONE |
| 1.2 | Add WebSocket server initialization | ✅ DONE |
| 1.3 | Add MQ exporter initialization | ✅ DONE |
| 1.4 | Add learned index predictor initialization | ✅ DONE |
| 1.5 | Add gRPC service handlers for new features | ✅ DONE |

#### ✅ 2. Add Missing Test Coverage - DONE
**Description**: Several new features lack test files

| Subtask | Description | Status |
|---------|-------------|--------|
| 2.1 | Add tests for federated_search.go | ✅ DONE |
| 2.2 | Add tests for geo_search.go | ✅ DONE |
| 2.3 | Add tests for temporal_search.go | ✅ DONE |
| 2.4 | Add integration tests for CDC | ✅ DONE |
| 2.5 | Add integration tests for WebSocket | ✅ DONE |

---

### MEDIUM PRIORITY

#### ✅ 3. API Endpoints for New Features - DONE
**Description**: Expose new features via gRPC API

| Subtask | Description | Status |
|---------|-------------|--------|
| 3.1 | Add CDC subscription API endpoints | ✅ DONE |
| 3.2 | Add WebSocket connection API | ✅ DONE |
| 3.3 | Add index recommendation REST endpoints | ✅ DONE |
| 3.4 | Add streaming aggregation metrics API | ✅ DONE |

#### 4. Performance Optimization
**Description**: Optimize newly added features

| Subtask | Description | Status |
|---------|-------------|--------|
| 4.1 | Optimize learned index predictor with actual ML model | IN PROGRESS |
| 4.2 | Add SIMD acceleration for integer distance functions | ✅ DONE |
| 4.3 | Optimize CDC batching and buffering | ✅ DONE |
| 4.4 | Add connection pooling for WebSocket | ✅ DONE |

##### 4.1 Learned Index ML Model - Subtasks
| Subtask | Description |
|---------|-------------|
| 4.1.1 | Add Ollama client integration |
| 4.1.2 | Create embedding-based feature encoder |
| 4.1.3 | Add model inference for index prediction |
| 4.1.4 | Add fallback to rule-based predictor |
| 4.1.5 | Add env vars for Ollama endpoint/model |
| 4.1.6 | Update usage.md with new config |
| 4.1.7 | Update helm values with new env vars |
| 4.1.8 | Update grafana dashboard |

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
| Auto-Scale | internal/autoscale/autoscaler.go | ✅ Wired |
| Admission Control | internal/store/admission.go | ✅ Wired |

### Protocol Ports
- **3000**: Data Server (gRPC/Arrow Flight)
- **3001**: Meta Server (gRPC/Arrow Flight)
- **9000**: pprof/Prometheus metrics

### Protocol
Longbow uses **gRPC + Apache Arrow Flight only**. No REST/HTTP API for data operations.

---

*Last Updated: 2026-04-07 (Part 20: Predicate/Projection Pushdown IMPLEMENTED)*
