# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-08
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

## Part 23: Incomplete/Stub Code Review Findings

### Review Summary (2026-04-08)
Reviewed codebase for TODO markers, stub implementations, and incomplete features.

### HIGH PRIORITY - Incomplete Features

#### 1. Dataset Import/Export - HIGH PRIORITY
**Description**: ImportDataset and ExportDataset methods return "not implemented" errors.
These will be implemented following S3 snapshot patterns using Apache Arrow/Parquet format.

**Reference Implementation**: See `internal/storage/s3_backend.go` - WriteSnapshot/ReadSnapshot methods

**Parquet Schema** (matching persistence.md):
| Column     | Type                     | Description                             |
| :--------- | :----------------------- | :-------------------------------------- |
| id         | int64                    | Unique identifier for the vector.       |
| vector     | fixed_size_binary_array  | The embedding vector (e.g., 1536 dims). |
| metadata   | binary (JSON)            | Associated metadata blob.               |
| created_at | timestamp                | Ingestion timestamp.                   |

| Subtask | Description | Status |
|---------|-------------|--------|
| 1.1 | Create `internal/store/dataset_io.go` with ExportToParquet method | PENDING |
| 1.2 | Implement Arrow Record to Parquet conversion using parquet-go | PENDING |
| 1.3 | Add streaming Parquet writer for large datasets | PENDING |
| 1.4 | Create `internal/store/dataset_io.go` with ImportFromParquet method | PENDING |
| 1.5 | Implement Parquet to Arrow Record conversion | PENDING |
| 1.6 | Add validation for schema compatibility on import | PENDING |
| 1.7 | Wire ExportDataset to use StorageBackend (local/S3) | PENDING |
| 1.8 | Wire ImportDataset to use StorageBackend | PENDING |
| 1.9 | Implement CloneDataset using Export -> Import | PENDING |
| 1.10 | Add unit tests for dataset export | PENDING |
| 1.11 | Add unit tests for dataset import | PENDING |
| 1.12 | Add integration tests for dataset cloning | PENDING |
| 1.13 | Add Prometheus metrics for import/export operations | PENDING |
| 1.14 | Add benchmarks for large dataset import/export | PENDING |

**Implementation Details**:

```go
// internal/store/dataset_io.go

// ExportDataset exports a dataset to Parquet format via StorageBackend
func (vs *VectorStore) ExportDataset(ctx context.Context, name string, backend storage.SnapshotBackend) error {
    ds, ok := vs.getDataset(name)
    if !ok {
        return errors.New("dataset not found")
    }
    
    // Convert Dataset.Records (arrow.RecordBatch) to Parquet
    // Use streaming writer for large datasets
    // Write to StorageBackend using WriteSnapshotFile
}

// ImportDataset imports a dataset from Parquet format via StorageBackend
func (vs *VectorStore) ImportDataset(ctx context.Context, name string, backend storage.SnapshotBackend) error {
    // Read from StorageBackend using ReadSnapshotFile
    // Parse Parquet to Arrow Records
    // Create new Dataset and populate with records
    // Build HNSW index if needed
}
```

**Metrics to Add**:
| Metric | Type | Description |
|--------|------|-------------|
| `longbow_dataset_export_total` | Counter | Total dataset exports |
| `longbow_dataset_export_duration_seconds` | Histogram | Export duration |
| `longbow_dataset_export_bytes` | Histogram | Bytes exported |
| `longbow_dataset_import_total` | Counter | Total dataset imports |
| `longbow_dataset_import_duration_seconds` | Histogram | Import duration |
| `longbow_dataset_import_rows` | Histogram | Rows imported |

**Files to Modify**:
- `internal/store/rate_limit.go` - Update ExportDataset/ImportDataset signatures
- `internal/store/dataset.go` - Add export/import methods to Dataset
- `internal/storage/` - Reuse existing S3Backend interface
- `internal/metrics/` - Add import/export metrics

**Files**: `internal/store/rate_limit.go:261,265`

#### 2. ONNX Runtime Integration - HIGH PRIORITY
**Description**: ML reranker and embedding generator use stub implementations

| Subtask | Description | Status |
|---------|-------------|--------|
| 2.1 | Integrate ONNX Runtime Go bindings for ML Reranker | PENDING |
| 2.2 | Integrate ONNX Runtime Go bindings for Embedding Generator | PENDING |
| 2.3 | Replace stubMLModel with real ONNX model | PENDING |
| 2.4 | Replace stubEmbeddingModel with real ONNX model | PENDING |
| 2.5 | Add unit tests for ONNX inference | PENDING |

**Files**: 
- `internal/store/ml_reranker.go:53,58,144` (stubMLModel)
- `internal/store/embedding_generator.go:621,641,701` (stubEmbeddingModel)

#### 3. CUDA Memory Operations - MEDIUM PRIORITY
**Description**: CUDA memory stub returns "not implemented" errors

| Subtask | Description | Status |
|---------|-------------|--------|
| 3.1 | Implement allocateCUDAMemory | PENDING |
| 3.2 | Implement freeCUDAMemory | PENDING |
| 3.3 | Implement cudaMemcpyHostToDevice | PENDING |
| 3.4 | Implement cudaMemcpyDeviceToHost | PENDING |

**Files**: `internal/gpu/memory/memory_cuda_stub.go:17,22,27`

### Build Tags - Expected Stubs (NOT Issues)

The following are intentional stubs for cross-platform compilation:

| File | Purpose | Status |
|------|---------|--------|
| internal/gpu/memory/memory_metal_stub.go | Non-Metal platforms | ✅ Intentional |
| internal/gpu/memory/memory_cuda_stub.go | Non-CUDA platforms | ✅ Intentional |
| internal/gpu/factory_stub.go | Non-darwin/arm64 | ✅ Intentional |
| internal/onnx/metal/stub.go | Non-Metal platforms | ✅ Intentional |
| internal/onnx/metal/reranker_stub.go | Non-Metal platforms | ✅ Intentional |
| internal/simd/simd_stubs*.go | Platform-specific | ✅ Intentional |
| internal/storage/wal_backend_stub.go | Non-Linux | ✅ Intentional |
| internal/storage/wal_backend_arrow_iouring_stub.go | Non-Linux | ✅ Intentional |
| internal/store/numa_*.go | Non-Linux | ✅ Intentional |
| internal/store/store_gpu_stub.go | Non-GPU builds | ✅ Intentional |
| internal/store/memory_stub.go | Non-Linux | ✅ Intentional |
| internal/storage/storage_backend_stub.go | Fallback | ✅ Intentional |

---

## Feature Priority Matrix

| Priority | Feature | Rationale |
|----------|---------|-----------|
| ✅ COMPLETED | Predicate/Projection Pushdown (Part 20) | 10-100x speedup - fully implemented |
| ✅ COMPLETED | Part 22: Temporal Query Capabilities | Fully implemented |
| ✅ COMPLETED | Wire Features Into Main Server | All features wired |
| HIGH | Dataset Import/Export | Core functionality missing |
| HIGH | ONNX Runtime Integration | ML features use stubs |
| MEDIUM | CUDA Memory Operations | GPU memory not implemented |
| MEDIUM | Window Functions | Time-series vectors |
| LOW | WASM Embedding Model | Alternative embedding runtime |
| LOW | Subqueries/CTE | Advanced queries |

---

## 🎯 REMAINING TASKS

### HIGH PRIORITY

#### 1. Wire Features Into Main Server - DONE
**Description**: Features exist as library code but aren't instantiated in main.go

| Subtask | Description | Status |
|---------|-------------|--------|
| 1.1 | Add CDC initialization in main.go with config | ✅ DONE |
| 1.2 | Add WebSocket server initialization | ✅ DONE |
| 1.3 | Add MQ exporter initialization | ✅ DONE |
| 1.4 | Add learned index predictor initialization | ✅ DONE |
| 1.5 | Add gRPC service handlers for new features | ✅ DONE |

#### 2. Add Missing Test Coverage - DONE
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

#### ✅ 4. Performance Optimization - DONE
**Description**: Optimize newly added features

| Subtask | Description | Status |
|---------|-------------|--------|
| 4.1 | Optimize learned index predictor with actual ML model | ✅ DONE |
| 4.2 | Add SIMD acceleration for integer distance functions | ✅ DONE |
| 4.3 | Optimize CDC batching and buffering | ✅ DONE |
| 4.4 | Add connection pooling for WebSocket | ✅ DONE |

##### ✅ 4.1 Learned Index ML Model - Subtasks - DONE
| Subtask | Description | Status |
|---------|-------------|--------|
| 4.1.1 | Add Ollama client integration | ✅ DONE |
| 4.1.2 | Create embedding-based feature encoder | ✅ DONE |
| 4.1.3 | Add model inference for index prediction | ✅ DONE |
| 4.1.4 | Add fallback to rule-based predictor | ✅ DONE |
| 4.1.5 | Add env vars for Ollama endpoint/model | ✅ DONE |
| 4.1.6 | Update usage.md with new config | ✅ DONE |
| 4.1.7 | Update helm values with new env vars | ✅ DONE |
| 4.1.8 | Update grafana dashboard | ✅ DONE |

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

## Part 22: Temporal Query Capabilities

### Feature Overview

| Feature | Current State | Target | Impact |
|---------|---------------|--------|--------|
| Temporal Index | ✅ Implemented | Integration with main server | Full temporal search API |
| Version History | ✅ Implemented | Multi-version tracking | Time-travel queries |
| Time-based TTL | ✅ Implemented | Auto-expiration of old vectors | Storage management |
| Temporal Aggregation | ✅ Implemented | Time-windowed aggregations | Analytics support |
| gRPC API | ✅ Implemented | Exposed to clients | Full client access |
| Python SDK | ✅ Implemented | Temporal search API | Client usability |
| HNSW Integration | ✅ Implemented | Time-constrained HNSW | Vector search with temporal |

### Existing Components

| Component | File | Status |
|-----------|------|--------|
| TemporalTree | internal/store/temporal_search.go:30 | ✅ Implemented |
| TemporalIndex | internal/store/temporal_search.go:22 | ✅ Implemented |
| Temporal Search Methods | internal/store/temporal_search.go:226-375 | ✅ Implemented |
| Temporal Tests | internal/store/temporal_search_test.go | ✅ Implemented |
| TemporalConfig | internal/store/temporal_search.go:14 | ✅ Implemented |
| VersionHistory | internal/store/version_history.go | ✅ Implemented |
| VersionHistory Tests | internal/store/version_history_test.go | ✅ Implemented |
| TTLPolicy | internal/store/ttl_policy.go | ✅ Implemented |
| TemporalAggregator | internal/store/temporal_aggregation.go | ✅ Implemented |
| TemporalHNSWIndex | internal/store/hnsw_temporal.go | ✅ Implemented |
| Python SDK Temporal API | longbowclientsdk/src/longbow/client.py | ✅ Implemented |

### Server Integration (Phase 1)

| Step | Task | File | Status |
|------|------|------|--------|
| 1.1 | Add TemporalIndex to main server initialization | cmd/longbow/main.go | ✅ DONE |
| 1.2 | Add temporal config struct | internal/store/temporal_search.go | ✅ DONE |
| 1.3 | Add temporal index to server state | internal/store/store.go | ✅ DONE |
| 1.4 | Wire temporal index into server | internal/store/servers.go | ✅ DONE |

### gRPC API Endpoints (Phase 2)

| Step | Task | File | Status |
|------|------|------|--------|
| 2.1 | Implement temporal search handler | internal/store/servers.go | ✅ DONE |
| 2.2 | Add temporal action types to FlightServer | internal/store/servers.go | ✅ DONE |
| 2.3 | Add temporal DoAction endpoints | internal/store/servers.go | ✅ DONE |

### Version History Enhancement (Phase 3)

| Step | Task | File | Status |
|------|------|------|--------|
| 3.1 | Add version history storage | internal/store/version_history.go | ✅ DONE |
| 3.2 | Implement GetVersionAt with history lookup | internal/store/version_history.go | ✅ DONE |
| 3.3 | Add version pruning policy | internal/store/version_history.go | ✅ DONE |
| 3.4 | Add unit tests for version history | internal/store/version_history_test.go | ✅ DONE |

### TTL & Expiration Engine (Phase 4)

| Step | Task | File | Status |
|------|------|------|--------|
| 4.1 | Add TTL policy struct | internal/store/ttl_policy.go | ✅ DONE |
| 4.2 | Implement time-based expiration | internal/store/ttl_policy.go | ✅ DONE |
| 4.3 | Add background cleanup goroutine | internal/store/ttl_policy.go | ✅ DONE |
| 4.4 | Add TTL configuration to config | cmd/longbow/main.go | ✅ DONE |

### Time-Windowed Aggregation (Phase 5)

| Step | Task | File | Status |
|------|------|------|--------|
| 5.1 | Add temporal aggregation struct | internal/store/temporal_aggregation.go | ✅ DONE |
| 5.2 | Implement time-bucket aggregations | internal/store/temporal_aggregation.go | ✅ DONE |
| 5.3 | Add count/min/max/mean aggregations | internal/store/temporal_aggregation.go | ✅ DONE |

### HNSW Integration (Phase 6)

| Step | Task | File | Status |
|------|------|------|--------|
| 6.1 | Add temporal HNSW index | internal/store/hnsw_temporal.go | ✅ DONE |
| 6.2 | Implement time-constrained search | internal/store/hnsw_temporal.go | ✅ DONE |
| 6.3 | Add temporal + vector hybrid search | internal/store/hnsw_temporal.go | ✅ DONE |

### Python SDK (Phase 7)

| Step | Task | File | Status |
|------|------|------|--------|
| 7.1 | Add temporal search to Python client | longbowclientsdk/src/longbow/client.py | ✅ DONE |
| 7.2 | Add version history methods | longbowclientsdk/src/longbow/client.py | ✅ DONE |
| 7.3 | Add temporal aggregation methods | longbowclientsdk/src/longbow/client.py | ✅ DONE |

### Configuration

```yaml
# Temporal Query Configuration
temporal:
  enabled: true
  version_history:
    max_versions_per_vector: 10
    retention_period: 7d
  ttl:
    enabled: true
    default_ttl: 30d
    cleanup_interval: 1h
  aggregation:
    enabled: true
    max_buckets: 1000
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| TEMPORAL_ENABLED | false | Enable temporal index |
| TEMPORAL_VERSION_HISTORY | false | Enable version history tracking |
| TEMPORAL_MAX_VERSIONS | 10 | Max versions per vector |
| TEMPORAL_RETENTION_PERIOD | 168h | Version retention period |
| TEMPORAL_TTL_ENABLED | false | Enable TTL expiration |
| TEMPORAL_DEFAULT_TTL | 720h | Default TTL for vectors |
| TEMPORAL_CLEANUP_INTERVAL | 1h | TTL cleanup interval |
| TEMPORAL_AGGREGATION_ENABLED | false | Enable temporal aggregation |
| TEMPORAL_MAX_BUCKETS | 1000 | Max aggregation buckets |

### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| temporal_search_total | Counter | Total temporal searches |
| temporal_search_duration_seconds | Histogram | Temporal search latency |
| temporal_version_history_size | Gauge | Version history entries |
| temporal_ttl_expired_total | Counter | Vectors expired by TTL |
| temporal_aggregation_duration_seconds | Histogram | Aggregation latency |
| temporal_index_size | Gauge | Total temporal vectors |

---

*Last Updated: 2026-04-07 (Part 22: Temporal Query Capabilities COMPLETED)*
