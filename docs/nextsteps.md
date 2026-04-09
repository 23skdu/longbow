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

## 🎯 REMAINING WORK SUMMARY

### HIGH PRIORITY

| # | Feature | Status | Files |
|---|---------|--------|-------|
| ~~1~~ | ~~Dataset Import/Export~~ | ✅ DONE | `internal/store/dataset_io.go` |
| 2 | ONNX Runtime Integration | STUB | `internal/store/ml_reranker.go`, `internal/store/embedding_generator.go` |
| 3 | CUDA Memory Operations | STUB | `internal/gpu/memory/memory_cuda_stub.go` |

### MEDIUM PRIORITY

| # | Feature | Status | Files |
|---|---------|--------|-------|
| 4 | Window Functions | PARTIAL | Advanced SQL features |
| 5 | Subqueries/CTE | NOT IMPLEMENTED | Advanced SQL features |

### LOW PRIORITY

| # | Feature | Status | Files |
|---|---------|--------|-------|
| 6 | Documentation Updates | PENDING | API docs, migration guide |
| 7 | Monitoring & Observability | PENDING | CDC metrics, tracing |

---

## Part 1: Dataset Import/Export — HIGH PRIORITY

### Overview

**Description**: ImportDataset and ExportDataset methods return "not implemented" errors.
These will be implemented following S3 snapshot patterns using Apache Arrow/Parquet format.

**Reference Implementation**: See `internal/storage/s3_backend.go` - WriteSnapshot/ReadSnapshot methods

### Parquet Schema

| Column     | Type                     | Description                             |
| :--------- | :----------------------- | :-------------------------------------- |
| id         | int64                    | Unique identifier for the vector.       |
| vector     | fixed_size_binary_array  | The embedding vector (e.g., 1536 dims). |
| metadata   | binary (JSON)            | Associated metadata blob.              |
| created_at | timestamp                | Ingestion timestamp.                    |

### Implementation Subtasks

| Step | Task | File | Status |
|------|------|------|--------|
| 1.1 | Create `internal/store/dataset_io.go` with ExportToParquet method | internal/store/dataset_io.go | PENDING |
| 1.2 | Implement Arrow Record to Parquet conversion using parquet-go | internal/store/dataset_io.go | PENDING |
| 1.3 | Add streaming Parquet writer for large datasets | internal/store/dataset_io.go | PENDING |
| 1.4 | Create `internal/store/dataset_io.go` with ImportFromParquet method | internal/store/dataset_io.go | PENDING |
| 1.5 | Implement Parquet to Arrow Record conversion | internal/store/dataset_io.go | PENDING |
| 1.6 | Add validation for schema compatibility on import | internal/store/dataset_io.go | PENDING |
| 1.7 | Wire ExportDataset to use StorageBackend (local/S3) | internal/store/rate_limit.go | PENDING |
| 1.8 | Wire ImportDataset to use StorageBackend | internal/store/rate_limit.go | PENDING |
| 1.9 | Implement CloneDataset using Export -> Import | internal/store/dataset_io.go | PENDING |
| 1.10 | Add unit tests for dataset export | internal/store/dataset_io_test.go | PENDING |
| 1.11 | Add unit tests for dataset import | internal/store/dataset_io_test.go | PENDING |
| 1.12 | Add integration tests for dataset cloning | internal/store/dataset_io_test.go | PENDING |
| 1.13 | Add Prometheus metrics for import/export operations | internal/metrics/ | PENDING |
| 1.14 | Add benchmarks for large dataset import/export | internal/store/dataset_io_test.go | PENDING |

### Implementation Details

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

### Metrics to Add

| Metric | Type | Description |
|--------|------|-------------|
| `longbow_dataset_export_total` | Counter | Total dataset exports |
| `longbow_dataset_export_duration_seconds` | Histogram | Export duration |
| `longbow_dataset_export_bytes` | Histogram | Bytes exported |
| `longbow_dataset_import_total` | Counter | Total dataset imports |
| `longbow_dataset_import_duration_seconds` | Histogram | Import duration |
| `longbow_dataset_import_rows` | Histogram | Rows imported |

### Files to Modify

- `internal/store/rate_limit.go` - Update ExportDataset/ImportDataset signatures
- `internal/store/dataset.go` - Add export/import methods to Dataset
- `internal/storage/` - Reuse existing S3Backend interface
- `internal/metrics/` - Add import/export metrics

---

## Part 2: ONNX Runtime Integration — HIGH PRIORITY

### Overview

**Description**: ML reranker and embedding generator use stub implementations.

### Implementation Subtasks

| Step | Task | File | Status |
|------|------|------|--------|
| 2.1 | Integrate ONNX Runtime Go bindings for ML Reranker | internal/store/ml_reranker.go | PENDING |
| 2.2 | Integrate ONNX Runtime Go bindings for Embedding Generator | internal/store/embedding_generator.go | PENDING |
| 2.3 | Replace stubMLModel with real ONNX model | internal/store/ml_reranker.go | PENDING |
| 2.4 | Replace stubEmbeddingModel with real ONNX model | internal/store/embedding_generator.go | PENDING |
| 2.5 | Add unit tests for ONNX inference | internal/store/ml_reranker_test.go | PENDING |

### Files

- `internal/store/ml_reranker.go:53,58,144` (stubMLModel)
- `internal/store/embedding_generator.go:621,641,701` (stubEmbeddingModel)

---

## Part 3: CUDA Memory Operations — MEDIUM PRIORITY

### Overview

**Description**: CUDA memory stub returns "not implemented" errors.

### Implementation Subtasks

| Step | Task | File | Status |
|------|------|------|--------|
| 3.1 | Implement allocateCUDAMemory | internal/gpu/memory/memory_cuda.go | PENDING |
| 3.2 | Implement freeCUDAMemory | internal/gpu/memory/memory_cuda.go | PENDING |
| 3.3 | Implement cudaMemcpyHostToDevice | internal/gpu/memory/memory_cuda.go | PENDING |
| 3.4 | Implement cudaMemcpyDeviceToHost | internal/gpu/memory/memory_cuda.go | PENDING |

### Files

`internal/gpu/memory/memory_cuda_stub.go:17,22,27`

---

## Part 4: Window Functions — MEDIUM PRIORITY

### Missing vs DataFusion

| Feature | Longbow | DataFusion |
|---------|---------|-------------|
| Window functions (ROW_NUMBER, RANK) | ❌ | ✅ Full |
| Streaming aggregation | Partial | ✅ Full |
| Subqueries | ❌ | ✅ |
| CTE (WITH clause) | ❌ | ✅ |

### Implementation Subtasks

| Step | Task | Status |
|------|------|--------|
| 4.1 | Add window functions | PENDING |
| 4.2 | Add subquery support | PENDING |
| 4.3 | Add CTE support | PENDING |

---

## Part 5: Documentation & Monitoring — LOW PRIORITY

### Documentation Updates

| Step | Task | Status |
|------|------|--------|
| 5.1 | Add API documentation for CDC | PENDING |
| 5.2 | Add usage examples for learned indexes | PENDING |
| 5.3 | Add migration guide for new features | PENDING |

### Monitoring & Observability

| Step | Task | Status |
|------|------|--------|
| 6.1 | Add Prometheus metrics for CDC | PENDING |
| 6.2 | Add tracing for WebSocket operations | PENDING |
| 6.3 | Add metrics for learned index predictor | PENDING |
| 6.4 | Add health checks for streaming features | PENDING |

---

## ✅ COMPLETED FEATURES

### Part 20: Predicate & Projection Pushdown

| Feature | Status |
|---------|--------|
| Predicate Pushdown | ✅ DONE |
| Projection Pushdown | ✅ DONE |
| FilterEvaluator (int64, float32, float64, string, compound, nested) | ✅ DONE |
| Fuzz Tests | ✅ DONE |
| HNSW Predicate Integration | ✅ DONE |
| Python SDK Integration | ✅ DONE |

### Part 21: Temporal Query Capabilities

| Feature | Status |
|---------|--------|
| TemporalIndex | ✅ DONE |
| Version History | ✅ DONE |
| Time-based TTL | ✅ DONE |
| Temporal Aggregation | ✅ DONE |
| gRPC API | ✅ DONE |
| Python SDK | ✅ DONE |
| HNSW Integration | ✅ DONE |

### Server Integration

| Feature | Status |
|---------|--------|
| CDC Initialization | ✅ DONE |
| WebSocket Server | ✅ DONE |
| MQ Exporter | ✅ DONE |
| Learned Index Predictor | ✅ DONE |
| gRPC Service Handlers | ✅ DONE |

### Test Coverage

| Feature | Status |
|---------|--------|
| Federated Search Tests | ✅ DONE |
| Geo Search Tests | ✅ DONE |
| Temporal Search Tests | ✅ DONE |
| CDC Integration Tests | ✅ DONE |
| WebSocket Integration Tests | ✅ DONE |

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

## Build Tags - Expected Stubs (NOT Issues)

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

*Last Updated: 2026-04-08*
