# Releases

## v0.1.0 (2025-12-20)

This release introduces significant features for production reliability, including vector deletion, metadata filtering, and an enhanced observability suite.

### New Features

- **Vector Deletion**: Vectors can now be deleted using the `delete-vector` action via the Meta Server. Deletions are persistent and handled via in-memory tombstones.
- **Metadata Filtering**: Support for predicate-based filtering (e.g., `id > 100`) has been integrated into both vector searches and dataset scans.
- **Flight DoExchange**: Implemented a bidirectional "ping-pong" health check on the Data Server.
- **Prometheus Monitoring Suite**:
  - Added 100+ new metrics for deep observability into HNSW search, WAL performance, and memory management.
  - Added recording and alerting rules in `grafana/rules.yml`.
  - Added a comprehensive Grafana dashboard in `grafana/dashboards/longbow.json`.

### Improvements & Fixes

- **HNSW Indexing Consistency**: Switched to sequential indexing for the initial allocation phase to ensure deterministic mapping between `VectorID` and row locations, resolving a critical search result misalignment bug.
- **Zero-Alloc Parsing**: Optimized Flight ticket parsing to reduce allocations and improve safety when handling concurrent requests.
- **Updated Test Scripts**: `ops_test.py` and `perf_test.py` now support the full range of new features including deletion benchmarking and filtered queries.

---

## v0.0.9 (2024-11-15)

- Initial release with basic Apache Arrow Flight support.
- Minimal HNSW index implementation.
- Basic WAL and Parquet snapshots.
