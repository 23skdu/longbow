# Releases

## v0.1.1 (2025-12-22)

This release marks a major architectural evolution, transforming Longbow from a single-node engine into a distributed, fault-tolerant cluster system. It introduces a custom SWIM-based Gossip protocol, hybrid CPU/GPU search acceleration, and production-grade reliability controls.

### Top 10 Features

1. **Distributed Mesh Clustering (SWIM)**: Implemented a proprietary, eventually consistent cluster membership protocol based on SWIM (Scalable Weakly-consistent Infection-style Process Group Membership). This enables decentralized failure detection and rapid cluster convergence without a single point of failure.
2. **Global Query Federation**: Nodes now intelligently proxy requests across the mesh, allowing clients to query any node and receive globally aggregated results, abstracting data locality from the application layer.
3. **Hybrid Compute Architecture**: Introduced native GPU acceleration for both Apple Silicon (Metal) and NVIDIA (CUDA) platforms. The system dynamically manages hybrid indices, transparently falling back to CPU when sufficient hardware resources are unavailable.
4. **Rate Limiting & Traffic Control**: Integrated a token-bucket rate limiter at the gRPC interceptor level. This provides configurable throughput protection (RPS and Burst) to prevent service degradation during traffic spikes.
5. **Graceful Shutdown & Data Safety**: Implemented robust signal handling (SIGINT/SIGTERM) with a 30-second coordination window, ensuring all in-flight requests complete and Write-Ahead Logs (WAL) are flushed to disk before termination.
6. **Kubernetes-Native Resilience**: Validated 3-node distributed deployments on Kubernetes. The system is certified to recover automatically from pod failures, maintaining partition tolerance and service availability.
7. **Enhanced Observability Stack**: Expanded Prometheus metrics coverage to include GPU utilization, Gossip mesh health, compaction latency, and cross-node proxy traffic, offering deep visibility into distributed system behavior.
8. **Compaction Engine Stability**: Critical improvements to the storage compaction subsystem, enforcing strict type safety to ensure long-term data durability and preventing corruption during background maintenance tasks.
9. **Automated Regression Suite**: Introduced a comprehensive distributed testing framework that validates end-to-end data consistency and cluster recoverability under failure conditions.
10. **Optimized Deployment Artifacts**: Significantly improved container build processes and context optimization, resulting in leaner, more secure Docker images for production deployment.

---

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
