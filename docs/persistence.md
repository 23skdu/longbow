# Storage, Durability & Persistence

Longbow provides a multi-layered storage architecture designed for high availability, transactional integrity, and automated data lifecycle management.

---

## 1. Data Durability (WAL & Snapshots)

Longbow ensures zero data loss using a combination of Write-Ahead Logs (WAL) and periodic Parquet snapshots.

### Write-Ahead Log (WAL)
- **Mechanism**: Every `DoPut` is synchronously written to a batched WAL before being acknowledged.
- **Performance**: High-throughput writes using `io_uring` (Linux) and asynchronous fsync options.
- **Recovery**: On startup, Longbow replays the WAL to reconstruct the in-memory HNSW index and Arrow buffers.

### Snapshots (Parquet)
- **Format**: Data is periodically flushed to Apache Parquet files, providing a columnar, compressed representation of the dataset.
- **Cloud-Native**: Snapshots can be offloaded to **S3-compatible storage** for long-term retention and cross-region recovery.

---

## 2. Data Lifecycle: Eviction & TTL

Longbow automatically manages memory pressure and data staleness through active eviction policies.

### Time-To-Live (TTL)
- **Behavior**: Removes datasets that have not been accessed within a configured duration.
- **Configuration**: Set `LONGBOW_TTL` (e.g., `24h`) to enable automated cleanup of transient caches.

### Least Recently Used (LRU)
- **Mechanism**: Triggered when memory usage approaches `LONGBOW_MAX_MEMORY`.
- **Action**: Evicts the least active datasets to make room for new high-priority writes.

---

## 3. Temporal Capabilities & Versioning

Longbow supports time-travel queries and multi-version concurrency control (MVCC) for evolving datasets.

### Temporal Search
Find vectors as they existed at a specific point in time or within a sliding window:
- **As-Of Search**: `search_type: "as_of"` at timestamp $T$.
- **Range Search**: Retrieve all updates within $[T_{start}, T_{end}]$.
- **Sliding Window**: Search the $N$ most recent vectors back from now.

### Version History
Maintain a log of changes per vector ID (configured via `TEMPORAL_MAX_VERSIONS`). This allows for audit trails and tracking model drift over time.

---

## 4. Schema Evolution

Longbow allows datasets to evolve their metadata schema without requiring re-indexing or downtime.

- **Additive Evolution**: New columns can be appended to existing Arrow schemas.
- **Compatibility**: Existing columns must retain their name and data type to ensure backward compatibility for search and scans.
- **Enforcement**: Mismatched schemas that break these rules are rejected at the ingestion layer.

---

## 5. Metrics & Observability

Monitor storage health via Prometheus:
- `longbow_evictions_total{reason="ttl|lru"}`: Count of dataset evictions.
- `longbow_persistence_wal_bytes_total`: WAL throughput.
- `longbow_temporal_index_size`: Resident temporal vectors.
