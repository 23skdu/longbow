# Storage, Durability & Persistence

Longbow provides a multi-layered storage architecture designed for high availability, transactional integrity, and automated data lifecycle management.

---

## 1. Data Durability (WAL & Snapshots)

Longbow ensures zero data loss using a combination of Write-Ahead Logs (WAL) and periodic Parquet snapshots.

### Write-Ahead Log (WAL)

- **Mechanism**: Every `DoPut` is synchronously written to a batched WAL before being acknowledged.
- **Performance**: High-throughput writes using `io_uring` (Linux) and asynchronous fsync options.
- **Recovery**: On startup, Longbow replays the WAL to reconstruct the in-memory HNSW index and Arrow buffers.

### High Availability & Replication (v0.2.1+)

- **Quorum-based Replication**: When running in a cluster (via Gossip), Longbow performs **synchronous WAL replication** to peer nodes.
- **Durability Guarantee**: A write is only acknowledged to the client after it has been persisted locally AND replicated to a quorum ($N/2 + 1$) of nodes.
- **Failover**: If a primary node fails, follower nodes have a consistent copy of the WAL up to the last acknowledged write, enabling rapid failover with zero data loss.
- **Observability**: Monitor replication health via `longbow_wal_replication_latency_seconds`.

### Snapshots (Parquet & Arrow)

- **Format**: Data is periodically flushed to Apache Parquet files, providing a columnar, compressed representation of the dataset.
- **Cloud-Native**: Snapshots can be offloaded to **S3-compatible storage** and **Google Cloud Storage (GCS)** for long-term retention and cross-region recovery.

---

## 2. Remote Persistence (S3 & GCS)

Longbow supports reading and writing directly to cloud storage for both ingestion and exports.

### Supported URIs

- **S3**: `s3://bucket-name/path/to/file.parquet`
- **GCS**: `gs://bucket-name/path/to/file.parquet`

### Ingestion via CLI

You can import large datasets directly from cloud buckets without local buffering (where supported by the format):

```bash
# Import from S3
longbow-cli import -dataset my-collection -input s3://my-bucket/data.parquet

# Import from GCS
longbow-cli import -dataset my-collection -input gs://my-bucket/data.parquet
```

### Export to Cloud

Longbow can export resident datasets directly to Arrow IPC (.arrow) or Parquet files in the cloud:

```bash
# Export to GCS
longbow-cli export -dataset my-collection -file gs://my-bucket/exports/today.arrow
```

---

## 3. Tiered Storage Configuration

For large-scale deployments, Longbow can offload "cold" or "warm" data to remote tiers.

### Configuration Environment Variables

| Variable | Description |
| :--- | :--- |
| `STORAGE_REMOTE_TYPE` | `s3` or `gcs` |
| `S3_BUCKET` | S3 bucket name |
| `S3_ENDPOINT` | Custom S3 endpoint (e.g. MinIO) |
| `GCS_BUCKET` | GCS bucket name |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to Google service account JSON key |

---

## 4. Data Lifecycle: Eviction & TTL

Longbow automatically manages memory pressure and data staleness through active eviction policies.

### Time-To-Live (TTL)

- **Behavior**: Removes datasets that have not been accessed within a configured duration.
- **Configuration**: Set `LONGBOW_TTL` (e.g., `24h`) to enable automated cleanup of transient caches.

### Least Recently Used (LRU)

- **Mechanism**: Triggered when memory usage approaches `LONGBOW_MAX_MEMORY`.
- **Action**: Evicts the least active datasets to make room for new high-priority writes.

---

## 5. Temporal Capabilities & Versioning

Longbow supports time-travel queries and multi-version concurrency control (MVCC) for evolving datasets.

### Temporal Search

Find vectors as they existed at a specific point in time or within a sliding window:

- **As-Of Search**: `search_type: "as_of"` at timestamp $T$.
- **Range Search**: Retrieve all updates within $[T_{start}, T_{end}]$.
- **Sliding Window**: Search the $N$ most recent vectors back from now.

### Version History

Maintain a log of changes per vector ID (configured via `TEMPORAL_MAX_VERSIONS`). This allows for audit trails and tracking model drift over time.

---

## 6. Schema Evolution

Longbow allows datasets to evolve their metadata schema without requiring re-indexing or downtime.

- **Additive Evolution**: New columns can be appended to existing Arrow schemas.
- **Compatibility**: Existing columns must retain their name and data type to ensure backward compatibility for search and scans.
- **Enforcement**: Mismatched schemas that break these rules are rejected at the ingestion layer.

---

## 7. Metrics & Observability

Monitor storage health via Prometheus:

- `longbow_evictions_total{reason="ttl|lru"}`: Count of dataset evictions.
- `longbow_persistence_wal_bytes_total`: WAL throughput.
- `longbow_remote_storage_duration_seconds{provider="s3|gcs"}`: Latency of remote operations.
- `longbow_remote_storage_ops_total{status="success|error"}`: Remote operation counters.
