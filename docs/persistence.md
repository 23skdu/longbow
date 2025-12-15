# Persistence Architecture

Longbow uses a hybrid persistence model to ensure data durability, efficient
storage, and fast recovery. This document describes the Write-Ahead Logging
(WAL), Snapshot mechanisms, Hot Reload capabilities, and Observability signals.

## Architecture Overview

Longbow splits persistence into two distinct paths:

1. **Hot Path (WAL)**: Synchronous, row-oriented log for immediate crash

 consistency.

1. **Cold Storage (Snapshots)**: Asynchronous, columnar (Parquet) storage for

 compaction and fast startup.

### Storage Tiering Strategy

With the introduction of split persistence paths in the Helm chart, you can
optimize your storage costs and performance:

* **WAL Volume**: Should be backed by high-IOPS storage (e.g., NVMe SSD) as
  every write operation hits this disk.
* **Snapshot Volume**: Can be backed by slower, cheaper storage (e.g., HDD or
  standard cloud block storage) as snapshots are written asynchronously in the
  background.

## Write-Ahead Log (WAL)

* **Format**: Row-oriented, append-only log (Arrow IPC stream).
* **Purpose**: Provides immediate durability for incoming writes.
* **Mechanism**: Every `DoPut` operation appends the record to `wal.log` before

 acknowledging success to the client.

* **File Location**: Configurable via `LONGBOW_DATA_PATH` (default:

 `/data/wal.log`).

## Snapshots

* **Format**: Apache Parquet (`.parquet`).
* **Purpose**: Efficient long-term storage, compaction, and faster startup.

### Parquet Schema

Snapshots are stored using a standardized schema, allowing them to be queried
directly by external tools like DuckDB or Pandas:

```text
root
<!-- markdownlint-disable MD013 -->
 |-- id: int32
 |-- vector: list<element: float> (fixed_size_list[128])
<!-- markdownlint-enable MD013 -->
```

### Mechanism

1. A background ticker triggers snapshots at configured intervals.
1. In-memory Arrow records are serialized into Parquet files (one per dataset).
1. Parquet offers superior compression (Snappy/ZSTD) and is a standard format

 for analytics.

1. After a successful snapshot, the WAL is truncated to free up space.

## Configuration

The persistence layer is configured via environment variables, which are mapped
in the Helm chart.

<!-- markdownlint-disable MD013 -->
| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_DATA_PATH` | Directory/File path for the WAL. | `/data` |
| `LONGBOW_SNAPSHOT_PATH` | Directory path for storing Parquet snapshots. | `/snapshots` |
| `LONGBOW_SNAPSHOT_INTERVAL` | Interval for automated snapshots (e.g., `5m`, `1h`). | `1h` |
| `LONGBOW_MAX_MEMORY` | Max memory limit before eviction/snapshotting. | `0` (unlimited) |
<!-- markdownlint-enable MD013 -->
<!-- markdownlint-enable MD013 -->

## Hot Reloading

Longbow supports dynamic configuration updates without restarting the process by
sending a `SIGHUP` signal.

### Triggering a Reload

```bash
kill -HUP <PID>
```

### Reloadable Parameters

* **Max Memory** (`LONGBOW_MAX_MEMORY`)
* **Snapshot Interval** (`LONGBOW_SNAPSHOT_INTERVAL`)

## Management Actions

Longbow exposes a `DoAction` Flight endpoint for administrative tasks.

<!-- markdownlint-disable MD013 -->
| Action Type | Description |
| :--- | :--- |
| `force_snapshot` | Triggers an immediate snapshot of the current state to disk. |
| `get_stats` | Returns current statistics (record count, memory usage). |
| `drop_dataset` | Drops a specific dataset from memory. Requires a request body with the dataset name. |
<!-- markdownlint-enable MD013 -->
<!-- markdownlint-enable MD013 -->

### Example: Force Snapshot (Python)

```python
client.do_action(flight.Action("force_snapshot", b""))
```

## Observability & Metrics

Longbow exports Prometheus metrics to track the health and performance of the
persistence layer.

### WAL Metrics

<!-- markdownlint-disable MD013 -->
| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_wal_writes_total` | Counter | status (ok/error) | Total number of write operations to the WAL. |
| `longbow_wal_bytes_written_total` | Counter | - | Total bytes written to the WAL file. |
| `longbow_wal_replay_duration_seconds` | Histogram | - | Time taken to replay the WAL during startup. |
<!-- markdownlint-enable MD013 -->
<!-- markdownlint-enable MD013 -->

### Snapshot Metrics

<!-- markdownlint-disable MD013 -->
| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_snapshot_operations_total` | Counter | status (ok/error) | Total number of snapshot attempts. |
| `longbow_snapshot_duration_seconds` | Histogram | - | Duration of the snapshot process. |
<!-- markdownlint-enable MD013 -->
<!-- markdownlint-enable MD013 -->
