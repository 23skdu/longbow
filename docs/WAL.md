# Persistence and Hot Reloading

This document describes the persistence mechanisms (Write-Ahead Logging and Snapshots), Hot Reload capability, and Management Actions implemented in Longbow.

## Write-Ahead Logging (WAL)

Longbow uses a Write-Ahead Log (WAL) to ensure data durability. Every write operation (`DoPut`) is first appended to a WAL file before being acknowledged to the client. This ensures that in the event of a crash, the data can be recovered.

- **File Location**: `wal.log` in the working directory.
- **Format**: The WAL stores raw Arrow IPC streams.
- **Recovery**: On startup, Longbow reads the `wal.log` to reconstruct the in-memory state.

## Snapshots

To prevent the WAL from growing indefinitely and to speed up recovery, Longbow periodically creates snapshots of the in-memory state.

- **Directory**: `snapshots/`
- **Mechanism**: The entire in-memory vector store is serialized to disk.
- **Optimization**: Uses memory mapping (`mmap`) for Zero-Copy loading, drastically reducing startup time for large datasets.
- **Configuration**: The interval is controlled by the `LONGBOW_SNAPSHOT_INTERVAL` environment variable (e.g., `5m`, `1h`).

## Hot Reloading

Longbow supports dynamic configuration updates without restarting the process. This is achieved by sending a `SIGHUP` signal to the running process.

### Triggering a Reload

To trigger a reload, send the `SIGHUP` signal to the Longbow process ID (PID):

```bash
kill -HUP <PID>
```

### Reloadable Configurations

When a `SIGHUP` is received, Longbow re-reads the `.env` file and updates the following parameters dynamically:

1. **Max Memory** (`LONGBOW_MAX_MEMORY`): Updates the memory limit for the vector store.
1. **Snapshot Interval** (`LONGBOW_SNAPSHOT_INTERVAL`): Updates the frequency of automated snapshots.

### Logging

The application logs the reload event:

```json
{"level":"INFO","msg":"Received SIGHUP, reloading configuration"}
{"level":"INFO","msg":"Configuration reloaded","max_memory":...,"snapshot_interval":...}
```

## Management Actions

Longbow exposes a `DoAction` Flight endpoint for administrative tasks. The following actions are supported:

| Action Type | Description |
| :--- | :--- |
| `force_snapshot` | Triggers an immediate snapshot of the current state to disk. |
| `get_stats` | Returns current statistics (record count, memory usage). |
| `drop_dataset` | Drops a specific dataset from memory. Requires a request body with the dataset name. |

### Example: Force Snapshot

```python
client.do_action(flight.Action("force_snapshot", b""))
```

## Compression

To optimize network bandwidth and performance, Longbow uses **LZ4** compression
for all Arrow Flight data streams (`DoGet`, `DoPut`). LZ4 was chosen over ZSTD
for its superior decompression speed, which is critical for high-throughput
real-time applications.

## Observability & Metrics

Longbow exports Prometheus metrics to track the health and performance of the
persistence layer.

### WAL Metrics

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| longbow_wal_writes_total | Counter | status (ok/error) | Total number of write operations to the WAL. |
| longbow_wal_bytes_written_total | Counter | - | Total bytes written to the WAL file. |
| longbow_wal_replay_duration_seconds | Histogram | - | Time taken to replay the WAL during startup. |

### Snapshot Metrics

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| longbow_snapshot_operations_total | Counter | status (ok/error) | Total number of snapshot attempts. |
| longbow_snapshot_duration_seconds | Histogram | - | Duration of the snapshot process. |
