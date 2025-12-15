# Longbow Metrics Documentation

Longbow exposes Prometheus metrics to provide visibility into the system's
performance, resource usage, and data characteristics. These metrics are
available at the `/metrics` endpoint on the configured metrics port
(default: 9090).

## Flight Operations Metrics

These metrics track the core Arrow Flight operations (DoGet, DoPut, etc.).

<!-- markdownlint-disable MD013 -->
| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_flight_operations_total` | Counter | `method`, `status` | The total number of processed Arrow Flight operations. Use this to track throughput and error rates (e.g., `status="error"`). |
| `longbow_flight_duration_seconds` | Histogram | `method` | The latency distribution of Flight operations. Useful for monitoring SLIs/SLOs for request latency. |
| `longbow_flight_bytes_processed_total` | Counter | `method` | The total estimated bytes processed during Flight operations. Helps in understanding network and I/O load. |
<!-- markdownlint-enable MD013 -->

## Vector Store Metrics

These metrics provide insights into the state and characteristics of the
stored vectors.

<!-- markdownlint-disable MD013 -->
| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_vector_index_size` | Gauge | None | The current total number of vectors stored in the index. This indicates the scale of the dataset currently in memory. |
| `longbow_average_vector_norm` | Gauge | None | The average L2 norm of the vectors currently in the index. This can be useful for detecting data drift or anomalies in the embedding generation process. |
| `longbow_index_build_latency_seconds` | Histogram | None | The time taken to "build" or update the index (e.g., during `DoPut` operations). High latency here may indicate performance bottlenecks in the write path. |
<!-- markdownlint-enable MD013 -->

## Persistence (WAL) Metrics

Metrics related to the Write-Ahead Log (WAL) for durability.

<!-- markdownlint-disable MD013 -->
| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_wal_writes_total` | Counter | `status` | The total number of write operations to the WAL. Failures here are critical for data durability. |
| `longbow_wal_bytes_written_total` | Counter | None | The total bytes written to the WAL file. Useful for monitoring disk I/O usage. |
| `longbow_wal_replay_duration_seconds` | Histogram | None | The time taken to replay the WAL during system startup. Longer replay times increase the time to readiness after a restart. |
<!-- markdownlint-enable MD013 -->

## Snapshot & Eviction Metrics

Metrics related to data lifecycle management.

<!-- markdownlint-disable MD013 -->
| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_snapshot_operations_total` | Counter | `status` | The total number of snapshot attempts. |
| `longbow_snapshot_duration_seconds` | Histogram | None | The duration of the snapshot creation process. |
| `longbow_evictions_total` | Counter | `reason` | The number of records evicted due to memory limits (`reason="lru"` or `reason="ttl"`). High eviction rates may suggest the need for more memory or a larger cluster. |
<!-- markdownlint-enable MD013 -->

## System Resource Metrics

<!-- markdownlint-disable MD013 -->
| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_memory_fragmentation_ratio` | Gauge | None | The ratio of `HeapAlloc` to `Sys` memory. A low ratio indicates high fragmentation or unused reserved memory, which might require tuning Go's GC or memory limits. |
<!-- markdownlint-enable MD013 -->
