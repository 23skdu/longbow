# Longbow Metrics Documentation

Longbow exposes comprehensive Prometheus metrics to provide visibility into system performance, resource usage, data characteristics, and distributed operations.
These metrics are available at the `/metrics` endpoint on the configured metrics port (default: 9090).

## 1. Flight Operations

Core Arrow Flight endpoint metrics.

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_flight_operations_total` | Counter | `method`, `status` | Total number of processed Flight operations (DoGet, DoPut, etc.). |
| `longbow_flight_duration_seconds` | Histogram | `method` | Latency distribution of Flight operations. |
| `longbow_flight_bytes_processed_total` | Counter | `method` | Total estimated bytes processed during operations. |
| `longbow_flight_ticket_parse_duration_seconds` | Histogram | None | Time spent parsing Flight ticket JSON. |
| `longbow_active_search_contexts` | Gauge | None | Number of concurrent DoGet/search operations in progress. |

## 2. Vector Store & HNSW Index

Metrics related to the vector storage engine and HNSW graph.

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_vector_index_size` | Gauge | None | Total number of vectors in the index. |
| `longbow_average_vector_norm` | Gauge | None | Average L2 norm of stored vectors. |
| `longbow_index_build_latency_seconds` | Histogram | None | Latency of index build/update operations. |
| `longbow_hnsw_nodes_visited` | Histogram | `dataset` | Number of HNSW nodes visited per search. |
| `longbow_hnsw_distance_calculations_total` | Counter | None | Total distance calculations performed. |
| `longbow_hnsw_active_readers` | Gauge | `dataset` | Number of active zero-copy readers per dataset. |
| `longbow_hnsw_graph_height` | Gauge | `dataset` | Maximum layer height of the HNSW graph. |
| `longbow_hnsw_node_count` | Gauge | `dataset` | Total nodes in the HNSW graph. |
| `longbow_hnsw_epoch_transitions_total` | Counter | None | Total count of epoch transitions for zero-copy concurrency. |

## 3. Persistence (WAL)

Write-Ahead Log durability and performance metrics.

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_wal_writes_total` | Counter | `status` | Total WAL write operations. |
| `longbow_wal_bytes_written_total` | Counter | None | Total bytes written to WAL. |
| `longbow_wal_replay_duration_seconds` | Histogram | None | Time taken to replay WAL on startup. |
| `longbow_wal_fsync_duration_seconds` | Histogram | `status` | Time taken for `fsync` operations. |
| `longbow_wal_batch_size` | Histogram | None | Number of entries flushed per batch. |
| `longbow_wal_pending_entries` | Gauge | None | Current depth of WAL entry channel (backpressure indicator). |
| `longbow_wal_adaptive_interval_ms` | Gauge | None | Current adaptive flush interval (ms). |
| `longbow_wal_write_rate_per_second` | Gauge | None | Current write rate tracking. |

## 4. Snapshot & Persistence Backends

Metrics for data lifecycle and backends (Local/S3).

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_snapshot_operations_total` | Counter | `status` | Total snapshot attempts. |
| `longbow_snapshot_duration_seconds` | Histogram | None | Duration of snapshot creation. |
| `longbow_snapshot_write_duration_seconds` | Histogram | None | Time spent writing Parquet files. |
| `longbow_snapshot_size_bytes` | Histogram | None | Size of generated snapshots in bytes. |
| `longbow_s3_request_duration_seconds` | Histogram | `operation` | S3 operation latency. |
| `longbow_s3_retries_total` | Counter | `operation` | S3 operation retry counts. |

## 5. Memory Management & Pools

Detailed tracking of memory allocators and object pools.

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_memory_fragmentation_ratio` | Gauge | None | Ratio of `HeapAlloc` to `Sys` memory. |
| `longbow_memory_heap_in_use_bytes` | Gauge | None | Current heap memory in use. |
| `longbow_arrow_memory_used_bytes` | Gauge | `allocator` | Memory used by Arrow allocators. |
| `longbow_arrow_zerocopy_reads_total` | Counter | None | Count of zero-copy buffer reads. |
| `longbow_arrow_buffer_retained_bytes` | Gauge | None | Bytes currently retained in zero-copy buffers. |
| `longbow_arena_alloc_bytes_total` | Counter | None | Total bytes allocated from search arenas. |
| `longbow_wal_buffer_pool_operations_total` | Counter | `operation` | WAL buffer pool Get/Put counts. |
| `longbow_ipc_buffer_pool_utilization` | Gauge | None | IPC buffer pool utilization ratio. |
| `longbow_vector_scratch_pool_misses_total` | Counter | None | Scratch buffer pool misses. |
| `longbow_memory_pressure_level` | Gauge | None | Current pressure level (0=None, 1=Soft, 2=Hard). |

## 6. Distributed Mesh & Replication

Metrics for multi-node operations, replication, and split-brain detection.

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_do_exchange_calls_total` | Counter | None | Total DoExchange invocations. |
| `longbow_replication_lag_seconds` | Gauge | `peer` | Replication lag by peer. |
| `longbow_peer_health_status` | Gauge | `peer` | Peer health (0=Down, 1=Up). |
| `longbow_gossip_active_members` | Gauge | None | Current number of alive members in the gossip mesh. |
| `longbow_gossip_pings_total` | Counter | `direction` | Total number of gossip pings (sent/received). |
| `longbow_mesh_sync_deltas_total` | Counter | `status` | Total record batches replicated via mesh sync (success/error). |
| `longbow_mesh_sync_bytes_total` | Counter | None | Total bytes replicated via mesh sync. |
| `longbow_mesh_merkle_match_total` | Counter | `result` | Merkle root comparison results (match/mismatch). |
| `longbow_replication_queued_total` | Counter | None | Records queued for replication. |
| `longbow_split_brain_partitions_total` | Counter | None | Network partitions detected. |
| `longbow_quorum_operation_duration_seconds` | Histogram | `operation`, `level` | Duration of quorum operations. |
| `longbow_merkle_tree_builds_total` | Counter | None | Count of Merkle tree anti-entropy builds. |
| `longbow_merkle_divergences_found_total` | Counter | None | Divergent records found via Merkle sync. |

## 7. Search & Filtering Pipelines

Analysis of search performance, filtering, and query pipelines.

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_vector_search_action_total` | Counter | None | Count of VectorSearch DoAction calls. |
| `longbow_filter_execution_duration_seconds` | Histogram | `dataset` | Duration of filter execution. |
| `longbow_filter_selectivity_ratio` | Histogram | `dataset` | Ratio of output rows to input rows. |
| `longbow_fast_path_usage_total` | Counter | `path` | Count of fast-path filter usage vs fallback. |
| `longbow_pipeline_batches_per_second` | Gauge | None | DoGet pipeline throughput estimate. |
| `longbow_zero_alloc_ticket_parse_total` | Counter | None | Successful zero-alloc ticket parses. |

## 8. SIMD & Optimization

Hardware acceleration metrics.

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_simd_dispatch_count_total` | Counter | `impl` | SIMD calls by implementation (AVX2, AVX512, NEON). |
| `longbow_simd_fma_operations_total` | Counter | `kernel` | Total SIMD FMA operations. |
| `longbow_simd_kernel_type` | Gauge | `operation` | Active kernel type ID. |
| `longbow_binary_quantize_ops_total` | Counter | None | Binary quantization operations. |
| `longbow_popcnt_distance_ops_total` | Counter | None | POPCNT-based Hamming distance calcs. |
| `longbow_quantization_memory_saved_bytes` | Gauge | None | Memory saved via quantization. |

## 9. Miscellaneous

System and component-specific counters.

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_evictions_total` | Counter | `reason` | Records evicted due to memory/TTL. |
| `longbow_gc_pause_duration_seconds` | Histogram | None | Go Garbage Collector pause times. |
| `longbow_warmup_progress_percent` | Gauge | None | Warmup completion percentage. |
| `longbow_idx_creations_total` | Counter | `type`, `result` | Index creation attempts by type. |
| `longbow_circuit_breaker_rejections_total` | Counter | None | Requests rejected by circuit breakers. |
