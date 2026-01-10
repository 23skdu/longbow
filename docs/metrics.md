# Longbow Metrics Reference

Complete reference for all Prometheus metrics exported by Longbow.

**Metrics Endpoint**: `http://localhost:9090/metrics` (configurable via `LONGBOW_METRICS_ADDR`)

## Table of Contents

1. [Flight & RPC](#flight--rpc)
2. [Vector Search](#vector-search)
3. [HNSW Index & PQ](#hnsw-index--pq)
4. [WAL & Persistence](#wal--persistence)
5. [Memory Management & NUMA](#memory-management--numa)
6. [Mesh & Gossip](#mesh--gossip)
7. [Replication & Quorum](#replication--quorum)
8. [Sharding](#sharding)
9. [Hybrid Search](#hybrid-search)
10. [Performance Optimizations](#performance-optimizations)
11. [System & Configuration](#system--configuration)
12. [Compaction & Background](#compaction--background)

---

## Flight & RPC

### longbow_flight_operations_total

**Type**: Counter  
**Labels**: `method`, `status`  
**Description**: Total number of processed Arrow Flight operations (DoPut, DoGet, DoAction, etc.)

### longbow_flight_duration_seconds

**Type**: Histogram  
**Labels**: `method`  
**Description**: Duration of Arrow Flight operations

### longbow_flight_bytes_processed_total

**Type**: Counter  
**Labels**: `method`  
**Description**: Total bytes processed in Flight operations

### longbow_flight_rows_processed_total

**Type**: Counter  
**Labels**: `method`, `status`  
**Description**: Total number of rows processed in Flight operations

### longbow_flight_ticket_parse_duration_seconds

**Type**: Histogram  
**Description**: Time spent parsing Flight ticket JSON

### longbow_flight_pool_connections_active

**Type**: Gauge  
**Labels**: `host`  
**Description**: Active Flight client pool connections by host

### longbow_flight_pool_wait_duration_seconds

**Type**: Histogram  
**Labels**: `host`  
**Description**: Time spent waiting for Flight pool connection by host

---

## Vector Search

### longbow_vector_search_latency_seconds

**Type**: Histogram  
**Labels**: `dataset`  
**Description**: Latency of vector search operations

### longbow_vector_search_action_requests_total

**Type**: Counter  
**Description**: Total vector search action requests processed

### longbow_vector_search_action_errors_total

**Type**: Counter  
**Description**: Total errors during vector search action processing

### longbow_vector_search_action_duration_seconds

**Type**: Histogram  
**Description**: Latency of vector search action requests

### longbow_active_search_contexts

**Type**: Gauge  
**Description**: Number of concurrent DoGet/search operations in progress

### longbow_bruteforce_searches_total

**Type**: Counter  
**Description**: Total searches performed using BruteForce linear scan

### longbow_zero_alloc_vector_search_parse_total

**Type**: Counter  
**Description**: Total successful zero-allocation parsing of search queries

### longbow_vector_search_parse_fallback_total

**Type**: Counter  
**Description**: Total fallback to standard JSON parsing for search queries

---

## HNSW Index & PQ

### longbow_hnsw_node_count

**Type**: Gauge  
**Labels**: `dataset`  
**Description**: Total number of nodes in the HNSW graph

### longbow_hnsw_graph_height

**Type**: Gauge  
**Labels**: `dataset`  
**Description**: Maximum layer height of the HNSW graph (search complexity)

### longbow_hnsw_distance_calculations_total

**Type**: Counter  
**Description**: Total HNSW distance calculations performed

### longbow_hnsw_nodes_visited

**Type**: Histogram  
**Labels**: `dataset`  
**Description**: Number of HNSW nodes visited per search

### longbow_hnsw_searches_total

**Type**: Counter  
**Description**: Total number of searches performed using HNSW index

### longbow_hnsw_active_readers

**Type**: Gauge  
**Labels**: `dataset`  
**Description**: Number of active zero-copy readers per dataset

### longbow_hnsw_epoch_transitions_total

**Type**: Counter  
**Description**: Total HNSW epoch transitions for zero-copy access

### longbow_hnsw_pq_enabled

**Type**: Gauge  
**Labels**: `dataset`  
**Description**: Whether Product Quantization is enabled (1) or disabled (0) for the dataset

### longbow_hnsw_pq_training_duration_seconds

**Type**: Histogram  
**Labels**: `dataset`  
**Description**: Time taken to train PQ encoder for a dataset

### longbow_adaptive_index_migrations_total

**Type**: Counter  
**Description**: Total number of migrations from BruteForce to HNSW index

### longbow_hnsw_parallel_search_splits_total

**Type**: Counter  
**Labels**: `dataset`
**Description**: Total number of parallel search splits (distribution of work across workers)

### longbow_hnsw_pq_compressed_bytes_total

**Type**: Gauge
**Labels**: `dataset`
**Description**: Total bytes of memory saved/used by Product Quantization compressed vectors

### longbow_vector_search_gpu_latency_seconds

**Type**: Histogram
**Description**: Latency of GPU-accelerated vector search operations

### longbow_vector_search_gpu_operations_total

**Type**: Counter
**Description**: Total number of GPU-accelerated vector search operations

---

## WAL & Persistence

### longbow_wal_writes_total

**Type**: Counter  
**Labels**: `status`  
**Description**: Total number of WAL write operations

### longbow_wal_bytes_written_total

**Type**: Counter  
**Description**: Total bytes written to the Write-Ahead Log

### longbow_wal_fsync_duration_seconds

**Type**: Histogram  
**Labels**: `status`  
**Description**: Time taken for WAL fsync operations

### longbow_wal_batch_size

**Type**: Histogram  
**Description**: Number of entries flushed per WAL batch

### longbow_wal_pending_entries

**Type**: Gauge  
**Description**: Current number of pending WAL entries (backpressure indicator)

### longbow_wal_write_rate_per_second

**Type**: Gauge  
**Description**: Current WAL write rate per second

### longbow_wal_adaptive_interval_ms

**Type**: Gauge  
**Description**: Current adaptive WAL flush interval in milliseconds

### longbow_wal_replay_duration_seconds

**Type**: Histogram  
**Description**: Time taken to replay the Write-Ahead Log

### longbow_wal_lock_wait_duration_seconds

**Type**: Histogram  
**Labels**: `type`  
**Description**: Time spent waiting for WAL locks

### longbow_s3_operations_total

**Type**: Counter
**Labels**: `operation`, `status`
**Description**: Total S3 operations (PutObject, GetObject, etc.)

### longbow_s3_request_duration_seconds

**Type**: Histogram
**Labels**: `operation`
**Description**: Latency of S3 operations

### longbow_s3_retries_total

**Type**: Counter
**Labels**: `operation`
**Description**: Total S3 operation retries

### longbow_ipc_buffer_pool_utilization

**Type**: Gauge
**Description**: Current utilization of the IPC buffer pool (0-1)

---

## Memory Management & NUMA

### longbow_memory_heap_in_use_bytes

**Type**: Gauge  
**Description**: Current heap memory in use

### longbow_arrow_memory_used_bytes

**Type**: Gauge  
**Labels**: `allocator`  
**Description**: Memory bytes used by Arrow allocator

### longbow_memory_pressure_level

**Type**: Gauge  
**Description**: Current memory pressure level (0-100)

### longbow_memory_fragmentation_ratio

**Type**: Gauge  
**Description**: Ratio of system memory reserved vs used (fragmentation indicator)

### longbow_memory_backpressure_rejects_total

**Type**: Counter  
**Description**: Total number of requests rejected due to memory backpressure

### longbow_memory_backpressure_acquires_total

**Type**: Counter  
**Description**: Total number of memory permits acquired

### longbow_memory_backpressure_releases_total

**Type**: Counter  
**Description**: Total number of memory permits released

### longbow_evictions_total

**Type**: Counter  
**Labels**: `reason`  
**Description**: Total number of evicted records due to memory limits

### longbow_numa_cross_node_access_total

**Type**: Counter  
**Labels**: `worker_node`, `data_node`  
**Description**: Total number of memory accesses where worker node != data node

### longbow_numa_worker_distribution

**Type**: Gauge  
**Labels**: `node`  
**Description**: Number of workers pinned to each NUMA node

---

## Mesh & Gossip

### longbow_gossip_active_members

**Type**: Gauge  
**Description**: Current number of alive members in the gossip mesh

### longbow_gossip_pings_total

**Type**: Counter  
**Labels**: `direction`  
**Description**: Total number of gossip pings (sent/received)

### longbow_do_exchange_calls_total

**Type**: Counter  
**Description**: Total number of DoExchange (gossip) calls

### longbow_do_exchange_duration_seconds

**Type**: Histogram  
**Description**: Latency of DoExchange (gossip) operations

### longbow_mesh_sync_deltas_total

**Type**: Counter  
**Labels**: `status`  
**Description**: Total number of record batches replicated via mesh sync

### longbow_mesh_sync_bytes_total

**Type**: Counter  
**Description**: Total bytes replicated via mesh sync

### longbow_mesh_merkle_match_total

**Type**: Counter  
**Labels**: `result`  
**Description**: Total Merkle root comparison results (match/mismatch)

---

## Replication & Quorum

### longbow_replication_peers_total

**Type**: Gauge  
**Description**: Total number of replication peers

### longbow_replication_success_total

**Type**: Counter  
**Description**: Total number of successful replication operations

### longbow_replication_failures_total

**Type**: Counter  
**Description**: Total number of replication failures

### longbow_replication_retries_total

**Type**: Counter  
**Description**: Total number of replication retries

### longbow_replication_queued_total

**Type**: Counter  
**Description**: Total number of operations queued for replication

### longbow_replication_lag_seconds

**Type**: Gauge  
**Labels**: `peer`  
**Description**: Replication lag in seconds by peer

### longbow_quorum_operation_duration_seconds

**Type**: Histogram  
**Labels**: `operation`, `consistency`  
**Description**: Duration of quorum operations

### longbow_quorum_success_total

**Type**: Counter  
**Labels**: `operation`, `consistency`  
**Description**: Total number of successful quorum operations

### longbow_quorum_failure_total

**Type**: Counter  
**Labels**: `operation`, `consistency`, `reason`  
**Description**: Total number of failed quorum operations

---

## Sharding

### longbow_sharded_hnsw_shard_size

**Type**: Gauge  
**Labels**: `dataset`, `shard`  
**Description**: Number of vectors in each HNSW shard

### longbow_sharded_hnsw_load_factor

**Type**: Gauge  
**Labels**: `dataset`, `shard`  
**Description**: Sharded HNSW load factor by shard (0-1)

### longbow_hnsw_sharding_migrations_total

**Type**: Counter  
**Description**: Total number of HNSW index migrations to sharded format

### longbow_shard_lock_wait_seconds

**Type**: Histogram  
**Description**: Time spent waiting to acquire shard locks

---

## Hybrid Search

### longbow_hybrid_search_vector_total

**Type**: Counter  
**Description**: Total dense vector searches in hybrid path

### longbow_hybrid_search_keyword_total

**Type**: Counter  
**Description**: Total sparse keyword searches in hybrid path

### longbow_bm25_documents_indexed_total

**Type**: Counter  
**Description**: Total documents added to the BM25 inverted index

---

## Performance Optimizations

### longbow_simd_dispatch_total

**Type**: Counter  
**Labels**: `implementation`  
**Description**: Count of SIMD implementation selections (AVX2, AVX512, NEON, etc.)

### longbow_doget_zero_copy_total

**Type**: Counter  
**Labels**: `type`  
**Description**: Total DoGet operations by copy method (zero-copy vs deep-copy)

### longbow_doget_pipeline_steps_total

**Type**: Counter  
**Labels**: `method`  
**Description**: Total DoGet pipeline steps processed by method

### longbow_pipeline_worker_utilization

**Type**: Gauge  
**Labels**: `worker_id`  
**Description**: DoGet pipeline worker utilization (0-1)

---

## System & Configuration

### longbow_gc_pause_duration_seconds

**Type**: Histogram  
**Description**: Go garbage collector pause durations

### longbow_grpc_max_recv_msg_size_bytes

**Type**: Gauge  
**Description**: Configured maximum gRPC receive message size in bytes

### longbow_grpc_max_send_msg_size_bytes

**Type**: Gauge  
**Description**: Configured maximum gRPC send message size in bytes

### longbow_grpc_initial_window_size_bytes

**Type**: Gauge  
**Description**: Configured gRPC initial window size in bytes

---

## Compaction & Background

### longbow_compaction_operations_total

**Type**: Counter  
**Labels**: `dataset`, `status`  
**Description**: Total compaction operations by status

### longbow_compaction_duration_seconds

**Type**: Histogram  
**Labels**: `dataset`  
**Description**: Duration of compaction operations

### longbow_compaction_records_removed_total

**Type**: Counter  
**Labels**: `dataset`  
**Description**: Total number of records removed during compaction (tombstone filtering)

### longbow_compaction_auto_triggers_total

**Type**: Counter  
**Description**: Total number of auto-triggered compactions

### longbow_warmup_progress_percent

**Type**: Gauge  
**Description**: Warmup progress percentage (0-100)

### longbow_tombstones_total

**Type**: Gauge  
**Labels**: `dataset`  
**Description**: Total number of active tombstones

---

## Distributed Consistency & Failover

### longbow_split_brain_heartbeats_total

**Type**: Counter
**Description**: Total number of split brain detector heartbeats

### longbow_split_brain_healthy_peers

**Type**: Gauge
**Description**: Current number of healthy peers seen by detector

### longbow_split_brain_partitions_total

**Type**: Counter
**Description**: Total number of partition events detected

### longbow_split_brain_fenced_state

**Type**: Gauge
**Description**: Whether the node is currently fenced (1=fenced, 0=normal)

### longbow_vector_clock_merges_total

**Type**: Counter
**Description**: Total number of vector clock merges

### longbow_vector_clock_conflicts_total

**Type**: Counter
**Description**: Total number of vector clock conflicts detected

---

## Load Balancing & Routing

### longbow_load_balancer_replicas_total

**Type**: Gauge
**Description**: Total number of replicas tracked by load balancer

### longbow_load_balancer_unhealthy_total

**Type**: Gauge
**Description**: Total number of unhealthy replicas

### longbow_load_balancer_selections_total

**Type**: Counter
**Labels**: `strategy`
**Description**: Total number of replica selections for read operations

### longbow_proxy_requests_forwarded_total

**Type**: Counter
**Labels**: `method`, `status`
**Description**: Total number of requests forwarded to other nodes

### longbow_proxy_request_latency_seconds

**Type**: Histogram
**Labels**: `method`
**Description**: Latency of forwarded requests

### longbow_global_search_duration_seconds

**Type**: Histogram
**Description**: Latency of global search operations

### longbow_global_search_fanout_size

**Type**: Histogram
**Description**: Number of peers queried during global search

### longbow_global_search_partial_failures_total

**Type**: Counter
**Description**: Total number of failed peer queries during global search

### longbow_id_resolution_duration_seconds

**Type**: Histogram
**Description**: Latency of resolving internal IDs to user IDs

---

## Circuit Breakers

### longbow_store_circuit_breaker_state_changes_total

**Type**: Counter
**Labels**: `name`, `from`, `to`
**Description**: Total number of circuit breaker state changes

### longbow_store_circuit_breaker_rejections_total

**Type**: Counter
**Description**: Total number of requests rejected by store circuit breaker

### longbow_store_circuit_breaker_successes_total

**Type**: Counter
**Description**: Total number of successful requests passing through breaker

### longbow_store_circuit_breaker_failures_total

**Type**: Counter
**Description**: Total number of failed requests passing through breaker

### longbow_trace_spans_total

**Type**: Counter
**Labels**: `name`
**Description**: Total number of trace spans created

---

## Dataset Metrics

### longbow_store_dropped_datasets_total

**Type**: Counter
**Description**: Total number of datasets explicitly dropped

### longbow_store_active_datasets

**Type**: Gauge
**Description**: Current number of active datasets in memory

---

## HNSW Adaptive Metrics

### longbow_hnsw_adaptive_m_value

**Type**: GaugeVec
**Labels**: `index_name`
**Description**: Current value of M parameter in HNSW graph

### longbow_hnsw_intrinsic_dimensionality

**Type**: GaugeVec
**Labels**: `index_name`
**Description**: Estimated intrinsic dimensionality of the data

### longbow_hnsw_adaptive_adjustments_total

**Type**: CounterVec
**Labels**: `index_name`
**Description**: Total number of times M has been adjusted dynamically

---

## HNSW Repair Metrics

### longbow_hnsw_repair_total

**Type**: Counter
**Description**: Total number of tombstone repairs performed

### longbow_hnsw_repair_duration_seconds

**Type**: Histogram
**Description**: Duration of tombstone repair cycles

### longbow_hnsw_repaired_connections_total

**Type**: Counter
**Description**: Number of connections re-wired from tombstones to valid nodes

---

**Total Metrics Documented**: 100+  
**Last Updated**: 2025-12-23
