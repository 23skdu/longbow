# Longbow Metrics Reference

Complete reference for all Prometheus metrics exported by Longbow.

**Metrics Endpoint**: `http://localhost:9090/metrics` (configurable via `LONGBOW_METRICS_ADDR`)

## Table of Contents

1. [Flight & RPC](#flight--rpc)
2. [Vector Search](#vector-search)
3. [HNSW Index](#hnsw-index)
4. [WAL & Persistence](#wal--persistence)
5. [Memory Management](#memory-management)
6. [Mesh & Gossip](#mesh--gossip)
7. [Replication & Quorum](#replication--quorum)
8. [Sharding](#sharding)
9. [Hybrid Search](#hybrid-search)
10. [Performance Optimizations](#performance-optimizations)
11. [System & Configuration](#system--configuration)

---

## Flight & RPC

### longbow_flight_operations_total

**Type**: Counter  
**Labels**: `method`, `status`  
**Description**: Total number of Flight RPC operations (DoPut, DoGet, DoAction, etc.)

### longbow_flight_duration_seconds

**Type**: Histogram  
**Labels**: `method`  
**Description**: Duration of Flight RPC operations in seconds

### longbow_flight_rows_processed_total

**Type**: Counter  
**Labels**: `method`, `status`  
**Description**: Total number of rows processed in Flight operations

### longbow_flight_bytes_processed_total

**Type**: Counter  
**Labels**: `method`, `direction`  
**Description**: Total bytes processed in Flight operations

### longbow_flight_pool_connections_active

**Type**: Gauge  
**Description**: Number of active Flight client pool connections

### longbow_flight_pool_wait_duration_seconds

**Type**: Histogram  
**Description**: Time spent waiting for Flight pool connections

### longbow_flight_ticket_parse_duration_seconds

**Type**: Histogram  
**Description**: Duration of Flight ticket parsing operations

### longbow_do_exchange_calls_total

**Type**: Counter  
**Labels**: `status`  
**Description**: Total DoExchange RPC calls

### longbow_do_exchange_duration_seconds

**Type**: Histogram  
**Description**: Duration of DoExchange operations

### longbow_do_exchange_batches_sent_total

**Type**: Counter  
**Description**: Total record batches sent via DoExchange

### longbow_do_exchange_batches_received_total

**Type**: Counter  
**Description**: Total record batches received via DoExchange

### longbow_do_exchange_errors_total

**Type**: Counter  
**Labels**: `error_type`  
**Description**: Total DoExchange errors by type

---

## Vector Search

### longbow_vector_search_latency_seconds

**Type**: Histogram  
**Labels**: `dataset`  
**Description**: Vector similarity search latency

### longbow_vector_search_action_requests_total

**Type**: Counter  
**Labels**: `dataset`, `status`  
**Description**: Total vector search action requests

### longbow_vector_search_action_errors_total

**Type**: Counter  
**Labels**: `dataset`, `error_type`  
**Description**: Vector search action errors by type

### longbow_vector_search_action_duration_seconds

**Type**: Histogram  
**Labels**: `dataset`  
**Description**: Duration of vector search actions

### longbow_active_search_contexts

**Type**: Gauge  
**Description**: Number of active search contexts

### longbow_zero_alloc_vector_search_parse_total

**Type**: Counter  
**Description**: Total zero-allocation vector search parses

### longbow_vector_search_parse_fallback_total

**Type**: Counter  
**Description**: Fallback to standard JSON parsing (not zero-alloc)

---

## HNSW Index

### longbow_hnsw_node_count

**Type**: Gauge  
**Labels**: `dataset`  
**Description**: Number of nodes in HNSW graph

### longbow_hnsw_graph_height

**Type**: Gauge  
**Labels**: `dataset`  
**Description**: Maximum layer height of HNSW graph

### longbow_hnsw_distance_calculations_total

**Type**: Counter  
**Labels**: `dataset`  
**Description**: Total distance calculations performed

### longbow_hnsw_nodes_visited

**Type**: Histogram  
**Labels**: `dataset`  
**Description**: Number of nodes visited during search

### longbow_hnsw_searches_total

**Type**: Counter  
**Labels**: `dataset`, `type`  
**Description**: Total HNSW searches by type (knn, filtered, etc.)

### longbow_hnsw_active_readers

**Type**: Gauge  
**Labels**: `dataset`  
**Description**: Number of active concurrent readers on HNSW graph

### longbow_hnsw_epoch_transitions_total

**Type**: Counter  
**Labels**: `dataset`  
**Description**: Total epoch-based concurrency control transitions

### longbow_hnsw_graph_sync_deltas_total

**Type**: Counter  
**Labels**: `dataset`, `direction`  
**Description**: Total graph sync delta operations

### longbow_hnsw_graph_sync_exports_total

**Type**: Counter  
**Labels**: `dataset`  
**Description**: Total graph exports for synchronization

### longbow_hnsw_graph_sync_imports_total

**Type**: Counter  
**Labels**: `dataset`  
**Description**: Total graph imports from synchronization

### longbow_hnsw_graph_sync_delta_applies_total

**Type**: Counter  
**Labels**: `dataset`, `status`  
**Description**: Total delta applications during sync

### longbow_hnsw_sharding_migrations_total

**Type**: Counter  
**Labels**: `dataset`  
**Description**: Total migrations to sharded HNSW

---

## WAL & Persistence

### longbow_wal_writes_total

**Type**: Counter  
**Labels**: `status`  
**Description**: Total WAL write operations

### longbow_wal_bytes_written_total

**Type**: Counter  
**Description**: Total bytes written to WAL

### longbow_wal_pending_entries

**Type**: Gauge  
**Description**: Number of pending WAL entries

### longbow_wal_batch_size

**Type**: Histogram  
**Description**: Size of WAL write batches

### longbow_wal_fsync_duration_seconds

**Type**: Histogram  
**Description**: Duration of WAL fsync operations

### longbow_wal_lock_wait_duration_seconds

**Type**: Histogram  
**Description**: Time spent waiting for WAL locks

### longbow_wal_replay_duration_seconds

**Type**: Histogram  
**Description**: Duration of WAL replay on startup

### longbow_wal_write_rate_per_second

**Type**: Gauge  
**Description**: Current WAL write rate (entries/sec)

### longbow_wal_adaptive_interval_ms

**Type**: Gauge  
**Description**: Current adaptive fsync interval in milliseconds

### longbow_wal_uring_submit_latency_seconds

**Type**: Histogram  
**Description**: io_uring submission latency (Linux only)

### longbow_wal_uring_sq_depth

**Type**: Gauge  
**Description**: io_uring submission queue depth

### longbow_wal_uring_cq_depth

**Type**: Gauge  
**Description**: io_uring completion queue depth

### longbow_wal_buffer_pool_operations_total

**Type**: Counter  
**Labels**: `operation`  
**Description**: WAL buffer pool operations (get, put, reuse)

### longbow_snapshot_operations_total

**Type**: Counter  
**Labels**: `operation`, `status`  
**Description**: Snapshot operations (create, restore, delete)

### longbow_snapshot_duration_seconds

**Type**: Histogram  
**Labels**: `operation`  
**Description**: Duration of snapshot operations

### longbow_snapshot_size_bytes

**Type**: Histogram  
**Labels**: `dataset`  
**Description**: Size of snapshots in bytes

### longbow_snapshot_write_duration_seconds

**Type**: Histogram  
**Description**: Duration of snapshot write operations

---

## Memory Management

### longbow_memory_heap_in_use_bytes

**Type**: Gauge  
**Description**: Heap memory currently in use

### longbow_arrow_memory_used_bytes

**Type**: Gauge  
**Description**: Memory used by Arrow allocator

### longbow_memory_pressure_level

**Type**: Gauge  
**Description**: Current memory pressure level (0=none, 1=moderate, 2=high)

### longbow_memory_fragmentation_ratio

**Type**: Gauge  
**Description**: Memory fragmentation ratio (reserved/used)

### longbow_memory_backpressure_acquires_total

**Type**: Counter  
**Description**: Total memory backpressure acquire attempts

### longbow_memory_backpressure_rejects_total

**Type**: Counter  
**Description**: Total memory backpressure rejections

### longbow_memory_backpressure_releases_total

**Type**: Counter  
**Description**: Total memory backpressure releases

### longbow_evictions_total

**Type**: Counter  
**Labels**: `reason`  
**Description**: Total evictions by reason (memory, ttl, manual)

### longbow_arena_alloc_bytes_total

**Type**: Counter  
**Description**: Total bytes allocated via arena allocator

### longbow_arena_pool_gets_total

**Type**: Counter  
**Description**: Total arena gets from pool

### longbow_arena_pool_puts_total

**Type**: Counter  
**Description**: Total arena puts to pool

### longbow_arena_resets_total

**Type**: Counter  
**Description**: Total arena resets

### longbow_arena_overflow_total

**Type**: Counter  
**Description**: Total arena overflows (allocation exceeded arena size)

---

## Mesh & Gossip

### longbow_gossip_active_members

**Type**: Gauge  
**Description**: Number of active members in gossip cluster

### longbow_gossip_pings_total

**Type**: Counter  
**Labels**: `direction`  
**Description**: Total gossip ping operations (sent, received, failed)

### longbow_mesh_sync_deltas_total

**Type**: Counter  
**Labels**: `status`  
**Description**: Total mesh synchronization deltas

### longbow_mesh_sync_bytes_total

**Type**: Counter  
**Labels**: `direction`  
**Description**: Total bytes transferred in mesh sync

### longbow_mesh_merkle_match_total

**Type**: Counter  
**Labels**: `result`  
**Description**: Merkle tree consistency check results

### longbow_split_brain_healthy_peers

**Type**: Gauge  
**Description**: Number of healthy peers (split-brain detection)

### longbow_split_brain_partitions_total

**Type**: Counter  
**Description**: Total network partitions detected

### longbow_split_brain_heartbeats_total

**Type**: Counter  
**Labels**: `status`  
**Description**: Total split-brain heartbeats

### longbow_split_brain_fenced_state

**Type**: Gauge  
**Description**: Current fenced state (0=active, 1=fenced)

---

## Replication & Quorum

### longbow_replication_lag_seconds

**Type**: Gauge  
**Labels**: `node`  
**Description**: Replication lag in seconds

### longbow_replication_success_total

**Type**: Counter  
**Labels**: `node`  
**Description**: Successful replication operations

### longbow_replication_failures_total

**Type**: Counter  
**Labels**: `node`, `reason`  
**Description**: Failed replication operations

### longbow_replication_retries_total

**Type**: Counter  
**Labels**: `node`  
**Description**: Replication retry attempts

### longbow_replication_peers_total

**Type**: Gauge  
**Description**: Number of replication peers

### longbow_replication_queued_total

**Type**: Counter  
**Description**: Total operations queued for replication

### longbow_replication_queue_dropped_total

**Type**: Counter  
**Labels**: `reason`  
**Description**: Operations dropped from replication queue

### longbow_quorum_success_total

**Type**: Counter  
**Labels**: `consistency`  
**Description**: Successful quorum operations

### longbow_quorum_failure_total

**Type**: Counter  
**Labels**: `consistency`, `reason`  
**Description**: Failed quorum operations

### longbow_quorum_operation_duration_seconds

**Type**: Histogram  
**Labels**: `consistency`  
**Description**: Duration of quorum operations

---

**Total Metrics**: 188  
**Last Updated**: 2025-12-22
