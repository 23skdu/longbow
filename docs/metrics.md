# Longbow Metrics Reference

This document provides a comprehensive reference for all Prometheus metrics exported by the Longbow storage engine. These metrics are accessible via the `/metrics` endpoint (default port 9090).

> [!NOTE]
> This list is automatically synchronized with the Go source code to ensure 100% coverage of all system signals.

| Metric Name | Description |
|-------------|-------------|
| `longbow_active_search_contexts` | Number of currently active search contexts |
| `longbow_adaptive_gc_adjustments_total` | Total number of GOGC adjustments made by adaptive controller |
| `longbow_adaptive_gc_allocation_rate_bytes_per_sec` | Current memory allocation rate in bytes per second |
| `longbow_adaptive_gc_current_gogc` | Current GOGC value set by adaptive GC controller |
| `longbow_adaptive_gc_memory_pressure_ratio` | Current memory pressure ratio (0-1, where 1 is maximum pressure) |
| `longbow_adaptive_index_migrations_total` | Total number of adaptive index migrations |
| `longbow_adjacency_padding_bytes_total` | Total bytes used for alignment padding in graph adjacency |
| `longbow_allocator_allocations_active` | Current number of active memory allocations |
| `longbow_allocator_bytes_allocated_total` | Total bytes allocated by the memory allocator |
| `longbow_allocator_bytes_freed_total` | Total bytes freed by the memory allocator |
| `longbow_arena_alloc_bytes_total` | Total bytes allocated from search arenas |
| `longbow_arena_allocated_bytes` | Total bytes allocated in SlabArenas |
| `longbow_arena_allocations_total` | Total number of vector allocations from arena |
| `longbow_arena_bytes_allocated` | Total bytes allocated from arena |
| `longbow_arena_fast_path_failed_total` | Total number of fast path allocations that failed and fell back to slow path |
| `longbow_arena_fast_path_total` | Total number of allocations using the lock-free fast path |
| `longbow_arena_hit_rate` | Arena fast-path hit rate (0-1, higher is better) |
| `longbow_arena_memory_bytes` | Current bytes allocated in arena pools by size |
| `longbow_arena_overflow_total` | Total arena capacity overflow events requiring heap fallback |
| `longbow_arena_pool_gets_total` | Total arena acquisitions from global pool |
| `longbow_arena_pool_puts_total` | Total arena returns to global pool |
| `longbow_arena_resets_total` | Total arena reset operations |
| `longbow_arena_slab_allocations_total` | Total number of slab allocations in arena |
| `longbow_arena_slabs_total` | Total number of slabs allocated |
| `longbow_arena_slow_path_total` | Total number of allocations using the mutex-based slow path |
| `longbow_arrow_memory_used_bytes` | Current bytes used by Arrow memory allocators |
| `longbow_average_vector_norm` | Average L2 norm of vectors in the index |
| `longbow_batch_distance_batch_size` | Distribution of batch sizes in distance calculations |
| `longbow_batch_distance_calls_total` | Total number of batch distance calculation calls |
| `longbow_batch_distance_compute_duration_seconds` | Duration of batch distance compute operations |
| `longbow_batch_distance_compute_fallback_total` | Total number of batch operations falling back to scalar |
| `longbow_batch_distance_compute_pairs_total` | Total number of query-candidate pairs processed in batch |
| `longbow_batch_distance_compute_simd_used_total` | Total number of batch operations using SIMD optimization |
| `longbow_batch_distance_compute_total` | Total number of batch distance compute operations |
| `longbow_batch_distance_duration_seconds` | Duration of batch distance calculations |
| `longbow_batch_search_latency_seconds` | Latency of batch search operations |
| `longbow_batch_search_queries_total` | Total number of queries processed in batch searches |
| `longbow_batch_search_requests_total` | Total number of batch search requests |
| `longbow_binary_quantize_ops_total` | Total number of binary quantization operations |
| `longbow_bitmap_pool_discards_total` | Total number of bitmap pool discards (oversized) |
| `longbow_bitmap_pool_gets_total` | Total number of bitmap pool retrievals |
| `longbow_bitmap_pool_hits_total` | Total number of bitmap pool hits (reused) |
| `longbow_bitmap_pool_misses_total` | Total number of bitmap pool misses (allocations) |
| `longbow_bitmap_pool_puts_total` | Total number of bitmap pool returns |
| `longbow_bloom_false_positive_rate` | Estimated false positive rate of Bloom filters |
| `longbow_bloom_false_positives_total` | Total number of Bloom filter false positives |
| `longbow_bloom_filter_bitmap_zero_checks_total` | Total number of bitmap zero checks performed during filter evaluation |
| `longbow_bloom_filter_early_exits_total` | Total number of times Bloom filter optimization caused early exit (all rows rejected) |
| `longbow_bloom_filter_selectivity` | Distribution of estimated filter selectivity (0-1, where 1 means all rows match) |
| `longbow_bloom_hits_total` | Total number of Bloom filter hits (likely present) |
| `longbow_bloom_lookups_total` | Total number of Bloom filter lookups |
| `longbow_bloom_misses_total` | Total number of Bloom filter misses (definitely absent) |
| `longbow_bm25_documents_indexed_total` | Total number of documents indexed in BM25 |
| `longbow_boolean_filter_operations_total` | Total number of boolean filter operations by type |
| `longbow_bq_vectors_total` | Total number of vectors indexed with Binary Quantization |
| `longbow_brute_force_searches_total` | Total number of brute force searches |
| `longbow_buffer_pool_hits_total` | Total number of buffer pool hits |
| `longbow_buffer_pool_misses_total` | Total number of buffer pool misses |
| `longbow_buffer_pool_size_bytes` | Current size of buffer pool in bytes |
| `longbow_bulk_insert_dimension_errors_total` | Total number of dimension mismatch errors during bulk vector inserts |
| `longbow_checkpoint_barrier_reached_total` | Total number of checkpoint barriers reached |
| `longbow_checkpoint_epoch` | Current checkpoint epoch |
| `longbow_checkpoint_timeouts_total` | Total number of checkpoint timeouts |
| `longbow_checkpoints_total` | Total number of checkpoints created |
| `longbow_column_index_lookup_duration_seconds` | Latency of columnar index lookups |
| `longbow_column_index_size_bytes` | Size of on-disk columnar indexes in bytes |
| `longbow_compaction_auto_triggers_total` | Total number of auto-triggered compactions when batch count exceeds threshold |
| `longbow_compaction_batches_merged_total` | Total number of batches merged during compaction |
| `longbow_compaction_duration_seconds` | Duration of compaction jobs |
| `longbow_compaction_errors_total` | Total number of compaction errors |
| `longbow_compaction_events_total` | Total number of compaction events by type |
| `longbow_compaction_fragmented_batches_total` | Number of batches exceeding fragmentation threshold |
| `longbow_compaction_operations_total` | Total compaction operations by status |
| `longbow_compaction_rate_limit_wait_seconds` | Time spent waiting for compaction rate limiter |
| `longbow_compaction_records_processed_total` | Total number of records processed during compaction |
| `longbow_compaction_records_removed_total` | Total number of records removed during compaction |
| `longbow_compaction_runs_total` | Total number of compaction runs performed |
| `longbow_compaction_tombstone_density_ratio` | Current tombstone density per batch (0-1) |
| `longbow_compaction_triggers_total` | Total number of compaction triggers by reason |
| `longbow_compound_filter_depth` | Depth of compound filter expression trees |
| `longbow_compound_filter_ops_total` | Total compound filter operations by logic type |
| `longbow_compressed_vectors_sent_total` | Total number of quantized (SQ8/PQ) vectors sent in search results |
| `longbow_connection_pool_active_connections` | Current number of active connections in the pool |
| `longbow_connection_pool_close_total` | Total number of connections closed |
| `longbow_connection_pool_create_total` | Total number of new connections created |
| `longbow_connection_pool_get_duration_seconds` | Duration of connection pool get operations |
| `longbow_connection_pool_get_total` | Total number of connection pool get operations |
| `longbow_connection_pool_health_check_total` | Total number of connection health checks |
| `longbow_connection_pool_refresh_total` | Total number of connection refreshes due to health check failure |
| `longbow_correlation_id_active` | Currently active correlation IDs by operation |
| `longbow_correlation_id_total` | Total correlation IDs generated |
| `longbow_dataset_dimension_auto_detect_total` | Total number of dataset dimension auto-detection events |
| `longbow_dataset_dimension_mismatch_total` | Total number of vector dimension mismatch errors |
| `longbow_dataset_export_bytes` | Number of bytes exported |
| `longbow_dataset_export_duration_seconds` | Duration of dataset export in seconds |
| `longbow_dataset_export_empty_total` | Total number of empty dataset exports |
| `longbow_dataset_export_failures_total` | Total number of dataset export failures |
| `longbow_dataset_export_total` | Total number of dataset exports |
| `longbow_dataset_export_vectors` | Number of vectors exported |
| `longbow_dataset_import_bytes` | Number of bytes imported |
| `longbow_dataset_import_duration_seconds` | Duration of dataset import in seconds |
| `longbow_dataset_import_failures_total` | Total number of dataset import failures |
| `longbow_dataset_import_total` | Total number of dataset imports |
| `longbow_dataset_import_vectors` | Number of vectors imported |
| `longbow_dataset_lock_wait_duration_seconds` | Time spent waiting for dataset-level locks |
| `longbow_dataset_record_batches_count` | Number of record batches per dataset (high = fragmentation) |
| `longbow_dataset_update_retries_total` | Total number of retries during lock-free dataset map updates (CAS failures) |
| `longbow_dataset_vector_type_total` | Number of datasets by declared vector type |
| `longbow_dimension_buffer_bytes` | Buffer memory allocated per dimension |
| `longbow_disk_store_read_bytes_total` | Total bytes read from disk vector store |
| `longbow_disk_store_write_bytes_total` | Total bytes written to disk vector store |
| `longbow_do_exchange_batches_received_total` | Total number of record batches received via DoExchange |
| `longbow_do_exchange_batches_sent_total` | Total number of record batches sent via DoExchange |
| `longbow_do_exchange_calls_total` | Total number of DoExchange (gossip) calls |
| `longbow_do_exchange_duration_seconds` | Latency of DoExchange (gossip) operations |
| `longbow_do_exchange_errors_total` | Total number of failed DoExchange (gossip) calls |
| `longbow_do_exchange_search_duration_seconds` | Latency of DoExchange search operations |
| `longbow_do_exchange_search_total` | Total number of DoExchange search operations |
| `longbow_do_get_adaptive_chunks_total` | Total number of adaptive chunks created during DoGet operations |
| `longbow_do_get_chunk_size_bytes` | Distribution of chunk sizes returned by DoGet operations |
| `longbow_do_get_pipeline_steps_total` | Total number of DoGet pipeline steps executed |
| `longbow_do_get_time_to_first_chunk_seconds` | Time from DoGet start to receiving first chunk |
| `longbow_do_get_zero_copy_total` | Total number of zero-copy optimizations in DoGet |
| `longbow_do_put_payload_size_bytes` | Size of DoPut payloads in bytes |
| `longbow_doput_batch_latency_seconds` | Latency of DoPut batch processing |
| `longbow_doput_batch_size_bytes` | Payload size of each flushed DoPut batch |
| `longbow_doput_batch_size_vectors` | Number of vectors per DoPut batch |
| `longbow_embedding_generation_duration_seconds` | Time spent generating a batch of embeddings |
| `longbow_embedding_normalization_duration_seconds` | Time spent normalizing embedding vectors |
| `longbow_embedding_pooling_duration_seconds` | Time spent pooling token embeddings |
| `longbow_eof_normalisation_total` | Total number of stream EOF normalisations (healthy stream terminations detected) |
| `longbow_eviction_rejected_queries_total` | Total queries rejected because dataset was evicting |
| `longbow_evictions_total` | Total number of evicted records due to memory limits |
| `longbow_fast_path_usage_total` | Filter fast path usage count (fast/fallback) |
| `longbow_filter_complexity_score` | Complexity score of applied filters |
| `longbow_filter_early_exit_total` | Count of searches skipped due to all-zero filter bitmasks |
| `longbow_filter_evaluator_allocations_total` | Total number of allocations during filter evaluation |
| `longbow_filter_evaluator_duration_seconds` | Duration of filter evaluator operations |
| `longbow_filter_evaluator_ops_total` | Total number of filter evaluator operations |
| `longbow_filter_memory_usage_bytes` | Memory used by active filter state |
| `longbow_filter_optimization_total` | Total number of filter optimizations applied |
| `longbow_filter_selectivity_ratio` | Filter selectivity ratio (output rows / input rows) |
| `longbow_filter_vectorized_ops_total` | Total number of filter evaluations using SIMD paths |
| `longbow_flight_active_tickets` | Number of currently active Flight tickets |
| `longbow_flight_bytes_read_total` | Total bytes read from Flight tickets |
| `longbow_flight_bytes_written_total` | Total bytes written to Flight streams |
| `longbow_flight_duration_seconds` | Latency of Flight operations |
| `longbow_flight_ops_total` | Total number of Flight operations |
| `longbow_flight_pool_connections_active` | Number of active connections in the flight client pool |
| `longbow_flight_pool_connections_created_total` | Total number of flight client connections created |
| `longbow_flight_pool_connections_destroyed_total` | Total number of flight client connections destroyed |
| `longbow_flight_pool_wait_duration_seconds` | Time spent waiting for a flight client connection from pool |
| `longbow_flight_rows_processed_total` | Total number of rows processed by Flight service |
| `longbow_flight_stream_pool_size` | Size of the Flight stream writer pool |
| `longbow_flight_ticket_parse_duration_seconds` | Latency of flight ticket parsing |
| `longbow_gc_pause_duration_seconds` | Duration of GC pauses |
| `longbow_gc_tuner_gpu_utilization` | Current GPU utilization percentage (0-100) |
| `longbow_gc_tuner_heap_utilization` | Current heap utilization ratio (heap_inuse / limit) |
| `longbow_gc_tuner_target_gogc` | Current target GOGC value set by the tuner |
| `longbow_geo_index_points_total` | Total number of points stored in geospatial indexes |
| `longbow_geo_search_duration_seconds` | Duration of geospatial search operations in seconds |
| `longbow_geo_search_ops_total` | Total number of geospatial search operations |
| `longbow_get_neighbors_latency_seconds` | Latency of GetNeighbors operations |
| `longbow_get_neighbors_result_size` | Number of neighbors returned per GetNeighbors call |
| `longbow_get_neighbors_total` | Total number of GetNeighbors operations |
| `longbow_global_search_duration_seconds` | Latency of global search operations |
| `longbow_global_search_fanout_size` | Number of peers queried during global search |
| `longbow_global_search_partial_failures_total` | Total number of failed peer queries during global search |
| `longbow_gossip_active_members` | Current number of alive members in the gossip mesh |
| `longbow_gossip_pings_total` | Total number of gossip pings |
| `longbow_gpu_batch_size` | Current number of vectors pending in GPU batch |
| `longbow_gpu_compute_duration_seconds` | Time spent in GPU compute operations |
| `longbow_gpu_device_power_watts` | GPU device power consumption in Watts |
| `longbow_gpu_device_temperature_celsius` | GPU device temperature in Celsius |
| `longbow_gpu_device_utilization_percent` | GPU device utilization percentage (0-100) |
| `longbow_gpu_fallback_total` | Total number of GPU to CPU fallback events |
| `longbow_gpu_hnsw_build_batch_duration_seconds` | Duration of GPU HNSW build batch operations |
| `longbow_gpu_hnsw_build_batch_size` | Current batch size for GPU HNSW build |
| `longbow_gpu_hnsw_build_duration_seconds` | Duration of GPU-accelerated HNSW index building |
| `longbow_gpu_hnsw_build_fallback_total` | Total number of GPU HNSW build fallback events |
| `longbow_gpu_hnsw_build_operations_total` | Total number of GPU HNSW build operations |
| `longbow_gpu_hnsw_build_vectors_processed_total` | Total number of vectors processed during GPU HNSW build |
| `longbow_gpu_index_pool_active` | Number of active (checked out) GPU indexes |
| `longbow_gpu_index_pool_created_total` | Total number of GPU indexes created |
| `longbow_gpu_index_pool_idle` | Number of idle GPU indexes in the pool |
| `longbow_gpu_index_pool_reused_total` | Total number of times GPU indexes were reused from pool |
| `longbow_gpu_index_size` | Number of vectors stored in GPU index |
| `longbow_gpu_memory_bytes` | GPU memory usage in bytes |
| `longbow_gpu_operations_total` | Total number of GPU operations |
| `longbow_gpu_search_duration_seconds` | Duration of GPU search operations |
| `longbow_gpu_sync_duration_seconds` | Duration of GPU synchronization operations |
| `longbow_gpu_used_total` | Total number of search operations that used GPU acceleration |
| `longbow_graph_clustering_duration_seconds` | Duration of graph clustering operations |
| `longbow_graph_communities_total` | Total number of detected graph communities |
| `longbow_graph_f16_conversion_errors_total` | Total number of Float16 conversion errors |
| `longbow_graph_f16_memory_savings_bytes` | Memory saved by using Float16 instead of Float32 |
| `longbow_graph_f16_neighbors_total` | Total number of neighbors stored as Float16 |
| `longbow_graph_gpu_dispatch_fallback_total` | Total GraphRAG expansion calls falling back to CPU due to threshold |
| `longbow_graph_gpu_dispatch_total` | Total GraphRAG expansion calls to GPU backend |
| `longbow_graph_navigation_frontier_max_size` | Maximum size of the search frontier during traversal. |
| `longbow_graph_navigation_hops_total` | Number of hops in a successful graph navigation path. |
| `longbow_graph_navigation_latency_seconds` | Execution time of graph navigation operations. |
| `longbow_graph_navigation_nodes_visited_total` | Total number of unique nodes visited during traversal. |
| `longbow_graph_navigation_operations_total` | Total number of graph navigation operations (FindPath). |
| `longbow_graph_navigation_strategy_selection_total` | Total number of times a navigation strategy was selected by the planner. |
| `longbow_graph_rag_alpha_value` | Distribution of GraphRAG spreading activation alpha (damping) values |
| `longbow_graph_rag_depth_value` | Distribution of GraphRAG traversal depth values |
| `longbow_graph_rag_expanded_nodes_total` | Number of nodes returned after GraphRAG graph expansion |
| `longbow_graph_rag_operations_total` | Total number of GraphRAG spreading-activation operations |
| `longbow_graph_rag_rerank_latency_seconds` | Latency of the GraphRAG graph re-ranking phase |
| `longbow_graph_rag_seed_nodes_total` | Number of ANN seed nodes before GraphRAG graph expansion |
| `longbow_graph_store_edge_count` | Number of edges in GraphStore |
| `longbow_graph_store_export_bytes` | Size of GraphStore Arrow export in bytes |
| `longbow_graph_store_export_duration_seconds` | Duration of GraphStore Arrow export operations |
| `longbow_graph_store_export_total` | Total number of GraphStore export operations |
| `longbow_graph_store_import_duration_seconds` | Duration of GraphStore Arrow import operations |
| `longbow_graph_store_import_total` | Total number of GraphStore import operations |
| `longbow_graph_store_predicate_count` | Number of unique predicates in GraphStore |
| `longbow_graph_traversal_duration_seconds` | Duration of graph traversal operations |
| `longbow_graphrag_expansion_duration_seconds` | Detailed latency of the GraphRAG expansion step |
| `longbow_graphrag_nodes_visited_total` | Total number of unique nodes visited during GraphRAG expansion. |
| `longbow_grpc_call_duration_seconds` | Duration of gRPC calls |
| `longbow_grpc_call_total` | Total number of gRPC calls |
| `longbow_grpc_initial_conn_window_size_bytes` | Configured gRPC initial connection window size |
| `longbow_grpc_initial_window_size_bytes` | Configured gRPC initial window size |
| `longbow_grpc_max_concurrent_streams` | Configured gRPC maximum concurrent streams |
| `longbow_grpc_max_header_list_size` | Configured max header list size for gRPC |
| `longbow_grpc_max_recv_msg_size` | Configured max receive message size for gRPC |
| `longbow_grpc_max_recv_msg_size_bytes` | Configured gRPC maximum receive message size |
| `longbow_grpc_max_send_msg_size` | Configured max send message size for gRPC |
| `longbow_grpc_max_send_msg_size_bytes` | Configured gRPC maximum send message size |
| `longbow_grpc_messages_received_total` | Total number of gRPC messages received |
| `longbow_grpc_messages_sent_total` | Total number of gRPC messages sent |
| `longbow_grpc_stream_send_latency_seconds` | Latency of gRPC stream Send operations |
| `longbow_grpc_stream_stall_total` | Total number of gRPC stream stalls detected |
| `longbow_hnsw_active_readers` | Number of active zero-copy readers per dataset |
| `longbow_hnsw_adaptive_adjustments_total` | Total number of times M has been adjusted dynamically |
| `longbow_hnsw_adaptive_chunk_size` | Chunk sizes used in adaptive parallel search |
| `longbow_hnsw_adaptive_m_value` | Current value of M parameter in HNSW graph |
| `longbow_hnsw_arena_allocation_bytes_total` | Total bytes allocated in HNSW arenas per data type |
| `longbow_hnsw_arrow_extraction_errors_total` | Total number of errors encountered while extracting vectors from Arrow record batches |
| `longbow_hnsw_average_degree` | Average degree of nodes in the HNSW graph |
| `longbow_hnsw_avg_level_distribution` | Average number of nodes at each HNSW level |
| `longbow_hnsw_bitmap_filter_duration_seconds` | Time spent evaluating bitmap filters during HNSW search |
| `longbow_hnsw_bitmap_index_entries_total` | Number of entries in the HNSW bitmap metadata index |
| `longbow_hnsw_bitset_grow_total` | Total number of bitset grows during HNSW operations |
| `longbow_hnsw_branch_prediction_likely_total` | Total number of branches marked as likely (true) |
| `longbow_hnsw_branch_prediction_total` | Total number of branch predictions by type |
| `longbow_hnsw_branch_prediction_unlikely_total` | Total number of branches marked as unlikely (false) |
| `longbow_hnsw_bulk_insert_duration_seconds` | Duration of HNSW bulk vector insertion |
| `longbow_hnsw_bulk_insert_latency_by_dim_seconds` | Latency of HNSW bulk insert operations bucketed by dimension |
| `longbow_hnsw_bulk_insert_latency_by_type_seconds` | Latency of HNSW bulk insert operations bucketed by vector type |
| `longbow_hnsw_bulk_vectors_processed_total` | Total number of vectors processed in bulk operations |
| `longbow_hnsw_complex_ops_total` | Total number of complex number distance calculations |
| `longbow_hnsw_context_check_cancelled_total` | Total number of times context check detected cancellation |
| `longbow_hnsw_context_check_total` | Total number of context checks performed during traversal |
| `longbow_hnsw_disconnected_components` | Number of disconnected components in the HNSW graph |
| `longbow_hnsw_distance_calculations_f16_total` | Total number of F16 distance calculations |
| `longbow_hnsw_distance_calculations_total` | Total HNSW distance calculations performed |
| `longbow_hnsw_early_termination_total` | Total number of HNSW searches that hit the visited nodes budget |
| `longbow_hnsw_epoch_transitions_total` | Total HNSW epoch transitions for zero-copy access |
| `longbow_hnsw_estimated_diameter` | Estimated diameter (max BFS depth) of the HNSW graph |
| `longbow_hnsw_fuzz_crash_recovered_total` | Total number of recovered crashes during fuzz testing |
| `longbow_hnsw_graph_height` | Maximum layer height of the HNSW graph (search complexity) |
| `longbow_hnsw_graph_node_allocations_total` | Total number of HNSW graph node allocations |
| `longbow_hnsw_graph_sync_delta_applies_total` | Total number of graph sync deltas applied |
| `longbow_hnsw_graph_sync_deltas_total` | Total number of graph sync deltas generated |
| `longbow_hnsw_graph_sync_exports_total` | Total number of graph sync exports |
| `longbow_hnsw_graph_sync_imports_total` | Total number of graph sync imports |
| `longbow_hnsw_index_growth_duration_seconds` | Time spent growing the HNSW index capacity |
| `longbow_hnsw_index_type_count` | Total number of HNSW indices created by data type |
| `longbow_hnsw_ingestion_throughput_vectors_per_second` | Rate of vector ingestion in vectors per second |
| `longbow_hnsw_insert_duration_seconds` | Duration of HNSW vector insertion |
| `longbow_hnsw_insert_latency_by_dim_seconds` | Latency of HNSW insert operations bucketed by dimension |
| `longbow_hnsw_insert_latency_by_type_seconds` | Latency of HNSW insert operations bucketed by vector type |
| `longbow_hnsw_insert_ops_total` | Total number of HNSW insertion operations |
| `longbow_hnsw_insert_pool_get_total` | Total number of insert contexts retrieved from pool |
| `longbow_hnsw_insert_pool_new_total` | Total number of new insert contexts created |
| `longbow_hnsw_insert_pool_put_total` | Total number of insert contexts returned to pool |
| `longbow_hnsw_intrinsic_dimensionality` | Estimated intrinsic dimensionality of the data |
| `longbow_hnsw_max_component_size` | Size of the largest connected component in the HNSW graph |
| `longbow_hnsw_memory_usage_bytes` | Memory usage of HNSW index components |
| `longbow_hnsw_node_count` | Total number of nodes in the HNSW graph |
| `longbow_hnsw_nodes_added_total` | Total number of nodes added to HNSW |
| `longbow_hnsw_nodes_skipped_total` | Total number of HNSW nodes skipped due to early-exit filtering |
| `longbow_hnsw_nodes_visited` | Number of HNSW nodes visited per search |
| `longbow_hnsw_orphan_nodes` | Number of orphan nodes (degree 0) in the HNSW graph |
| `longbow_hnsw_parallel_search_efficiency` | Efficiency ratio (work per worker) in parallel search |
| `longbow_hnsw_parallel_search_splits_total` | Total number of parallel search splits |
| `longbow_hnsw_parallel_search_worker_count` | Number of workers used in parallel search |
| `longbow_hnsw_polymorphic_latency_seconds` | Latency of search operations by polymorphic vector type |
| `longbow_hnsw_polymorphic_search_count` | Total number of searches by polymorphic vector type |
| `longbow_hnsw_polymorphic_throughput_bytes` | Total bytes processed during polymorphic search |
| `longbow_hnsw_pq_compressed_bytes_total` | Total number of bytes stored in PQ compressed format |
| `longbow_hnsw_pq_enabled` | Whether Product Quantization is enabled (1) or disabled (0) for the dataset |
| `longbow_hnsw_pq_training_duration_seconds` | Time taken to train PQ encoder for a dataset |
| `longbow_hnsw_pq_training_triggered_total` | Total number of auto-triggered PQ training events |
| `longbow_hnsw_prefiltered_searches_total` | Total number of HNSW searches using pre-filter optimization |
| `longbow_hnsw_prewarm_duration_seconds` | Time taken to page-fault HNSW memory into RAM |
| `longbow_hnsw_prewarm_total` | Total number of HNSW index prewarm operations executed |
| `longbow_hnsw_range_search_duration_seconds` | Duration of range search operations |
| `longbow_hnsw_range_search_ops_total` | Total number of range search operations |
| `longbow_hnsw_range_search_results_total` | Total number of results returned by range search |
| `longbow_hnsw_refine_throughput_total` | Total number of vectors refined during search |
| `longbow_hnsw_repair_duration_seconds` | Duration of tombstone repair cycles |
| `longbow_hnsw_repair_failure_total` | Total number of failed HNSW graph repairs |
| `longbow_hnsw_repair_last_scan_timestamp_seconds` | Unix timestamp of last repair scan |
| `longbow_hnsw_repair_nodes_visited_total` | Total number of nodes visited during repair operations |
| `longbow_hnsw_repair_orphans_detected_total` | Total number of orphaned nodes detected by repair agent |
| `longbow_hnsw_repair_orphans_repaired_total` | Total number of orphaned nodes repaired by repair agent |
| `longbow_hnsw_repair_scan_duration_seconds` | Time spent scanning for orphaned nodes |
| `longbow_hnsw_repair_success_total` | Total number of successful HNSW graph repairs |
| `longbow_hnsw_repair_total` | Total number of tombstone repairs performed |
| `longbow_hnsw_repaired_connections_total` | Number of connections re-wired from tombstones to valid nodes |
| `longbow_hnsw_resizes_total` | Total number of HNSW graph resizes |
| `longbow_hnsw_search_duration_seconds` | Duration of HNSW search operations |
| `longbow_hnsw_search_early_exits_total` | Total number of HNSW search early exits by reason |
| `longbow_hnsw_search_early_terminations_total` | Total number of HNSW searches that terminated early due to optimization |
| `longbow_hnsw_search_latency_by_dim_seconds` | Latency of HNSW search operations bucketed by dimension |
| `longbow_hnsw_search_latency_by_type_seconds` | Latency of HNSW search operations bucketed by vector type |
| `longbow_hnsw_search_ops_total` | Total number of HNSW search operations |
| `longbow_hnsw_search_phase_duration_seconds` | Duration of HNSW search phases |
| `longbow_hnsw_search_pool_get_total` | Total number of search contexts retrieved from pool |
| `longbow_hnsw_search_pool_new_total` | Total number of new search contexts created |
| `longbow_hnsw_search_pool_put_total` | Total number of search contexts returned to pool |
| `longbow_hnsw_search_queries_total` | Total number of HNSW search queries executed, labeled by dimensions |
| `longbow_hnsw_search_scratch_space_resizes_total` | Total number of scratch space resizes during HNSW search |
| `longbow_hnsw_search_throughput_dims_total` | Total number of HNSW searches bucketed by dimension |
| `longbow_hnsw_searches_total` | Total number of HNSW index searches |
| `longbow_hnsw_serial_fallback_total` | Total number of serial fallback decisions |
| `longbow_hnsw_sharding_migrations_total` | Total number of HNSW index migrations to sharded format |
| `longbow_hnsw_simd_dispatch_latency_seconds` | Latency of dynamic SIMD kernel dispatch by data type |
| `longbow_hnsw_traversal_iterations_total` | Total number of iterations during HNSW graph traversal |
| `longbow_hnsw_vector_allocated_bytes_total` | Total bytes allocated for HNSW vector storage |
| `longbow_hnsw_vector_allocations_total` | Total number of vector allocations for HNSW graph storage |
| `longbow_hnsw_visited_reset_duration_seconds` | Time spent resetting HNSW visited set |
| `longbow_hybrid_bm25_arena_bytes` | Memory used by BM25 arena storage |
| `longbow_hybrid_bm25_posting_list_size` | Size of BM25 posting lists |
| `longbow_hybrid_bm25_tokens_total` | Total number of tokens indexed in BM25 |
| `longbow_hybrid_dense_result_ratio` | Fraction of top-K results sourced from dense ANN search (0–1) |
| `longbow_hybrid_graph_rerank_enabled_total` | Total number of hybrid searches with graph re-ranking enabled or disabled |
| `longbow_hybrid_graph_rerank_latency_seconds` | Duration of the graph re-ranking phase in hybrid search |
| `longbow_hybrid_result_origin_total` | Per-result provenance counter for hybrid search (dense, sparse, graph_expanded) |
| `longbow_hybrid_rrf_fusion_latency_seconds` | Duration of the Reciprocal Rank Fusion phase in hybrid search |
| `longbow_hybrid_search_bm25_duration_seconds` | Duration of BM25 scoring in hybrid search |
| `longbow_hybrid_search_cache_hits_total` | Total number of hybrid search cache hits |
| `longbow_hybrid_search_cache_misses_total` | Total number of hybrid search cache misses |
| `longbow_hybrid_search_duration_seconds` | Total duration of hybrid search operations |
| `longbow_hybrid_search_keyword_total` | Total number of hybrid searches triggered by keyword query |
| `longbow_hybrid_search_merge_duration_seconds` | Duration of result merging (RRF) in hybrid search |
| `longbow_hybrid_search_vector_duration_seconds` | Duration of vector search in hybrid search |
| `longbow_hybrid_search_vector_total` | Total number of hybrid searches triggered by vector query |
| `longbow_hybrid_sparse_result_ratio` | Fraction of top-K results sourced from sparse BM25 search (0–1) |
| `longbow_id_resolution_duration_seconds` | Latency of resolving internal IDs to user IDs |
| `longbow_index_build_duration_seconds` | Duration of index building operations |
| `longbow_index_build_latency_seconds` | Latency of vector index build operations |
| `longbow_index_creation_duration_seconds` | Duration of index creation operations |
| `longbow_index_creations_total` | Total number of index creation attempts |
| `longbow_index_job_latency_seconds` | Latency of index job processing by dataset |
| `longbow_index_jobs_dropped_total` | Total number of index jobs dropped due to queue overflow |
| `longbow_index_jobs_overflow_total` | Total number of index jobs sent to overflow buffer or retried asynchronously |
| `longbow_index_lock_wait_duration_seconds` | Time spent waiting for HNSW index structure locks |
| `longbow_index_migration_duration_seconds` | Duration of index migration operations |
| `longbow_index_queue_depth` | Current depth of the indexing queue (lag indicator) |
| `longbow_index_sync_delta_total` | Total number of vectors synchronized via delta-sync |
| `longbow_index_types_registered` | Total number of registered index types |
| `longbow_indexing_latency_traced_seconds` | Indexing operation latency with distributed tracing |
| `longbow_indexing_paused_duration_seconds` | Total time background graph maintenance is delayed due to ingestion bursts |
| `longbow_ingestion_duration_seconds` | Latency of high-throughput ingestion operations |
| `longbow_ingestion_lag_count` | Total number of records waiting to be ingested |
| `longbow_ingestion_queue_depth` | Current number of batches waiting in the ingestion queue |
| `longbow_ingestion_queue_latency_seconds` | Time spent in the ingestion queue before processing |
| `longbow_ingestion_records_total` | Total number of records ingested |
| `longbow_insert_mu_wait_duration_seconds` | Time spent waiting for insertMus sharded locks |
| `longbow_inverted_index_postings_total` | Total number of postings in inverted indexes |
| `longbow_inverted_index_size_bytes` | Size of inverted index in bytes |
| `longbow_io_fsync_duration_seconds` | Latency of fsync syscalls |
| `longbow_io_latency_traced_seconds` | I/O operation latency with distributed tracing |
| `longbow_io_page_cache_usage_bytes` | Estimated resident set size (RSS) attributed to file mappings |
| `longbow_io_read_bytes_total` | Total bytes read from disk storage |
| `longbow_io_read_latency_seconds` | Latency of I/O read operations by backend |
| `longbow_io_read_ops_total` | Total read operations (syscalls) |
| `longbow_io_write_bytes_total` | Total bytes written to disk storage |
| `longbow_io_write_latency_seconds` | Latency of I/O write operations by backend |
| `longbow_io_write_ops_total` | Total write operations (syscalls) |
| `longbow_ipc_buffer_pool_evictions_total` | Total number of IPC buffer pool evictions |
| `longbow_ipc_buffer_pool_hits_total` | Total number of IPC buffer pool hits |
| `longbow_ipc_buffer_pool_misses_total` | Total number of IPC buffer pool misses |
| `longbow_ipc_buffer_pool_size` | Current size of the IPC buffer pool |
| `longbow_ipc_buffer_pool_utilization` | Current utilization of the IPC buffer pool (0-1) |
| `longbow_ivf_cluster_search_total` | Total number of clusters searched in IVF-PQ/OPQ |
| `longbow_ivf_load_balance_ratio` | Ratio of max cluster size to average cluster size (1.0 = perfect balance) |
| `longbow_ivfopq_lookup_hits_total` | Total number of ADC lookup cache hits in IVF-OPQ |
| `longbow_ivfopq_pqcodes_per_cluster` | Number of PQ codes per cluster in IVF-OPQ |
| `longbow_jit_compilation_duration_seconds` | Time spent compiling JIT kernels |
| `longbow_jit_kernel_calls_total` | Total number of JIT kernel function calls |
| `longbow_jit_kernel_errors_total` | Total number of JIT kernel execution errors |
| `longbow_kernel_performance_regression_ratio` | Ratio of current performance to baseline (1.0 = no change, <1.0 = regression) |
| `longbow_learned_index_adaptation_duration_seconds` | Duration of index adaptation (migration) process. |
| `longbow_learned_index_adaptation_latency_gain_ms` | Observed latency delta (before_ms - after_ms) after a completed index adaptation. Positive = improvement. |
| `longbow_learned_index_adaptations_total` | Total number of index adaptation events by lifecycle status. |
| `longbow_learned_index_knn_duration_seconds` | Wall-clock time for one k-NN scoring pass over the training sample buffer. |
| `longbow_learned_index_prediction_correct_total` | Total number of learned index predictions confirmed correct by subsequent training sample feedback. |
| `longbow_learned_index_predictions_total` | Total number of index-type recommendations issued by the learned index, labeled by chosen index and scoring method. |
| `longbow_learned_index_sample_overflow_total` | Total number of times the training sample buffer exceeded 10,000 entries and oldest samples were evicted. |
| `longbow_learned_index_training_samples_total` | Current number of training samples held in the learned index buffer (max 10,000). |
| `longbow_learned_index_weight_update_duration_seconds` | Wall-clock time for one online feature-weight update (LDA between-class variance). |
| `longbow_load_balancer_replicas_total` | Total number of replicas tracked by load balancer |
| `longbow_load_balancer_selections_total` | Total number of replica selections for read operations |
| `longbow_load_balancer_unhealthy_total` | Total number of unhealthy replicas |
| `longbow_lock_contention_duration_seconds` | Time spent waiting for generic instrumented locks |
| `longbow_lock_node_spin_cycles_total` | Total number of spin cycles performed by LockNode spinlocks |
| `longbow_memory_backpressure_acquires_total` | Total number of memory permits acquired |
| `longbow_memory_backpressure_rejects_total` | Total number of requests rejected due to memory backpressure |
| `longbow_memory_backpressure_releases_total` | Total number of memory permits released |
| `longbow_memory_current_bytes` | Current memory usage in bytes |
| `longbow_memory_evictions_triggered_total` | Total number of evictions triggered by write operations |
| `longbow_memory_fragmentation_ratio` | Ratio of system memory reserved vs used (fragmentation indicator) |
| `longbow_memory_heap_in_use_bytes` | Current heap memory in use |
| `longbow_memory_limit_bytes` | Configured memory limit in bytes |
| `longbow_memory_limit_rejects_total` | Total number of writes rejected due to memory limit |
| `longbow_memory_pressure_level` | Current memory pressure level (0-100) |
| `longbow_memory_utilization` | Memory utilization (currentMemory / maxMemory) |
| `longbow_mesh_merkle_match_total` | Total Merkle root comparison results |
| `longbow_mesh_sync_bytes_total` | Total bytes replicated via mesh sync |
| `longbow_mesh_sync_deltas_total` | Total number of record batches replicated via mesh sync |
| `longbow_metal_add_duration_seconds` | Duration of Metal add operations |
| `longbow_metal_add_operations_total` | Total number of Metal add operations |
| `longbow_metal_add_vectors_processed_total` | Total number of vectors added via Metal operations |
| `longbow_metal_index_dimensions` | Number of dimensions in Metal index |
| `longbow_metal_index_vectors` | Number of vectors stored in Metal index |
| `longbow_metal_init_duration_seconds` | Duration of Metal GPU initialization |
| `longbow_metal_init_operations_total` | Total number of Metal initialization operations |
| `longbow_metal_memory_bytes` | Metal GPU memory usage in bytes |
| `longbow_metal_search_duration_seconds` | Duration of Metal search operations |
| `longbow_metal_search_operations_total` | Total number of Metal search operations |
| `longbow_metal_search_vectors_processed_total` | Total number of vectors processed by Metal search operations |
| `longbow_metal_shader_compile_duration_seconds` | Duration of Metal shader compilation |
| `longbow_metal_shader_compile_total` | Total number of Metal shader compilation attempts |
| `longbow_metal_shader_kernel_count` | Number of Metal shader kernels compiled |
| `longbow_multi_gpu_device_errors` | Number of errors on each GPU device |
| `longbow_multi_gpu_device_queries` | Number of queries processed by each GPU device |
| `longbow_multi_gpu_fallback_total` | Total number of multi-GPU fallback events |
| `longbow_multi_gpu_queries_total` | Total number of multi-GPU queries |
| `longbow_multi_gpu_query_duration_seconds` | Duration of multi-GPU query operations |
| `longbow_multi_gpu_replicate_duration_seconds` | Duration of multi-GPU replication operations |
| `longbow_multi_gpu_replicate_operations_total` | Total number of multi-GPU replication operations |
| `longbow_multi_gpu_replicate_vectors_processed_total` | Total number of vectors replicated across GPUs |
| `longbow_multi_gpu_total_devices` | Total number of GPU devices in multi-GPU setup |
| `longbow_namespace_cache_hits_total` | Total cache hits per namespace |
| `longbow_namespace_cache_misses_total` | Total cache misses per namespace |
| `longbow_namespace_creation_total` | Total number of successful namespace creations |
| `longbow_namespace_datasets_total` | Total number of datasets in a namespace |
| `longbow_namespace_ingest_rate_vectors_per_sec` | Ingestion rate (vectors per second) per namespace |
| `longbow_namespace_queries_total` | Total number of queries per namespace |
| `longbow_namespace_query_latency_seconds` | Query latency per namespace |
| `longbow_namespace_quota_limit` | Quota limit per namespace (0 = unlimited) |
| `longbow_namespace_quota_used` | Quota usage per namespace |
| `longbow_namespace_rate_limit_hits_total` | Total number of rate-limited requests per namespace |
| `longbow_namespace_storage_bytes` | Current storage usage per namespace |
| `longbow_namespace_vector_count` | Current vector count per namespace |
| `longbow_namespaces_total` | Total number of active namespaces |
| `longbow_neighbor_selection_errors_total` | Total number of errors during neighbor selection operations |
| `longbow_nested_field_filter_ops_total` | Total filter operations on nested field paths |
| `longbow_null_filter_operations_total` | Total number of null filter operations by type |
| `longbow_numa_cross_node_access_total` | Total number of memory accesses where worker node != data node |
| `longbow_numa_enabled` | Whether NUMA awareness is enabled (1 = enabled, 0 = disabled) |
| `longbow_numa_node_count` | Number of NUMA nodes detected |
| `longbow_numa_worker_distribution` | Number of workers pinned to each NUMA node |
| `longbow_numeric_filter_operations_total` | Total number of numeric filter operations by type |
| `longbow_onnx_inference_duration_seconds` | Duration of ONNX inference operations |
| `longbow_onnx_inference_errors_total` | Total number of ONNX inference errors |
| `longbow_onnx_metal_batch_size` | Batch size for ONNX Metal inference |
| `longbow_onnx_metal_inference_duration_seconds` | Duration of ONNX Metal inference operations |
| `longbow_onnx_metal_inference_errors_total` | Total number of ONNX Metal inference errors |
| `longbow_onnx_metal_inference_requests_total` | Total number of ONNX Metal inference requests |
| `longbow_onnx_metal_memory_used_bytes` | Memory currently used by ONNX Metal engine |
| `longbow_onnx_metal_model_load_duration_seconds` | Duration of ONNX model loading |
| `longbow_onnx_metal_model_loaded` | Whether ONNX Metal model is loaded (1 = yes, 0 = no) |
| `longbow_onnx_metal_tensor_allocations_total` | Total number of ONNX Metal tensor allocations |
| `longbow_onnx_model_load_duration_seconds` | Duration of ONNX model loading |
| `longbow_opq_encoder_warmup_duration_seconds` | Time spent warming up the OPQ encoder during training |
| `longbow_panic_total` | Total number of panics recovered |
| `longbow_parallel_reduction_vectors_processed_total` | Total number of vectors processed using parallel reduction optimizations |
| `longbow_parser_pool_gets_total` | Total number of parser pool retrievals |
| `longbow_parser_pool_hits_total` | Total number of parser pool hits (reused) |
| `longbow_parser_pool_misses_total` | Total number of parser pool misses (allocations) |
| `longbow_parser_pool_puts_total` | Total number of parser pool returns |
| `longbow_peer_health_status` | Peer health status (0=down, 1=up) |
| `longbow_pipeline_batches_per_second` | Current rate of record batches processed per second |
| `longbow_pipeline_duration_seconds` | Duration of pipeline stages |
| `longbow_pipeline_operations_total` | Total number of pipeline operations |
| `longbow_pipeline_utilization_total` | Total number of pipeline activations (misnamed as utilization) |
| `longbow_pipeline_worker_utilization` | Utilization of pipeline workers (0-1) |
| `longbow_pool_lock_wait_duration_seconds` | Time spent waiting for connection pool locks |
| `longbow_popcnt_distance_ops_total` | Total number of POPCNT distance calculations |
| `longbow_pq_encoding_duration_seconds` | Latency of PQ encoding operations |
| `longbow_pq_operations_total` | Total number of PQ operations |
| `longbow_pq_training_duration_seconds` | Latency of PQ training operations |
| `longbow_prefetch_operations_total` | Total number of software prefetch instructions issued during search |
| `longbow_proxy_request_latency_seconds` | Latency of forwarded requests |
| `longbow_proxy_requests_forwarded_total` | Total number of requests forwarded to other nodes |
| `longbow_quadtree_subdivisions_total` | Total number of quadtree subdivisions (node splits) |
| `longbow_quantization_active_type` | Current active quantization type for the dataset (1 if active). |
| `longbow_quantization_memory_savings_bytes` | Estimated memory savings in bytes achieved through quantization. |
| `longbow_quantization_recall_estimate` | Estimated search recall for quantized index compared to full precision. |
| `longbow_quantization_switches_total` | Total number of quantization type transitions triggered by auto-tuning. |
| `longbow_query_cache_evictions_total` | Total number of query cache evictions |
| `longbow_query_cache_hits_total` | Total number of query cache hits |
| `longbow_query_cache_misses_total` | Total number of query cache misses |
| `longbow_query_cache_ops_total` | Total number of query cache operations |
| `longbow_query_cache_size` | Current number of entries in query cache |
| `longbow_quorum_failure_total` | Total number of failed quorum operations |
| `longbow_quorum_operation_duration_seconds` | Duration of quorum operations |
| `longbow_quorum_success_total` | Total number of successful quorum operations |
| `longbow_range_filter_operations_total` | Total number of range filter operations by type |
| `longbow_rate_limit_requests_total` | Total number of rate limited requests |
| `longbow_raw_vectors_sent_total` | Total number of raw (F32/F16) vectors sent in search results |
| `longbow_rdma_bytes_processed_total` | The total number of bytes processed via RDMA transport |
| `longbow_rdma_errors_total` | The total number of RDMA-related errors encountered |
| `longbow_recommendations_latency_seconds` | Latency of Recommend operations |
| `longbow_recommendations_seed_count` | Number of seeds provided per Recommend request |
| `longbow_recommendations_total` | Total number of Recommend operations |
| `longbow_record_access_total` | Total number of record accesses (LRU tracking) |
| `longbow_record_metadata_entries` | Number of entries in record eviction metadata map |
| `longbow_remote_storage_download_bytes_total` | Total bytes downloaded from remote storage |
| `longbow_remote_storage_duration_seconds` | Duration of remote storage operations |
| `longbow_remote_storage_ops_total` | Total number of remote storage operations |
| `longbow_remote_storage_upload_bytes_total` | Total bytes uploaded to remote storage |
| `longbow_replication_failures_total` | Total number of replication failures |
| `longbow_replication_lag_seconds` | Replication lag in seconds by peer |
| `longbow_replication_peers_total` | Total number of replication peers |
| `longbow_replication_queue_dropped_total` | Total number of operations dropped from replication queue |
| `longbow_replication_queued_total` | Total number of operations queued for replication |
| `longbow_replication_retries_total` | Total number of replication retries |
| `longbow_replication_success_total` | Total number of successful replication operations |
| `longbow_requantization_duration_seconds` | Time taken to re-quantize a dataset in the background. |
| `longbow_reranker_batch_size` | Batch size for reranker operations |
| `longbow_reranker_errors_total` | Total number of reranker errors |
| `longbow_reranker_inference_duration_seconds` | Duration of reranker inference operations |
| `longbow_reranker_scores_computed_total` | Total number of reranker scores computed |
| `longbow_result_pool_hits_total` | Total number of result object pool hits |
| `longbow_result_pool_misses_total` | Total number of result object pool misses |
| `longbow_s3_operations_total` | Total number of S3 operations |
| `longbow_s3_request_duration_seconds` | Duration of S3 operations |
| `longbow_s3_retries_total` | Total number of S3 operation retries |
| `longbow_schema_columns_added_total` | Total number of columns added via schema evolution |
| `longbow_schema_columns_dropped_total` | Total number of columns dropped via schema evolution |
| `longbow_schema_evolution_duration_seconds` | Duration of schema evolution operations |
| `longbow_schema_evolution_total` | Total number of schema evolution operations |
| `longbow_schema_version_current` | Current schema version |
| `longbow_search_allocation_bytes` | Memory allocated during search operations in bytes |
| `longbow_search_consistency_level_total` | Total number of vector searches by consistency level |
| `longbow_search_dimension_distribution_total` | Distribution of search queries by dimension |
| `longbow_search_latency_by_dimension_seconds` | Search latency in seconds, bucketed by dimension |
| `longbow_search_latency_seconds` | Latency of search operations by type |
| `longbow_search_latency_traced_seconds` | Search operation latency with distributed tracing |
| `longbow_search_p50_latency_ms` | P50 search latency in milliseconds by dimension |
| `longbow_search_p99_latency_ms` | P99 search latency in milliseconds by dimension |
| `longbow_search_qps_by_dimension` | Current measured QPS by dimension |
| `longbow_search_requests_total` | Total number of search requests processed |
| `longbow_search_result_pool_get_total` | Total number of result slices retrieved from the pool |
| `longbow_search_result_pool_hits_total` | Total number of pool hits (reused slices) |
| `longbow_search_result_pool_misses_total` | Total number of pool misses (new allocations) |
| `longbow_search_result_pool_put_total` | Total number of result slices returned to the pool |
| `longbow_search_strong_mode_latency_seconds` | Latency of searches running in strong consistency mode |
| `longbow_semaphore_acquired_total` | Total number of semaphore acquisitions |
| `longbow_semaphore_active_requests` | Number of requests currently holding semaphore |
| `longbow_semaphore_queue_duration_seconds` | Time spent waiting in semaphore queue |
| `longbow_semaphore_timeouts_total` | Total number of semaphore acquisition timeouts |
| `longbow_semaphore_waiting_requests` | Number of requests waiting for semaphore |
| `longbow_shard_lock_wait_duration_seconds` | Time spent waiting for shard-level locks |
| `longbow_sharded_hnsw_load_factor` | Sharded HNSW load factor by shard (0-1) |
| `longbow_sharded_hnsw_shard_size` | Number of vectors in each HNSW shard |
| `longbow_sharded_hnsw_shard_split_total` | Total number of HNSW shard split events |
| `longbow_simd_activation_duration_seconds` | Latency of SIMD activation kernels (exp, log, softmax, sigmoid) |
| `longbow_simd_activation_kernel_calls_total` | Total number of SIMD activation kernel calls |
| `longbow_simd_activation_kernel_duration_seconds` | Duration of SIMD activation kernel execution |
| `longbow_simd_blocked_processing_total` | Total number of times blocked SIMD processing was used |
| `longbow_simd_cosine_batch_calls_total` | Total number of batched cosine distance calculations |
| `longbow_simd_dispatch_count` | Total number of dynamic SIMD instruction dispatches |
| `longbow_simd_dispatch_total` | Total number of SIMD dispatch calls by implementation |
| `longbow_simd_dot_product_batch_calls_total` | Total number of batched dot product calculations |
| `longbow_simd_enabled` | Whether SIMD acceleration is enabled for the architecture (1=yes, 0=no) |
| `longbow_simd_f16_ops_total` | Total number of FP16 SIMD operations explicitly dispatched |
| `longbow_simd_fallback_total` | Total number of times SIMD fell back to generic implementation |
| `longbow_simd_kernel_duration_seconds` | SIMD kernel execution duration in seconds |
| `longbow_simd_kernel_operations_total` | Total number of SIMD kernel operations |
| `longbow_simd_operations_total` | Total number of SIMD-accelerated operations |
| `longbow_simd_static_dispatch_type` | Type of SIMD implementation statically dispatched (0=Generic, 1=NEON, 2=AVX2, 3=AVX512) |
| `longbow_simd_tiled_distance_batch_total` | Total number of tiled distance batch operations performed for high-dim vectors (>1024 dims) |
| `longbow_slab_fragmentation_ratio` | Fragmentation ratio for slab pools (pooled/active) |
| `longbow_slab_pool_allocations_total` | Total number of slab allocations (both pooled and new) |
| `longbow_snapshot_duration_seconds` | Duration of snapshot creation operations |
| `longbow_snapshot_operations_total` | Total number of snapshot operations |
| `longbow_snapshot_rate_limit_wait_seconds` | Time spent waiting for snapshot rate limiter |
| `longbow_snapshot_size_bytes` | Size of generated Parquet snapshots in bytes |
| `longbow_snapshot_write_duration_seconds` | Duration of Parquet snapshot write operations |
| `longbow_split_brain_fenced_state` | Whether the node is currently fenced (1=fenced, 0=normal) |
| `longbow_split_brain_healthy_peers` | Current number of healthy peers seen by detector |
| `longbow_split_brain_heartbeats_total` | Total number of split brain detector heartbeats |
| `longbow_split_brain_partitions_total` | Total number of partition events detected |
| `longbow_store_active_datasets` | Current number of active datasets in memory |
| `longbow_store_circuit_breaker_failures_total` | Total number of failed requests passing through breaker |
| `longbow_store_circuit_breaker_rejections_total` | Total number of requests rejected by store circuit breaker |
| `longbow_store_circuit_breaker_state_changes_total` | Total number of circuit breaker state changes |
| `longbow_store_circuit_breaker_successes_total` | Total number of successful requests passing through breaker |
| `longbow_store_dropped_datasets_total` | Total number of datasets explicitly dropped |
| `longbow_store_vectors_managed_count` | Total number of vectors stored in managed arenas (SlabArena) |
| `longbow_stream_termination_errors_total` | Total number of unexpected stream termination errors (non-EOF) |
| `longbow_string_filter_bytes_compared_total` | Total number of bytes compared during string filtering |
| `longbow_string_filter_comparisons_total` | Total number of string comparisons performed |
| `longbow_string_filter_duration_seconds` | Duration of string filter operations |
| `longbow_string_filter_equal_length_total` | Total number of string filters using equal-length fast path |
| `longbow_string_filter_operations_total` | Total number of string filter operations by type |
| `longbow_string_filter_ops_total` | Total number of string filter operations |
| `longbow_stub_model_usage_total` | Total number of times a stub embedding model was used |
| `longbow_system_disk_read_bytes_per_second` | System-wide disk read throughput (from /proc/diskstats) |
| `longbow_system_disk_write_bytes_per_second` | System-wide disk write throughput (from /proc/diskstats) |
| `longbow_tcp_nodelay_connections_total` | Total number of TCP connections with NoDelay set |
| `longbow_tombstones_total` | Total number of active tombstones |
| `longbow_tpu_core_utilization_ratio` | TPU core utilization ratio (0.0 to 1.0) |
| `longbow_tpu_d2d_latency_seconds` | TPU die-to-die (D2D) interconnect latency in seconds |
| `longbow_tpu_hbm_usage_bytes` | TPU High Bandwidth Memory usage in bytes |
| `longbow_tpu_inference_duration_seconds` | TPU inference duration in seconds |
| `longbow_tq2_codes_per_vector` | Number of codes per vector for TurboQuant2 (2-bit) |
| `longbow_tq2_decode_duration_seconds` | Time spent decoding vectors with TurboQuant2 (2-bit) |
| `longbow_tq2_encode_duration_seconds` | Time spent encoding vectors with TurboQuant2 (2-bit) |
| `longbow_tq4_codes_per_vector` | Number of codes per vector for TurboQuant4 (4-bit) |
| `longbow_tq4_decode_duration_seconds` | Time spent decoding vectors with TurboQuant4 (4-bit) |
| `longbow_tq4_encode_duration_seconds` | Time spent encoding vectors with TurboQuant4 (4-bit) |
| `longbow_tq8_codes_per_vector` | Number of codes per vector for TurboQuant8 (8-bit) |
| `longbow_tq8_decode_duration_seconds` | Time spent decoding vectors with TurboQuant8 (8-bit) |
| `longbow_tq8_encode_duration_seconds` | Time spent encoding vectors with TurboQuant8 (8-bit) |
| `longbow_trace_buffer_utilization` | Current trace buffer utilization (0-1) |
| `longbow_trace_context_propagation_total` | Total number of trace context propagations |
| `longbow_trace_duration_seconds` | Duration of trace spans |
| `longbow_trace_errors_total` | Total number of trace span errors |
| `longbow_trace_exports_total` | Total number of trace exports to backends |
| `longbow_trace_sampling_rate` | Current trace sampling rate (0-1) |
| `longbow_trace_spans_created_total` | Total number of trace spans created |
| `longbow_trace_spans_total` | Total number of trace spans created |
| `longbow_turboquant_encoding_latency_seconds` | Latency of server-side TurboQuant encoding operations |
| `longbow_turboquant_encoding_total` | Total number of TurboQuant encoding operations |
| `longbow_turboquant_search_latency_seconds` | Latency of TurboQuant-accelerated search operations |
| `longbow_turboquant_search_total` | Total number of searches performed using TurboQuant acceleration |
| `longbow_turboquant_storage_bytes_total` | Total storage bytes used by TurboQuant-encoded vectors (vs float32 baseline) |
| `longbow_validation_failures_total` | Total number of validation failures |
| `longbow_vector_access_bytes_allocated_total` | Total bytes allocated for vector copies |
| `longbow_vector_access_copy_total` | Total number of vector accesses requiring copy |
| `longbow_vector_access_zerocopy_total` | Total number of zero-copy vector accesses |
| `longbow_vector_cast_f16_to_f32_total` | Total number of vector casts from Float16 to Float32 |
| `longbow_vector_cast_f32_to_f16_total` | Total number of vector casts from Float32 to Float16 |
| `longbow_vector_clock_conflicts_total` | Total number of vector clock conflicts detected |
| `longbow_vector_clock_merges_total` | Total number of vector clock merges |
| `longbow_vector_copy_total` | Total number of vector copies (indicates zero-copy violations) |
| `longbow_vector_index_size` | Current number of vectors in the index |
| `longbow_vector_pool_hits_total` | Total number of vector pool hits (reused vectors) |
| `longbow_vector_pool_misses_total` | Total number of vector pool misses (new allocations) |
| `longbow_vector_pool_puts_total` | Total number of vectors returned to pool |
| `longbow_vector_scratch_pool_misses_total` | Count of scratch buffer pool misses requiring allocation |
| `longbow_vector_search_action_duration_seconds` | Duration of vector search actions |
| `longbow_vector_search_action_errors_total` | Total number of vector search action errors |
| `longbow_vector_search_action_total` | Total number of vector search actions executed |
| `longbow_vector_search_ef_search` | efSearch value used in vector search queries |
| `longbow_vector_search_gpu_latency_seconds` | Latency of GPU vector search operations |
| `longbow_vector_search_gpu_operations_total` | Total number of GPU vector search operations |
| `longbow_vector_search_latency_seconds` | Latency of vector search operations |
| `longbow_vector_search_parse_fallback_total` | Total number of vector search parse fallbacks |
| `longbow_vector_sentinel_hit_total` | Total number of times a sentinel vector was used due to missing data |
| `longbow_vq_reconstruction_error` | Current average reconstruction error (MSE) of PQ/OPQ |
| `longbow_vq_training_duration_seconds` | Time spent training PQ/OPQ codebooks |
| `longbow_wal_adaptive_interval_ms` | Current adaptive WAL flush interval in milliseconds |
| `longbow_wal_batch_size` | Number of entries flushed per WAL batch |
| `longbow_wal_buffer_pool_operations_total` | Total number of WAL buffer pool operations |
| `longbow_wal_bytes_written_total` | Total bytes written to the Write-Ahead Log |
| `longbow_wal_flush_errors_total` | Total number of WAL flush failures |
| `longbow_wal_fsync_duration_seconds` | Time taken for WAL fsync operations |
| `longbow_wal_lock_wait_duration_seconds` | Time spent waiting for WAL locks |
| `longbow_wal_pending_entries` | Current number of pending WAL entries (backpressure indicator) |
| `longbow_wal_queue_depth` | Current number of batches waiting in the WAL persistence queue |
| `longbow_wal_queue_latency_seconds` | Time spent in the persistence queue before processing |
| `longbow_wal_replay_duration_seconds` | Time taken to replay the Write-Ahead Log |
| `longbow_wal_ring_buffer_drains_total` | Total number of ring buffer drain operations |
| `longbow_wal_ring_buffer_full_total` | Total number of times ring buffer was full (backpressure) |
| `longbow_wal_ring_buffer_pushes_total` | Total number of successful ring buffer push operations |
| `longbow_wal_ring_buffer_utilization` | Current utilization of WAL ring buffer (0-1) |
| `longbow_wal_uring_cq_depth` | Current depth of the io_uring completion queue |
| `longbow_wal_uring_sq_depth` | Current depth of the io_uring submission queue |
| `longbow_wal_uring_submit_latency_seconds` | Latency of io_uring Enter/Submit calls |
| `longbow_wal_write_duration_seconds` | Duration of WAL writes |
| `longbow_wal_write_errors_total` | Total number of WAL write errors |
| `longbow_wal_write_rate_per_second` | Current WAL write rate per second |
| `longbow_wal_writes_total` | Total number of WAL write operations |
| `longbow_warmup_datasets_completed` | Total number of datasets where warmup is completed |
| `longbow_warmup_datasets_total` | Total number of datasets to warmup |
| `longbow_warmup_progress_percent` | Current warmup progress percentage (0-100) |
| `longbow_wasm_inference_duration_seconds` | Duration of WASM-based model inferences |
| `longbow_wasm_inference_total` | Total number of WASM-based model inferences |
| `longbow_work_queue_backlog` | Current number of items in work queue |
| `longbow_work_queue_overflows_total` | Total number of work queue overflow rejections |
| `longbow_zero_alloc_vector_search_parse_total` | Total number of zero-alloc vector search parses |
