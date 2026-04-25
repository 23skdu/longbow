import json

missing = [
 "longbow_adaptive_gc_adjustments_total",
 "longbow_adaptive_gc_allocation_rate_bytes_per_sec",
 "longbow_adaptive_gc_current_gogc",
 "longbow_adaptive_gc_memory_pressure_ratio",
 "longbow_batch_distance_compute_duration_seconds",
 "longbow_batch_distance_compute_fallback_total",
 "longbow_bloom_filter_selectivity",
 "longbow_compaction_batches_merged_total",
 "longbow_compaction_fragmented_batches_total",
 "longbow_compaction_rate_limit_wait_seconds",
 "longbow_compaction_runs_total",
 "longbow_compaction_tombstone_density_ratio",
 "longbow_compaction_triggers_total",
 "longbow_compressed_vectors_sent_total",
 "longbow_connection_pool_close_total",
 "longbow_connection_pool_create_total",
 "longbow_connection_pool_get_duration_seconds",
 "longbow_connection_pool_health_check_total",
 "longbow_connection_pool_refresh_total",
 "longbow_disk_store_read_bytes_total",
 "longbow_disk_store_write_bytes_total",
 "longbow_do_get_time_to_first_chunk_seconds",
 "longbow_doput_batch_size_bytes",
 "longbow_embedding_generation_duration_seconds",
 "longbow_embedding_normalization_duration_seconds",
 "longbow_embedding_pooling_duration_seconds",
 "longbow_filter_evaluator_allocations_total",
 "longbow_filter_evaluator_duration_seconds",
 "longbow_gc_tuner_gpu_utilization",
 "longbow_hnsw_branch_prediction_likely_total",
 "longbow_hnsw_branch_prediction_unlikely_total",
 "longbow_hnsw_context_check_total",
 "longbow_hnsw_repair_failure_total",
 "longbow_hnsw_repair_nodes_visited_total",
 "longbow_hnsw_repair_success_total",
 "longbow_hnsw_search_early_exits_total",
 "longbow_ivf_cluster_search_total",
 "longbow_ivf_load_balance_ratio",
 "longbow_jit_compilation_duration_seconds",
 "longbow_jit_kernel_calls_total",
 "longbow_jit_kernel_errors_total",
 "longbow_namespace_creation_total",
 "longbow_panic_total",
 "longbow_pipeline_duration_seconds",
 "longbow_query_cache_evictions_total",
 "longbow_query_cache_hits_total",
 "longbow_query_cache_misses_total",
 "longbow_query_cache_ops_total",
 "longbow_query_cache_size",
 "longbow_rate_limit_requests_total",
 "longbow_raw_vectors_sent_total",
 "longbow_remote_storage_download_bytes_total",
 "longbow_remote_storage_duration_seconds",
 "longbow_remote_storage_ops_total",
 "longbow_remote_storage_upload_bytes_total",
 "longbow_s3_request_duration_seconds",
 "longbow_s3_retries_total",
 "longbow_schema_evolution_total",
 "longbow_simd_tiled_distance_batch_total",
 "longbow_snapshot_rate_limit_wait_seconds",
 "longbow_string_filter_duration_seconds",
 "longbow_string_filter_equal_length_total",
 "longbow_validation_failures_total",
 "longbow_vector_access_bytes_allocated_total",
 "longbow_vector_access_copy_total",
 "longbow_vector_sentinel_hit_total",
 "longbow_vq_reconstruction_error",
 "longbow_vq_training_duration_seconds",
 "longbow_wal_flush_errors_total",
 "longbow_wal_queue_latency_seconds",
 "longbow_wal_write_duration_seconds",
 "longbow_warmup_datasets_total",
 "longbow_wasm_inference_duration_seconds",
 "longbow_wasm_inference_total"
]

dashboard = {
  "annotations": {
    "list": []
  },
  "editable": True,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 0,
  "id": None,
  "links": [],
  "liveNow": False,
  "panels": [],
  "refresh": "",
  "schemaVersion": 38,
  "style": "dark",
  "tags": ["longbow", "internals"],
  "templating": {
    "list": []
  },
  "time": {
    "from": "now-6h",
    "to": "now"
  },
  "timepicker": {},
  "timezone": "",
  "title": "Longbow - Advanced Internals",
  "uid": "longbow_adv_int",
  "version": 1
}

x = 0
y = 0

for idx, metric in enumerate(missing):
    is_counter = metric.endswith("_total")
    is_duration = "duration" in metric or "latency" in metric or "seconds" in metric
    is_bytes = "bytes" in metric
    
    # generate a simple timeseries panel
    panel = {
      "datasource": {
        "type": "prometheus",
        "uid": "${DS_PROMETHEUS}"
      },
      "gridPos": {
        "h": 8,
        "w": 8,
        "x": x,
        "y": y
      },
      "id": idx + 1,
      "title": metric,
      "type": "timeseries",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "${DS_PROMETHEUS}"
          },
          "editorMode": "code",
          "expr": f"rate({metric}[5m])" if is_counter else metric,
          "legendFormat": "__auto",
          "range": True,
          "refId": "A"
        }
      ]
    }
    
    x += 8
    if x >= 24:
        x = 0
        y += 8
        
    dashboard["panels"].append(panel)

with open('grafana/dashboards/advanced-internals.json', 'w') as f:
    json.dump(dashboard, f, indent=2)

print("Dashboard created successfully.")
