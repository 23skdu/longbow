# Longbow Metrics Reference

Complete reference for all Prometheus metrics exported by Longbow.

**Metrics Endpoint**: `http://localhost:9090/metrics` (configurable via `LONGBOW_METRICS_ADDR`)

---

## 1. Flight & RPC

Performance and throughput of the Arrow Flight interface.

- **longbow_flight_ops_total**: (Counter) Total processed Arrow Flight operations. Labels: `action`, `status`.
- **longbow_flight_duration_seconds**: (Histogram) Latency of Flight operations. Labels: `action`.
- **longbow_flight_bytes_read_total**: (Counter) Bytes read from input streams.
- **longbow_flight_bytes_written_total**: (Counter) Bytes written to output streams.

---

## 2. Vector Search & HNSW

Metrics for the core vector indexing and search engine.

- **longbow_vector_search_latency_seconds**: (Histogram) Latency of search operations. Labels: `dataset`.
- **longbow_hnsw_node_count**: (Gauge) Total number of nodes in the HNSW graph. Labels: `dataset`.
- **longbow_hnsw_search_queries_total**: (Counter) Total search queries executed. Labels: `dims`.
- **longbow_hnsw_polymorphic_latency_seconds**: (Histogram) Search latency broken down by vector type (Float32, FP16, etc.). Labels: `type`.
- **longbow_hnsw_arrow_extraction_errors_total**: (Counter) Errors during zero-copy extraction from Arrow buffers. Labels: `type`, `error`.

---

## 3. TurboQuant & Acceleration (New in 0.1.9)

Monitoring the SIMD-accelerated quantization and bit-packing features.

- **longbow_turboquant_encoding_total**: (Counter) Total number of TurboQuant encoding operations. Labels: `dataset`, `direction` (client_provided | server_encoded).
- **longbow_turboquant_encoding_latency_seconds**: (Histogram) Server-side latency for TurboQuant encoding operations. Labels: `dataset`. Buckets: 0.1ms, 0.5ms, 1ms, 5ms, 10ms, 50ms.
- **longbow_turboquant_storage_bytes_total**: (Gauge) Total storage bytes used by TurboQuant-encoded vectors. Labels: `dataset`.
- **longbow_turboquant_search_total**: (Counter) Total searches performed using TurboQuant acceleration. Labels: `dataset`, `bit_width` (2 | 4 | 8).
- **longbow_turboquant_search_latency_seconds**: (Histogram) Latency of TurboQuant-accelerated search operations. Labels: `dataset`, `bit_width`. Buckets: 0.1ms, 0.5ms, 1ms, 5ms, 10ms, 50ms, 100ms.
- **longbow_simd_static_dispatch_type**: (Gauge) Currently active SIMD kernel type. Value: 0=Generic, 1=NEON, 2=AVX2, 3=AVX-512.
- **longbow_hnsw_simd_dispatch_latency_seconds**: (Histogram) Time taken for dynamic SIMD kernel selection per query. Labels: `type`.

---

## 4. Hardware & GPU

Metrics for CUDA and Metal acceleration.

- **longbow_onnx_inference_duration_seconds**: (Histogram) Duration of ML model execution. Labels: `backend` (onnx, metal, wazero).
- **longbow_onnx_metal_memory_used_bytes**: (Gauge) VRAM utilization on Apple Silicon.
- **longbow_gpu_memory_bytes**: (Gauge) VRAM utilization on NVIDIA/CUDA systems.
- **longbow_stub_model_usage_total**: (Counter) **New in 0.1.9**: Count of times a stub embedding model was used due to missing configuration. Labels: `model_path`.

---

## 5. Persistence & IO

Metrics for the WAL and snapshotting system.

- **longbow_snapshot_write_duration_seconds**: (Histogram) Latency of reflection-free Parquet snapshotting.
- **longbow_snapshot_size_bytes**: (Histogram) Distribution of snapshot file sizes.
- **longbow_iouring_ops_submitted_total**: (Counter) Total IO operations submitted via `io_uring`. Labels: `operation`.

---

## 6. Distributed & Mesh

Metrics for cluster membership and sharding.

- **longbow_gossip_active_members**: (Gauge) Number of healthy nodes in the gossip mesh.
- **longbow_gossip_state_changes_total**: (Counter) Number of membership transitions (join/leave/fail).
- **longbow_ring_vnode_distribution**: (Gauge) Number of virtual nodes assigned per physical node.
- **longbow_index_sync_delta_total**: (Counter) **New in 0.1.9**: Number of vectors synchronized via delta-sync anti-entropy. Labels: `index_type`, `dataset`.

---

## 7. Resource Management

- **longbow_arena_memory_bytes**: (Gauge) Memory allocated in custom slab arenas. Labels: `size`.
- **longbow_gc_pause_duration_seconds**: (Histogram) Latency of Go garbage collection cycles.
- **longbow_gctuner_heap_target_bytes**: (Gauge) The dynamic heap target set by the GCTuner.

---

## 8. SIMD Activation Kernels & Adaptive Dispatch

Metrics added during the RCU + SIMD optimization pass. These cover the new
AVX-512 `exp`/`softmax` kernels and the adaptive GPU dispatch threshold.

- **longbow_simd_activation_kernel_duration_seconds**: (Histogram) Execution
  time of SIMD activation kernels (`exp`, `softmax`, `sigmoid`). Labels:
  `kernel` (`exp` | `softmax` | `sigmoid`), `arch` (`avx512` | `avx2` |
  `neon`). Buckets: 1µs, 5µs, 10µs, 50µs, 100µs, 500µs, 1ms.

- **longbow_simd_activation_kernel_calls_total**: (Counter) Total invocations
  of SIMD activation kernels. Labels: `kernel`, `arch`. Use this to track
  whether the AVX-512 path is being exercised on production hardware.

> **Grafana**: Both metrics are wired into the **Advanced Internals** dashboard
> under the *SIMD Activation Kernels* row. Useful alert: fire when p99
> `longbow_simd_activation_kernel_duration_seconds{kernel="softmax"}` exceeds
> 500µs — this indicates the kernel is being called on unexpectedly large
> tensors or is falling back to the generic path.

---

## 9. Graph GPU Adaptive Dispatch

The adaptive dispatch threshold (`GPUWorkloadThreshold = 128 nodes`) can be
monitored using the following signals:

- **longbow_graph_gpu_dispatch_total**: (Counter, *planned*) Total
  `RankWithGraphGPU` calls. Labels: `dataset`.
- **longbow_graph_gpu_dispatch_fallback_total**: (Counter, *planned*) Calls
  that fell back to CPU due to the workload being below the threshold. Labels:
  `dataset`.

> Until these counters are wired, the adaptive-dispatch behavior can be
> inferred from a reduction in `longbow_onnx_inference_duration_seconds`
> variance at low-cardinality result sets.
