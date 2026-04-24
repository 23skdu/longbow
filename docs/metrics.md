# Longbow Metrics Reference

Complete reference for all Prometheus metrics exported by Longbow 0.1.9.

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

- **longbow_turboquant_search_total**: (Counter) Number of searches using TurboQuant acceleration. Labels: `dataset`, `bit_width` (4 or 2).
- **longbow_turboquant_search_latency_seconds**: (Histogram) Latency of TQ-accelerated searches. Labels: `dataset`, `bit_width`.
- **longbow_turboquant_encoding_total**: (Counter) Vectors encoded into TQ format. Labels: `dataset`, `direction` (client_provided or server_encoded).
- **longbow_turboquant_storage_bytes_total**: (Gauge) Memory used by TQ vectors (demonstrating compression gains). Labels: `dataset`.
- **longbow_simd_static_dispatch_type**: (Gauge) Currently active SIMD kernel type (0=Generic, 1=NEON, 2=AVX2, 3=AVX-512).
- **longbow_hnsw_simd_dispatch_latency_seconds**: (Histogram) Time taken for dynamic kernel selection per query. Labels: `type`.

---

## 4. Hardware & GPU
Metrics for CUDA and Metal acceleration.

- **longbow_onnx_inference_duration_seconds**: (Histogram) Duration of ML model execution. Labels: `backend` (onnx, metal, wazero).
- **longbow_onnx_metal_memory_used_bytes**: (Gauge) VRAM utilization on Apple Silicon.
- **longbow_gpu_memory_bytes**: (Gauge) VRAM utilization on NVIDIA/CUDA systems.

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

---

## 7. Resource Management
- **longbow_arena_memory_bytes**: (Gauge) Memory allocated in custom slab arenas. Labels: `size`.
- **longbow_gc_pause_duration_seconds**: (Histogram) Latency of Go garbage collection cycles.
- **longbow_gctuner_heap_target_bytes**: (Gauge) The dynamic heap target set by the GCTuner.
