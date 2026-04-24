# Hardware Acceleration (GPU & TPU)

Longbow is optimized for heterogeneous hardware, providing native support for NVIDIA GPUs (CUDA), Apple Silicon (Metal), and Google TPUs (Ironwood) for high-performance vector search and ML inference.

---

## 1. Acceleration Platform Matrix

| Platform | Backend | Hardware | Memory Model | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Linux (NVIDIA)** | **CUDA** | RTX/A-series/H-series | Dedicated VRAM | ✅ Production |
| **macOS (Apple)** | **Metal** | M1/M2/M3/M4 | Unified Memory | ✅ Production |
| **Google Cloud** | **TPU** | v7x (Ironwood) | HBM + VMEM | ✅ Beta |
| **Cross-Platform** | **SIMD** | AVX-512/AVX2/NEON | System RAM | ✅ Production |

---

## 2. NVIDIA CUDA Acceleration

Optimized for data center workloads on Linux.

- **Kernels**: Custom distance (L2, Cosine, IP) and HNSW traversal kernels written in CUDA C++.
- **Memory**: Leverages high-bandwidth VRAM with zero-copy tensor bridges for Arrow Flight integration.
- **Tuning**:
  - `LONGBOW_GPU_ENABLED=true`
  - `LONGBOW_GPU_MEMORY_LIMIT`: Configurable VRAM pool size.

---

## 3. Apple Metal Acceleration

Optimized for local development and edge inference on Apple Silicon.

- **Unified Memory**: Metal leverages the M-series unified memory architecture, eliminating expensive CPU-to-GPU copies.
- **MPS Integration**: Uses Metal Performance Shaders for highly optimized vector math.
- **Automatic Detection**: Longbow automatically detects and utilizes the Metal backend on macOS (ARM64) when built with the `gpu` tag.

---

## 4. Google TPU (Ironwood) Support

Experimental support for Google's latest TPU v7x architecture.

- **HBM Support**: Utilizes the 192GB of High Bandwidth Memory for massive vector indices.
- **VMEM Scratchpad**: Uses 16MB of ultra-fast SRAM (VMEM) for hot-path distance calculations.
- **Scalability**: Designed for petabyte-scale vector search in GCP environments.

---

## 5. Hybrid Search Strategy

Longbow uses a multi-stage search strategy for optimal resource utilization:
1.  **Candidate Generation**: Coarse-grained filtering on GPU/TPU for massive speedups.
2.  **Refinement**: CPU-based HNSW traversal for final precision and tombstone filtering.
3.  **Fallback**: Seamlessly falls back to CPU-only SIMD kernels if acceleration hardware is unavailable or under heavy contention.

---

## 6. Zero-Copy Networking (RDMA)

For distributed search across accelerated nodes, Longbow implements **RDMA over RoCEv2**. This allows pushing Arrow batches directly from a client into a remote node's GPU VRAM or TPU HBM, bypassing the OS network stack.

---

## 7. Metrics & Monitoring

Monitor acceleration performance via Prometheus (Port 9090):
- `longbow_gpu_memory_bytes`: VRAM/HBM utilization.
- `longbow_onnx_inference_duration_seconds`: Latency per backend.
- `longbow_simd_static_dispatch_type`: Active CPU kernel type.
