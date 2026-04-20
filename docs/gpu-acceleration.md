# Hardware Acceleration & GPU Guide

Longbow is optimized for heterogeneous hardware, providing native support for NVIDIA GPUs (CUDA), Apple Silicon (Metal), and high-performance networking (RDMA/RoCEv2) for zero-copy data movement.

---

## 1. GPU Acceleration (CUDA & Metal)

Longbow supports GPU-accelerated vector search and ML inference on both NVIDIA and Apple Silicon platforms.

### Platform Support Matrix

| Platform | Library | Acceleration Tech | Support Status |
| :--- | :--- | :--- | :--- |
| **Linux (NVIDIA)** | CUDA 11.8+ | Tensor Cores, FAISS | ✅ Production |
| **macOS (Apple)** | Metal | Unified Memory, vDSP | ✅ Production |

### Hybrid CPU/GPU Search
Longbow uses a hybrid approach for optimal performance:
1.  **Selection**: GPU performs coarse candidate generation (brute-force or IVF).
2.  **Refinement**: CPU HNSW graph filters tombstones and refines to top-k results.
3.  **Fallback**: If GPU resources are exhausted or unavailable, the system seamlessly falls back to CPU-only search.

---

## 2. ONNX Runtime & Inference

Longbow includes a high-performance ONNX runtime for re-ranking and embedding generation.

### Backends
- **Metal Backend**: Optimized for Apple Silicon M1-M4. Uses native Metal Shaders for cross-encoder inference.
- **CUDA Backend**: Optimized for NVIDIA RTX/A-series. Leverages the CUDA Execution Provider (EP).
- **CPU Backend**: Scalable fallback using parallel thread pools.

### Configuration
Set the execution provider via `LONGBOW_ONNX_EP`:
- `Metal`: Force Apple Silicon GPU.
- `CUDA`: Force NVIDIA GPU.
- `CPU`: Standard multi-threaded execution.

---

## 3. Zero-Copy Networking: RDMA over RoCEv2

For large-scale ingestion and distributed search, Longbow implements **RDMA (Remote Direct Memory Access)** over RoCEv2.

### The Zero-Copy Pipeline
RDMA allows clients to push Apache Arrow batches directly into GPU VRAM or NUMA-aligned CPU memory, bypassing the kernel network stack and host CPU.
1.  **Handshake**: Negotiation over TCP/gRPC.
2.  **Registration**: Server registers a Memory Region (MR) and provides a Remote Key (`RKey`).
3.  **Direct Transfer**: Client uses `ibv_post_send` (RDMA Write) to push data directly into pre-allocated VRAM.
4.  **Completion**: Server receives a notification and immediately triggers indexing/inference.

### Prerequisites & Setup
- **Hardware**: Mellanox (NVIDIA) ConnectX-4+ NICs.
- **Drivers**: CUDA with GPUDirect RDMA or GDRCopy enabled.
- **Enabling**: Set `LONGBOW_RDMA_ENABLED=true` and `LONGBOW_RDMA_INTERFACE` (e.g., `eth0`).

---

## 4. Performance Metrics

Monitor hardware acceleration via the following Prometheus metrics:
- `longbow_gpu_memory_bytes`: GPU VRAM utilization.
- `longbow_onnx_inference_duration_seconds`: Inference latency by backend.
- `longbow_rdma_bytes_received_total`: Data volume moved via zero-copy RDMA.
