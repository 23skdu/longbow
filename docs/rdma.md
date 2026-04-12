# RDMA over RoCEv2 Architecture in Longbow

Longbow implements high-performance, zero-copy data transfer using **RDMA (Remote Direct Memory Access)** over **RoCEv2 (RDMA over Converged Ethernet v2)**. This allows clients and peers to push large Apache Arrow batches directly into GPU memory (VRAM) or NUMA-aligned CPU memory without involving the host CPU for data copying.

## 1. Core Architecture

The RDMA implementation bypasses the kernel network stack, dramatically reducing latency and CPU utilization during massive ingestion or re-sharding operations.

### Zero-Copy Pipeline
1.  **Handshake**: Client and server perform a standard gRPC handshake over TCP (Port 3000) to negotiate RDMA capabilities.
2.  **Memory Registration**: Server registers a GPU Memory Region (MR) and provides a Remote Key (`RKey`) to the client.
3.  **Direct Transfer**: Client uses `ibv_post_send` (RDMA Write) to push Arrow RecordBatches directly into the pre-allocated VRAM buffers.
4.  **Completion**: Server receives a completion notification (CQE) and immediately triggers the HNSW indexing kernels.

## 2. Prerequisites

*   **Hardware**: Mellanox (NVIDIA) ConnectX-4 or newer NICs.
*   **Operating System**: Linux with `ibverbs` support.
*   **Drivers**: NVIDIA CUDA with GDRCopy or Peer-to-Peer (P2P) DMA enabled.

## 3. Configuration

RDMA is controlled via environment variables and the `longbow.yaml` configuration file.

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_RDMA_ENABLED` | Enables RDMA transport | `false` |
| `LONGBOW_RDMA_INTERFACE` | Network interface for RoCEv2 | `eth0` |
| `LONGBOW_RDMA_PORT` | Port for RDMA handshake | `3002` |
| `LONGBOW_RDMA_GPU_DIRECT` | Enables NVIDIA GPUDirect RDMA | `true` |

## 4. Performance Metrics

Longbow exposes the following Prometheus metrics for RDMA monitoring:

*   `longbow_rdma_bytes_received_total`: Total bytes received via zero-copy RDMA.
*   `longbow_rdma_errors_total`: Count of RDMA transfer or registration failures.
*   `longbow_rdma_active_peers`: Number of currently connected RDMA peers.

## 5. Security Considerations

RDMA bypasses standard kernel firewall rules (iptables). It is mandatory to:
*   Run RDMA-enabled clusters within a **private, isolated management network**.
*   Enable **RoCEv2 encryption** if supported by the NIC hardware.
*   Use Longbow's built-in **MTLS** for the control-plane handshake.
