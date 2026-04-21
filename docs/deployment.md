# Deployment & Operations Guide

Longbow is designed for cloud-native, distributed deployment using Kubernetes (Helm), providing high availability through Gossip-based mesh networking and horizontal scaling via Consistent Hashing.

---

## 1. Kubernetes Deployment (Helm)

The recommended way to deploy Longbow is using the official Helm chart.

### Quick Start

```bash
helm install my-release ./helm/longbow
```

### Docker & Multi-Platform Support

Official images are available on GitHub Container Registry (`ghcr.io/23skdu/longbow`):

- **Apple Silicon (`arm64`)**: `latest-arm64-metal` - Optimized for Metal GPU and Mach CPU clusters.
- **NVIDIA GPU (`amd64`)**: `latest-amd64-nvidia` - Includes custom CUDA 12.6 kernels and zero-copy tensor bridge.
- **General CPU (`amd64`)**: `latest-amd64-cpu` - Broadwell-level AVX2 optimizations with `io_uring` support.

### Performance Tuning

- **IO Performance**: Enable `LONGBOW_STORAGE_USE_IOURING=true` on Linux kernels 5.6+ to double WAL ingestion throughput.
- **Memory Limits**: Set `GOMEMLIMIT` to 90% of the container limit. Longbow's **GCTuner** will automatically balance GC frequency to prevent OOMs.

---

## 2. Distributed Architecture

Longbow uses a "Dynamo-style" architecture to scale horizontally across multiple nodes.

### Consistent Hashing Ring

- **Vnodes**: Each physical node is represented by 20 virtual nodes to ensure uniform data distribution.
- **Routing**: Keys are hashed (SHA-256) and assigned clockwise to the nearest vnode.
- **Partition Proxy**: Every node acts as a proxy, transparently forwarding requests to the authoritative owner using a gRPC interceptor.

### Mesh Networking (Gossip)

Longbow uses the **SWIM** (Scalable Weakly-consistent Infection-style Process Group Membership) protocol for decentralized coordination.

- **Failure Detection**: Periodic probing (Direct + Indirect Pings) to detect node health.
- **Discovery**: Supports Kubernetes API (pod labels), DNS (SRV records), or Static Seeds.
- **Convergence**: Changes in mesh state automatically drive the Consistent Hashing Ring rebalancing.

---

## 3. Multitenancy: Namespaces

Isolate datasets and workloads using the **Namespaces** API via `DoAction`.

```json
{"type": "CreateNamespace", "body": {"name": "tenant-a"}}
```

- **Isolation**: Each namespace manages its own collection of datasets and indexing workers.
- **Lifecycle**: Bulk deletion of an entire tenant's data via `DeleteNamespace`.

---

## 4. Traffic Control: Rate Limiting

Protects the server from being overwhelmed by spikes in request volume.
- **Algorithm**: Token Bucket (supports Unary and Streaming gRPC).

- **Configuration**:
  - `LONGBOW_RATE_LIMIT_RPS`: Requests per second.
  - `LONGBOW_RATE_LIMIT_BURST`: Maximum burst window.

- **Feedback**: Throttled requests return `ResourceExhausted` (gRPC status 8).

---

## 5. Observability & Monitoring

Longbow exposes internal state via a Prometheus-compatible metrics endpoint (default port 9090).

| Component | Metric Namespace | Key Metrics |
| :--- | :--- | :--- |
| **Gossip** | `longbow_gossip_` | `active_members`, `state_changes` |
| **Search** | `longbow_search_` | `latency_seconds`, `ops_total` |
| **Rate Limit**| `longbow_rate_limit_` | `requests_total{status="throttled"}` |
| **Resources** | `process_`, `go_` | CPU, Memory, GC cycles |
