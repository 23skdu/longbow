# Deployment & Operations Guide

Longbow is designed for cloud-native, distributed deployment using Kubernetes (Helm), providing high availability through Gossip-based mesh networking and horizontal scaling via Consistent Hashing.

---

## 1. Kubernetes Deployment (Helm)

The recommended way to deploy Longbow is using the official Helm chart.

### Quick Start

```bash
helm install my-release ./helm/longbow
```

### Key Configuration (values.yaml)

- **Services**: Toggle `data` (port 3000) and `meta` (port 3001) services.
- **Resources**: Set CPU/Memory limits (ensure memory is ~2.5x the sharding threshold).
- **gRPC Tuning**: Adjust `maxRecvMsgSize` and `maxConcurrentStreams` for high-throughput batching.

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
