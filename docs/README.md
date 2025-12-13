# Longbow Documentation

Longbow is a high-performance, in-memory vector store implementing the
Apache Arrow Flight protocol.

## Architecture

```mermaid
graph TD
 Client[Client Application] -->|Arrow Flight (gRPC/HTTP2)| LB[Longbow Server]
 LB -->|Store| Mem[In-Memory Vector Store]
 LB -->|Metrics| Prom[Prometheus]
 LB -->|Persistence| PVC[Persistent Volume]

 subgraph Kubernetes
 LB
 Mem
 PVC
 end
```

## Key Features

* **Apache Arrow Flight Protocol**: Zero-copy data transfer.
* **In-Memory Storage**: Fast read/write operations.
* **Prometheus Metrics**: Built-in observability.
* **Helm Deployment**: Easy installation on Kubernetes.
* **Persistence**: Optional persistent storage for data durability.
* **Security**: Configurable security contexts for Pods and Containers.

## Navigation

* [Components](components.md)
* [Usage Guide](usage.md)
* [Arrow Protocol Spec](arrow-protocol.md)

## Observability & Metrics

Longbow exposes Prometheus metrics on port `9090` at `/metrics`.

### Custom Metrics

* `longbow_flight_operations_total`: Total number of Flight operations (DoGet,
DoPut, etc.)
* `longbow_flight_duration_seconds`: Histogram of operation latencies
* `longbow_flight_bytes_processed_total`: Total bytes processed in operations

### Scrape Configuration

The Helm chart includes annotations for Prometheus scraping:

```yaml
podAnnotations:
 prometheus.io/scrape: "true"
 prometheus.io/port: "9090"
 prometheus.io/path: "/metrics"
```

## Status Badges

![CI](https://github.com/23skdu/longbow/actions/workflows/ci.yml/badge.svg)
![Helm Validation](https://github.com/23skdu/longbow/actions/workflows/helm-validation.yml/badge.svg)
![Markdown Lint](https://github.com/23skdu/longbow/actions/workflows/markdown-lint.yml/badge.svg)
![Release](https://github.com/23skdu/longbow/actions/workflows/release.yml/badge.svg)
