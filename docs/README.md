# Longbow Documentation

Longbow is a high-performance, in-memory vector store implementing the Apache Arrow Flight protocol.

## Architecture

```mermaid
graph TD
    Client[Client Application] -->|Arrow Flight (gRPC/HTTP2)| LB[Longbow Server]
    LB -->|Store| Mem[In-Memory Vector Store]
    LB -->|Metrics| Prom[Prometheus]
    
    subgraph Kubernetes
        LB
        Mem
    end
```

## Key Features

* **Apache Arrow Flight Protocol**: Zero-copy data transfer.
* **In-Memory Storage**: Fast read/write operations.
* **Prometheus Metrics**: Built-in observability.
* **Helm Deployment**: Easy installation on Kubernetes.

## Navigation

* [Components](components.md)
* [Usage Guide](usage.md)
* [Arrow Protocol Spec](arrow-protocol.md)
