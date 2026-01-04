# Longbow Helm Chart Documentation

The Longbow Helm chart deploys the Longbow distributed vector store on a Kubernetes cluster. It supports flexible configuration for services, ingress, gRPC tuning, and persistence.

## Installation

Add the repository (if applicable) or install from local source:

```bash
helm install my-longbow ./helm/longbow
```

## Configuration

### Services

Longbow exposes two main services and a metrics endpoint:

- **Data Server**: Handles vector operations (`DoSearch`, `DoPut`, `DoGet`, `DoExchange`).
- **Meta Server**: Handles metadata and coordination (future use).
- **Metrics**: Exposes Prometheus metrics (port 9090 default).

You can configure the Data and Meta services independently. This allows you to expose the Data Server via a LoadBalancer while keeping the Meta Server internal, for example.

```yaml
service:
  data:
    enabled: true
    type: LoadBalancer
    port: 3000
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-type: nlb
  meta:
    enabled: true
    type: ClusterIP
    port: 3001
```

The metrics endpoint is exposed on the pod but does not have a dedicated Service by default. You can use standard Prometheus service discovery annotations on the Pod (enabled by default).

### Ingress

Ingress can also be configured separately for Data and Meta services. This is useful if you want to route traffic via different hostnames or paths.

```yaml
ingress:
  data:
    enabled: true
    hosts:
      - host: longbow-data.example.com
        paths:
          - path: /
            pathType: Prefix
  meta:
    enabled: false
```

### gRPC Tuning

You can fine-tune gRPC performance settings for the internal Arrow Flight servers. This is critical for handling large datasets and high-throughput vector search.

```yaml
grpc:
  maxRecvMsgSize: 134217728      # 128MB
  maxSendMsgSize: 134217728      # 128MB
  initialWindowSize: 4194304     # 4MB
  initialConnWindowSize: 4194304 # 4MB
  maxConcurrentStreams: 500
  keepAliveTime: "2h"
```

### Hybrid Search

Enable hybrid search to combine vector similarity with keyword matching.

```yaml
hybrid:
  enabled: true
  textColumns: "text" # CSV list of columns to index
  alpha: 0.5          # 0.0=sparse only, 1.0=dense only
```

### Persistence

Longbow supports optional persistence using a Write-Ahead Log (WAL) and Snapshots.

```yaml
persistence:
  wal:
    enabled: true
    size: 10Gi
    storageClass: gp3
  snapshots:
    enabled: true
    size: 20Gi
    storageClass: gp3
```

### S3 Snapshots

For cloud-native persistence, you can offload snapshots to S3-compatible storage.

```yaml
s3:
  enabled: true
  bucket: "my-longbow-snapshots"
  region: "us-west-2"
  existingSecret: "my-aws-creds"
```

### Compaction

Configure background compaction to merge small record batches and reclaim space.

```yaml
compaction:
  enabled: true
  interval: "30s"
  targetBatchSize: 10000
  minBatches: 10
```

### Sharding

Longbow supports auto-sharding and consistent hashing ring sharding for horizontal scalability.

```yaml
sharding:
  auto:
    enabled: true
    threshold: 10000  # Number of vectors to trigger migration to sharded index
  ring:
    enabled: true     # Use consistent hashing ring (vnodes) for uniform distribution
```
