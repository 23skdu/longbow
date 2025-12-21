# Longbow Helm Chart

A Helm chart for deploying Longbow, high-performance vector database on Kubernetes.

## Configuration

The following table lists the configurable parameters of the Longbow chart and their default values.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Image repository | `ghcr.io/23skdu/longbow` |
| `image.tag` | Image tag | `0.0.8` |
| `gossip.enabled` | Enable SWIM gossip protocol | `false` |
| `gossip.port` | Gossip port | `7946` |
| `storage.asyncFsync` | Enable async WAL fsync | `true` |
| `service.data.port` | gRPC Data port | `3000` |
| `service.meta.port` | gRPC Meta port | `3001` |
