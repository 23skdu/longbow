# Longbow Helm Chart

A Helm chart for deploying Longbow, high-performance vector database on Kubernetes.

## Configuration

The following table lists the configurable parameters of the Longbow chart and their default values.

### Global & Image Settings

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Image repository | `ghcr.io/23skdu/longbow` |
| `image.tag` | Image tag | `latest` |
| `image.pullPolicy` | Image pull policy | `Always` |
| `imagePullSecrets` | Secrets to pull image | `[]` |
| `nameOverride` | Override the name of the chart | `""` |
| `fullnameOverride` | Override the full name of the release | `""` |
| `serviceAccount.create` | Create a service account | `true` |
| `serviceAccount.name` | Service account name | `""` |
| `podAnnotations` | Annotations to add to pods | `{}` |
| `podSecurityContext` | Pod security context | `{"fsGroup": 2000}` |
| `securityContext` | Container security context | `{"readOnlyRootFilesystem": true, "runAsNonRoot": true, "runAsUser": 1000}` |
| `resources` | Container resource limits and requests | `{}` |
| `nodeSelector` | Node labels for pod assignment | `{}` |
| `tolerations` | List of node tolerations | `[]` |
| `autoscaling.enabled` | Enable Horizontal Pod Autoscaler | `false` |
| `autoscaling.minReplicas` | Minimum HPA replicas | `1` |
| `autoscaling.maxReplicas` | Maximum HPA replicas | `100` |
| `autoscaling.targetCPUUtilizationPercentage` | HPA target CPU usage | `80` |
| `autoscaling.targetSearchQPS` | Target QPS per pod for scaling | `100` |
| `autoscaling.targetSearchLatencyMs` | Target P99 latency for scaling | `200` |

### gRPC & Networking

| Parameter | Description | Default |
|-----------|-------------|---------|
| `service.data.port` | gRPC Data port | `3000` |
| `service.data.type` | Service type for data | `ClusterIP` |
| `service.meta.port` | gRPC Meta port | `3001` |
| `service.meta.type` | Service type for meta | `ClusterIP` |
| `grpc.maxRecvMsgSize` | Max gRPC receive message size (bytes) | `536870912` |
| `grpc.maxSendMsgSize` | Max gRPC send message size (bytes) | `536870912` |
| `grpc.initialWindowSize` | gRPC initial window size | `1048576` |
| `grpc.maxConcurrentStreams` | Max concurrent gRPC streams | `250` |
| `grpc.keepAliveTime` | gRPC keepalive time | `2h` |
| `grpc.keepAliveTimeout` | gRPC keepalive timeout | `20s` |
| `rateLimit.rps` | Rate limit requests per second (0=disabled) | `0` |
| `rateLimit.burst` | Rate limit burst size | `0` |

### Storage & Persistence

| Parameter | Description | Default |
|-----------|-------------|---------|
| `config.maxMemory` | Max in-memory storage (bytes) | `1073741824` |
| `config.maxWALSize` | Max Write-Ahead Log size (bytes) | `104857600` |
| `config.snapshotInterval` | Interval between snapshots | `1h` |
| `config.ttl` | Default record TTL (0s=disabled) | `0s` |
| `storage.asyncFsync` | Enable asynchronous WAL fsync | `true` |
| `storage.useIOUring` | Use Linux io_uring (requires host support) | `false` |
| `storage.useDirectIO` | Use O_DIRECT for disk I/O | `false` |
| `storage.useParquetV2` | Use Parquet V2 format for storage | `true` |
| `persistence.wal.enabled` | Enable persistent storage for WAL | `true` |
| `persistence.wal.size` | WAL volume size | `5Gi` |
| `persistence.wal.path` | WAL mount path | `/data` |
| `persistence.snapshots.enabled` | Enable persistent storage for snapshots | `false` |
| `persistence.snapshots.size` | Snapshot volume size | `10Gi` |
| `persistence.snapshots.path` | Snapshot mount path | `/snapshots` |
| `compaction.enabled` | Enable dataset compaction | `true` |
| `compaction.interval` | Compaction cycle interval | `30s` |

### HNSW & Indexing Tuning

| Parameter | Description | Default |
|-----------|-------------|---------|
| `hnsw.m` | Number of bi-directional links (M) | `32` |
| `hnsw.efConstruction` | Size of dynamic candidate list | `400` |
| `hnsw.sq8Enabled` | Enable SQ8 (Scalar Quantization) | `false` |
| `hnsw.bqEnabled` | Enable BQ (Binary Quantization) | `false` |
| `hnsw.pqEnabled` | Enable PQ (Product Quantization) | `false` |
| `pq.m` | Number of sub-spaces for PQ | `16` |
| `pq.k` | Number of centroids per sub-space | `256` |
| `hnsw.turboQuantEnabled` | Enable SIMD-accelerated TurboQuant | `false` |
| `hnsw.geoSearchEnabled` | Enable geospatial search indexing | `false` |
| `hnsw.float16Enabled` | Use Float16 precision for vectors | `false` |
| `hnsw.useDisk` | Enable DiskANN-style disk offloading | `false` |
| `indexing.adaptive.enabled` | Enable adaptive indexing | `true` |
| `indexing.adaptive.threshold` | Threshold for adaptive migration | `1024` |
| `ingestion.workerCount` | Number of ingestion workers (0=auto) | `0` |
| `ingestion.adaptiveBatching` | Enable adaptive batching for puts | `true` |

### ML & Inference

| Parameter | Description | Default |
|-----------|-------------|---------|
| `learnedIndex.enabled` | Enable ML-based index selection | `false` |
| `learnedIndex.confidenceThreshold` | Confidence threshold for predictor | `0.7` |
| `learnedIndex.updateInterval` | Predictor training interval | `1h` |
| `ml.runner` | ML inference engine (`wazero`, `onnx`, `quarrel`) | `wazero` |
| `ml.reranker.enabled` | Enable search result reranking | `false` |
| `ml.reranker.type` | Reranker model type | `cross-encoder` |
| `ollama.enabled` | Enable Ollama local LLM integration | `false` |
| `ollama.endpoint` | Ollama API endpoint | `http://localhost:11434` |
| `ollama.model` | Embedding model for prediction | `""` |

### Cluster & Discovery

| Parameter | Description | Default |
|-----------|-------------|---------|
| `gossip.enabled` | Enable SWIM gossip protocol | `true` |
| `gossip.port` | UDP port for gossip communication | `7946` |
| `gossip.discovery.provider` | Node discovery provider (`k8s`, `dns`, `static`) | `k8s` |
| `gossip.discovery.labelSelector` | K8s label selector for discovery | `app.kubernetes.io/name=longbow` |
| `sharding.ring.enabled` | Enable consistent hashing (ring sharding) | `true` |
| `sharding.auto.enabled` | Enable automated data sharding | `true` |

### Integrations (S3, CDC, MQ)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `s3.enabled` | Enable S3-compatible snapshot storage | `false` |
| `s3.bucket` | S3 bucket name | `longbow-snapshots` |
| `s3.region` | S3 region | `us-east-1` |
| `s3.usePathStyle` | Use path-style addressing (MinIO) | `false` |
| `cdc.enabled` | Enable Change Data Capture (CDC) | `false` |
| `mq.enabled` | Enable message queue export | `false` |
| `mq.type` | MQ type (`kafka` or `pulsar`) | `kafka` |
| `websocket.enabled` | Enable real-time event WebSockets | `false` |

### Connectivity (Ingress & Gateway)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `ingress.data.enabled` | Enable Ingress for data plane | `false` |
| `ingress.meta.enabled` | Enable Ingress for meta plane | `false` |
| `gatewayAPI.enabled` | Enable Gateway API (HTTPRoute) support | `false` |
| `cloudflareTunnel.enabled` | Expose via Cloudflare Tunnel | `false` |

### Observability

| Parameter | Description | Default |
|-----------|-------------|---------|
| `metrics.enabled` | Enable Prometheus metrics export | `true` |
| `metrics.port` | Metrics scrape port | `9090` |
| `metrics.serviceMonitor.enabled` | Create Prometheus Operator ServiceMonitor | `false` |
| `config.logFormat` | Log format (`json` or `console`) | `json` |
| `config.logLevel` | Log level (`debug`, `info`, `warn`, `error`) | `info` |
