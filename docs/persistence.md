# Persistence & Storage

Longbow uses a tiered storage architecture designed for high throughput
ingestion and low-latency retrieval.

## Architecture

### Hot Tier (Memory)

- **Mutable Segment**: Incoming writes go here. Implemented as a concurrent
  skip-list.
- **Immutable Segments**: When the mutable segment fills up, it is sealed and
  becomes immutable.

### Cold Tier (Object Storage)

- **Parquet Snapshots**: Immutable segments are periodically flushed to disk or
  S3 as Parquet files.
- **Leveled Compaction**: Background workers automatically merge small batches (e.g., 10x100 rows) into larger optimally-sized batches (e.g., 1x1000 rows) to improve scan performance and reduce fragmentation.
- **Async Fsync**: WAL writes are buffered and fsync'd asynchronously (default 1s interval) to maximize ingestion throughput while maintaining durability guarantees.
- **WAL Size Limit**: A configurable limit (default 100MB) ensures that the
  Write-Ahead Log (WAL) doesn't grow indefinitely. When the limit is reached, a
  snapshot is automatically triggered to compact the data and truncate the WAL.

### Data Integrity

- **CRC32 Checksums**: All WAL entries are checksummed. The server validates integrity
  on startup (replay) and stops if corruption is detected.

## Data Format

Longbow stores vectors and metadata in **Apache Parquet** format. This allows
for:

- **Columnar Compression**: Efficient storage of homogeneous data.
- **Zero-Copy Reads**: Mapping data directly from disk to memory.
- **Ecosystem Compatibility**: Querying snapshots directly with tools like
  Spark, Pandas, or other Arrow-compatible tools.

### Schema

| Column     | Type                     | Description                             |
| :--------- | :----------------------- | :-------------------------------------- |
| id         | int64                    | Unique identifier for the vector.       |
| vector     | fixed_size_binary_array  | The embedding vector (e.g., 1536 dims). |
| metadata   | binary (JSON)            | Associated metadata blob.               |
| created_at | timestamp                | Ingestion timestamp.                    |

## Storage Backends

Longbow supports two storage backends for snapshots:

| Backend    | Use Case                          | Configuration       |
| :--------- | :-------------------------------- | :------------------ |
| Local Disk | Development, single-node setups   | `storage.path`      |
| S3         | Production, distributed, durable  | `storage.s3.*`      |

### Local Disk Backend (Default)

Snapshots are written to a local directory. Best for development and
single-node deployments.

```yaml
storage:
  backend: "local"
  path: "/var/lib/longbow/data"
  flush_interval_ms: 10000
  max_segment_size: 1048576
  compression: "snappy"
  max_wal_size: 104857600  # 100MB
```

### S3 Backend

Snapshots are written to S3-compatible object storage. Recommended for
production deployments requiring durability and distributed access.

**Supported S3-compatible services:**

- Amazon S3
- MinIO
- Cloudflare R2
- DigitalOcean Spaces
- Backblaze B2

#### S3 Configuration

```yaml
storage:
  backend: "s3"
  flush_interval_ms: 10000
  max_segment_size: 1048576
  compression: "snappy"
  max_wal_size: 104857600
  s3:
    endpoint: ""                    # Leave empty for AWS S3
    bucket: "longbow-snapshots"
    prefix: "prod/vectors"          # Optional key prefix
    region: "us-east-1"
    use_path_style: false           # Set true for MinIO
```

#### S3 Credentials

Credentials are provided via environment variables (recommended) or config:

```bash
# Environment variables (recommended)
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"

# Optional: for AWS IAM roles
export AWS_SESSION_TOKEN="your-session-token"
```

Or in configuration (not recommended for production):

```yaml
storage:
  s3:
    access_key_id: "your-access-key"
    secret_access_key: "your-secret-key"
```

#### MinIO Example

For MinIO or other S3-compatible services, set the endpoint and enable
path-style addressing:

```yaml
storage:
  backend: "s3"
  s3:
    endpoint: "http://minio.local:9000"
    bucket: "longbow-snapshots"
    prefix: "dev"
    region: "us-east-1"             # Required even for MinIO
    use_path_style: true            # Required for MinIO
```

```bash
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
```

#### S3 Snapshot Path Structure

Snapshots are stored with the following key pattern:

```text
{prefix}/snapshots/{collection_name}.parquet
```

Examples:

| Prefix         | Collection    | S3 Key                                  |
| :------------- | :------------ | :-------------------------------------- |
| (none)       | my_vectors  | `snapshots/my_vectors.parquet`             |
| prod         | embeddings  | `prod/snapshots/embeddings.parquet`        |
| prod/vectors | user_data   | `prod/vectors/snapshots/user_data.parquet` |

## Programmatic S3 Backend Usage

The S3 backend can be used programmatically in Go:

```go
package main

import (
    "context"
    "log"
    "os"

    "github.com/23skdu/longbow/internal/store"
)

func main() {
    cfg := &store.S3BackendConfig{
        Endpoint:        "http://minio:9000",  // Empty for AWS S3
        Bucket:          "longbow-snapshots",
        Prefix:          "prod/vectors",
        AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
        SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
        Region:          "us-east-1",
        UsePathStyle:    true,  // Required for MinIO
    }

    backend, err := store.NewS3Backend(cfg)
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Write a snapshot
    err = backend.WriteSnapshot(ctx, "my_collection", parquetData)
    if err != nil {
        log.Fatal(err)
    }

    // List all snapshots
    collections, err := backend.ListSnapshots(ctx)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Collections: %v", collections)

    // Read a snapshot
    reader, err := backend.ReadSnapshot(ctx, "my_collection")
    if err != nil {
        if store.IsNotFoundError(err) {
            log.Println("Snapshot not found")
        } else {
            log.Fatal(err)
        }
    }
    defer func() { _ = reader.Close() }()
}
```

## Metrics

| Metric                             | Type      | Labels | Description                      |
| :--------------------------------- | :-------- | :----- | :------------------------------- |
| `longbow_flush_ops_total`          | Counter   | status | Total number of flush operations |
| `longbow_snapshot_duration_seconds`| Histogram | -      | Duration of the snapshot process |

## Best Practices

### Development

```yaml
storage:
  backend: "local"
  path: "./data"
  max_wal_size: 10485760  # 10MB for faster iterations
```

### Production (AWS S3)

```yaml
storage:
  backend: "s3"
  s3:
    bucket: "mycompany-longbow-prod"
    prefix: "vectors"
    region: "us-west-2"
```

### Production (Self-hosted MinIO)

```yaml
storage:
  backend: "s3"
  s3:
    endpoint: "https://minio.internal.mycompany.com"
    bucket: "longbow"
    prefix: "prod"
    region: "us-east-1"
    use_path_style: true
```
