# Persistence & Storage

Longbow uses a tiered storage architecture designed for high throughput
ingestion and low-latency retrieval.

## Architecture

### Hot Tier (Memory)

- **Mutable Segments**: Incoming writes are stored as Arrow RecordBatches and indexed in the HNSW graph.
- **Auto-Sharding**: Small datasets use a single HNSW index; larger datasets are transparently migrated
  to a sharded, lock-striped `ShardedHNSW` for parallel insertion.

### Warm Tier (SSD Offloading)

- **Disk-Based Vectors**: When `LONGBOW_USE_DISK=1` is enabled, full-precision vectors are offloaded to an append-only file on SSD (`DiskVectorStore`).
- **Hybrid Index**: The HNSW graph and compressed vector representations (PQ/SQ8) remain in RAM for fast traversal, while the final re-ranking step retrieves full vectors from disk.
- **RAM Savings**: Significantly reduces memory footprint (e.g., 90% reduction for 3072d vectors), enabling billion-scale datasets on moderate hardware.

### Cold Tier (Object Storage)

- **Parquet Snapshots**: In-memory segments are periodically flushed to local disk or S3 as Parquet files.
- **Leveled Compaction**: Background workers automatically merge small batches into larger
  optimally-sized batches (e.g., 10k rows) to improve scan performance and reduce fragmentation.
- **Async Fsync**: WAL writes are buffered and fsync'd asynchronously to maximize ingestion throughput
  while maintaining durability.
- **WAL Size Limit**: A configurable limit ensures that the Write-Ahead Log (WAL) doesn't grow
  indefinitely. When reached, a snapshot is automatically triggered to compact data and truncate the WAL.

### Data Integrity

- **CRC32 Checksums**: All WAL entries are checksummed. The server validates integrity
  on startup (replay) and stops if corruption is detected.

### Graph Compaction (Vacuum)

While "Leveled Compaction" merges physical data (Arrow RecordBatches) on disk/S3, the in-memory HNSW
graph requires separate maintenance to handle deletions efficiently.

- **Problem**: Deleted nodes are logically hidden via a `Deleted` bitset but remain in the graph
  structure ("ghost nodes"), increasing traversal cost.
- **Solution (Vacuum)**: A background process (`CleanupTombstones`) periodically scans the graph:
  1. Identifies nodes marked as deleted.
  2. Prunes connections to these nodes from their neighbors.
  3. Atomically updates neighbor lists to maintain graph integrity.
- **Config**: Controlled via `CompactionWorker` settings. Runs automatically alongside data compaction.

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

Snapshots are written to a local directory. Best for development and single-node deployments.

```bash
export LONGBOW_DATA_PATH="/var/lib/longbow/data"
export LONGBOW_SNAPSHOT_INTERVAL="1h"
export LONGBOW_MAX_WAL_SIZE="104857600" # 100MB
export LONGBOW_STORAGE_ASYNC_FSYNC="true"
```

### S3 Backend

Snapshots are written to S3-compatible object storage. Recommended for production deployments
requiring durability and distributed access.

**Supported S3-compatible services:**

- Amazon S3, MinIO, Cloudflare R2, DigitalOcean Spaces, etc.

#### S3 Configuration

```bash
export LONGBOW_STORAGE_BACKEND="s3"
export LONGBOW_S3_BUCKET="longbow-snapshots"
export LONGBOW_S3_PREFIX="prod/vectors"
export LONGBOW_S3_REGION="us-east-1"
export LONGBOW_S3_ENDPOINT="http://minio.local:9000" # For MinIO
export LONGBOW_S3_USE_PATH_STYLE="true"              # For MinIO
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

    "github.com/23skdu/longbow/internal/storage"
)

func main() {
    cfg := &storage.S3BackendConfig{
        Endpoint:        "http://minio:9000",  // Empty for AWS S3
        Bucket:          "longbow-snapshots",
        Prefix:          "prod/vectors",
        AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
        SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
        Region:          "us-east-1",
        UsePathStyle:    true,  // Required for MinIO
    }

    backend, err := storage.NewS3Backend(cfg)
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Write a snapshot file
    err = backend.WriteSnapshotFile(ctx, "my_collection", ".parquet", dataReader)
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
        if storage.IsNotFoundError(err) {
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
