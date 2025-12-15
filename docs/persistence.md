# Persistence & Storage

Longbow uses a tiered storage architecture designed for high throughput ingestion and low-latency retrieval.

## Architecture

### Hot Tier (Memory)

* **Mutable Segment**: Incoming writes go here. Implemented as a concurrent skip-list.

* **Immutable Segments**: When the mutable segment fills up, it is sealed and becomes immutable.

### Cold Tier (Object Storage)

* **Parquet Snapshots**: Immutable segments are periodically flushed to disk/S3 as Parquet files.

* **Compaction**: Background processes merge small Parquet files into larger ones to optimize read performance.

## Data Format

Longbow stores vectors and metadata in **Apache Parquet** format. This allows for:

* **Columnar Compression**: Efficient storage of homogeneous data.

* **Zero-Copy Reads**: Mapping data directly from disk to memory.

* **Ecosystem Compatibility**: Querying snapshots directly with tools like DuckDB, Spark, or Pandas.

### Schema

| Column | Type | Description |
| :--- | :--- | :--- |
| id | int64 | Unique identifier for the vector. |
| vector | fixed_size_binary_array | The embedding vector (e.g., 1536 dimensions). |
| metadata | binary (JSON) | Associated metadata blob. |
| created_at | timestamp | Ingestion timestamp. |

## Configuration

Persistence is configured via the longbow.yaml file:

yaml
storage:
  path: "/var/lib/longbow/data"
  flush_interval_ms: 10000
  max_segment_size: 1048576
  compression: "snappy"

## Metrics

| Metric | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| longbow_flush_ops_total | Counter | status | Total number of flush operations. |
| longbow_snapshot_duration_seconds | Histogram | - | Duration of the snapshot process. |

## 🦆 Analytical Sidecar (DuckDB Integration)

Longbow includes an embedded **DuckDB** adapter that allows you to run complex SQL OLAP queries directly against your Parquet snapshots without loading them into the main memory.

### How it works

The Meta Server (port 3001) exposes a query_analytics action via the Arrow Flight DoAction interface. This action:

1. Accepts a JSON payload specifying the target dataset and the SQL query.

2. Spins up an ephemeral, in-process DuckDB instance.

3. Registers the dataset's Parquet snapshot as a virtual table.

4. Executes the query and returns the results as a JSON string.

### Use Cases

* **Ad-hoc Analytics**: "What is the average vector norm for data ingested yesterday?"

* **Data Quality Checks**: "Count rows where metadata field 'source' is null."

* **Debugging**: Inspecting the raw contents of the cold storage.

### Example Usage (Python)

python
import json
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:3001")
action_body = json.dumps({
 "dataset": "my_dataset",
 "query": "SELECT count(*) FROM my_dataset WHERE id > 1000"
}).encode('utf-8')

results = client.do_action(flight.Action("query_analytics", action_body))
for result in results:
 print(result.body.to_pybytes().decode('utf-8'))
