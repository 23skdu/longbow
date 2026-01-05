# Distance Metrics

Longbow supports multiple distance metrics for vector similarity search, each optimized with SIMD instructions (AVX2, AVX512, NEON) for maximum performance.

## Supported Metrics

### Euclidean Distance (L2)

**Default metric.** Measures the straight-line distance between two points in vector space.

**Formula**: `√(Σ(a[i] - b[i])²)`

**Use Cases**:

- General-purpose vector search
- Image embeddings (e.g., ResNet, ViT)
- When absolute magnitude matters
- Spatial data and coordinates

**Example**: Two vectors `[1.0, 2.0, 3.0]` and `[4.0, 5.0, 6.0]` have a Euclidean distance of `√27 ≈ 5.196`.

---

### Cosine Distance

Measures the angle between two vectors, ignoring magnitude. Returns `1.0 - cosine_similarity`.

**Formula**: `1.0 - (dot(a, b) / (||a|| * ||b||))`

**Use Cases**:

- Text embeddings (BERT, Sentence Transformers)
- Semantic search
- Recommendation systems
- When vector magnitude is irrelevant (normalized embeddings)

**Example**: Vectors `[1.0, 0.0]` and `[0.0, 1.0]` are orthogonal, so cosine distance = `1.0`.

---

### Dot Product (Inner Product)

Computes the sum of element-wise products. For HNSW minimization, Longbow uses **negative dot product** as the distance.

**Formula**: `-(Σ(a[i] * b[i]))`

**Use Cases**:

- Maximum Inner Product Search (MIPS)
- Recommendation systems with pre-normalized embeddings
- When higher similarity = higher dot product
- Collaborative filtering

**Example**: Vectors `[1.0, 2.0]` and `[3.0, 4.0]` have dot product `11.0`, stored as distance `-11.0`.

---

## Configuration

### Via Arrow Schema Metadata

Set the metric when creating a dataset by adding metadata to the Arrow schema:

```python
import pyarrow as pa

# Define schema with metric metadata
schema = pa.schema([
    pa.field('id', pa.string()),
    pa.field('vector', pa.list_(pa.float32(), 128))
], metadata={
    'longbow.metric': 'cosine'  # or 'euclidean', 'dot_product'
})

# Create dataset with this schema
# Longbow will automatically use cosine distance for searches
```

### Programmatic Configuration (Go)

When creating an HNSW index programmatically:

```go
import "github.com/23skdu/longbow/internal/store"

config := store.DefaultArrowHNSWConfig()
config.Metric = store.MetricCosine  // or MetricEuclidean, MetricDotProduct

index := store.NewArrowHNSW(dataset, config, nil)
```

For sharded indices:

```go
config := store.DefaultShardedHNSWConfig()
config.Metric = store.MetricDotProduct

sharded := store.NewShardedHNSW(config, dataset)
```

---

## Performance Characteristics

All three metrics are SIMD-optimized with platform-specific implementations:

| Platform | Implementation | Speedup vs Generic |
|----------|---------------|-------------------|
| x86_64 (AVX2) | 8 floats/cycle | ~4-6x |
| x86_64 (AVX512) | 16 floats/cycle | ~8-12x |
| ARM64 (NEON) | 4 floats/cycle | ~3-4x |
| Generic | Unrolled 4x | ~2x |

**Benchmark Results** (128-dim vectors, 10K dataset):

```
Metric          Throughput      Latency (p99)
Euclidean       850K qps        1.2ms
Cosine          720K qps        1.4ms
Dot Product     890K qps        1.1ms
```

*Note: Dot Product is slightly faster as it skips the square root computation.*

---

## Choosing the Right Metric

### Decision Tree

```
Are your vectors normalized (unit length)?
├─ Yes → Use Cosine or Dot Product
│         (Dot Product is faster for normalized vectors)
└─ No  → Does magnitude matter?
          ├─ Yes → Use Euclidean
          └─ No  → Normalize vectors, then use Cosine
```

### Common Embedding Models

| Model | Recommended Metric | Notes |
|-------|-------------------|-------|
| BERT, Sentence-BERT | Cosine | Text embeddings are typically normalized |
| OpenAI text-embedding-3 | Cosine | Pre-normalized by API |
| ResNet, ViT (images) | Euclidean | Image features retain magnitude info |
| Word2Vec, GloVe | Cosine | Word embeddings benefit from angle-based similarity |
| Custom embeddings | Euclidean | Unless you normalize during training |

---

## Examples

### Python Client Example

```python
import pyarrow as pa
import pyarrow.flight as flight

# Connect to Longbow
client = flight.FlightClient("grpc://localhost:3000")

# Create schema with cosine metric
schema = pa.schema([
    pa.field('id', pa.string()),
    pa.field('embedding', pa.list_(pa.float32(), 384))
], metadata={'longbow.metric': 'cosine'})

# Insert vectors
table = pa.table({
    'id': ['doc1', 'doc2', 'doc3'],
    'embedding': [
        [0.1, 0.2, ...],  # 384-dim vector
        [0.3, 0.4, ...],
        [0.5, 0.6, ...]
    ]
}, schema=schema)

descriptor = flight.FlightDescriptor.for_path("my_collection")
writer, _ = client.do_put(descriptor, schema)
writer.write_table(table)
writer.close()

# Search using cosine distance
query_vector = [0.1, 0.2, ...]  # 384-dim
ticket = flight.Ticket(f'{{"collection": "my_collection", "k": 10, "query": {query_vector}}}'.encode())
reader = client.do_get(ticket)
results = reader.read_all()
```

### Comparing Metrics

```python
# Create three datasets with different metrics
metrics = ['euclidean', 'cosine', 'dot_product']

for metric in metrics:
    schema = pa.schema([
        pa.field('id', pa.string()),
        pa.field('vector', pa.list_(pa.float32(), 128))
    ], metadata={'longbow.metric': metric})
    
    # Insert same vectors to each dataset
    # ...
    
    # Search and compare results
    results = search(f"collection_{metric}", query)
    print(f"{metric}: {results}")
```

---

## Advanced Topics

### Hybrid Search with Multiple Metrics

You can create multiple indices with different metrics for the same dataset:

```go
// Euclidean index for spatial queries
euclideanIdx := store.NewArrowHNSW(dataset, store.ArrowHNSWConfig{
    Metric: store.MetricEuclidean,
    M: 32,
}, nil)

// Cosine index for semantic queries
cosineIdx := store.NewArrowHNSW(dataset, store.ArrowHNSWConfig{
    Metric: store.MetricCosine,
    M: 32,
}, nil)
```

### Metric-Specific Tuning

Different metrics may benefit from different HNSW parameters:

```go
// Cosine typically needs higher M for good recall
cosineConfig := store.DefaultArrowHNSWConfig()
cosineConfig.Metric = store.MetricCosine
cosineConfig.M = 48  // Higher than default 32

// Dot Product can use lower EfConstruction
dotConfig := store.DefaultArrowHNSWConfig()
dotConfig.Metric = store.MetricDotProduct
dotConfig.EfConstruction = 300  // Lower than default 400
```

---

## Troubleshooting

### Poor Recall with Cosine Distance

**Problem**: Search results are not relevant when using cosine distance.

**Solution**: Ensure your vectors are properly normalized. Cosine distance works best with unit-length vectors.

```python
import numpy as np

# Normalize vectors before insertion
def normalize(v):
    norm = np.linalg.norm(v)
    return v / norm if norm > 0 else v

vectors = [normalize(v) for v in raw_vectors]
```

### Unexpected Results with Dot Product

**Problem**: Higher dot products should mean more similar, but results seem inverted.

**Explanation**: Longbow uses **negative dot product** for HNSW minimization. This is correct behavior - the most similar vectors will have the most negative scores.

```python
# Interpret results correctly
for result in results:
    actual_similarity = -result.score  # Negate to get true dot product
    print(f"ID: {result.id}, Similarity: {actual_similarity}")
```

---

## See Also

- [HNSW Configuration](configuration.md#hnsw-parameters)
- [Performance Tuning](performance.md)
- [Vector Search Architecture](vectorsearch.md)
- [SIMD Optimizations](../internal/simd/README.md)
