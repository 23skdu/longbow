# TurboQuant Extreme Compression

TurboQuant is Longbow's state-of-the-art vector compression engine, designed to achieve **6-8x memory reduction** with minimal loss in search recall. It is ideal for large-scale deployments where memory footprint is the primary cost driver.

## Architecture

TurboQuant uses a multi-stage compression pipeline:

1. **Random Rotation (Hadamard Transform)**: Spreads the vector energy across all dimensions using a SIMD-accelerated Fast Walsh-Hadamard Transform (FWHT). This ensures that the information is uniformly distributed before quantization.
2. **PolarQuant**: Converts pairs of Cartesian coordinates into recursive polar coordinates (Radius + Angles). Angles are quantized to 3 or 4 bits.
3. **QJL (1-bit Error Correction)**: Applies 1-bit sign-correction based on Johnson-Lindenstrauss transforms to the residual error, eliminating bias and improving dot-product estimation accuracy.

## Creating a TurboQuant Dataset

### Via gRPC/Flight (DoAction)

```json
{
  "type": "create_dataset",
  "body": {
    "name": "my_tq_dataset",
    "dimension": 768,
    "vector_type": "turboquant",
    "turboquant_bits": 8,
    "metric": "l2"
  }
}
```

### Via Python SDK

```python
client.create_namespace(
    name="my_tq_dataset",
    dims=768,
    data_type="turboquant"
)
```

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `vector_type` | (required) | Must be set to `"turboquant"` |
| `turboquant_bits` | 8 | Quantization bits: 4 or 8 bits per dimension |
| `metric` | `"l2"` | Distance metric: `"l2"`, `"cosine"`, or `"dot_product"` |
| `bits_per_angle` | 3 | Quantization depth (3 or 4 bits). 3 bits yields ~7-8x compression |
| `rotation_seed` | 42 | Seed for random rotation matrix. Must be consistent across cluster nodes |

## Ingestion Format

TurboQuant datasets expect standard `float32` vectors during ingestion. Longbow automatically performs the TQ transformation and quantization in the background as part of the ingestion pipeline.

```python
# Standard float32 ingestion - automatically compressed
vectors = [[0.1, 0.2, ...] for _ in range(1000)]
client.insert("my_tq_dataset", [{"id": i, "vector": v} for i, v in enumerate(vectors)])
```

## Search Parameters

When searching a TurboQuant-enabled dataset, you can optionally specify the bits to use for the query (though it usually matches the dataset configuration).

```python
results = client.search(
    dataset="my_tq_dataset",
    vector=[0.1, 0.2, ...],
    k=10,
    vector_type="turboquant",
    turboquant_bits=8
)
```

## Performance Comparison

### Compression Ratio

| Metric | Float32 Index | TurboQuant (8-bit) | TurboQuant (4-bit) |
|--------|---------------|-------------------|---------------------|
| Storage Size | 100 GB | 25 GB | 12.5 GB |
| Compression | 1x | 4x | 8x |

### Benchmark Results

| Metric | Float32 Index | TurboQuant (8-bit) | Improvement |
|--------|---------------|-------------------|-------------|
| Ingestion QPS | 5,000 | 12,000 | 2.4x speedup |
| Search QPS (K=10) | 1,200 | 4,500 | 3.7x speedup |
| Memory/Vector | 4 bytes | 1 byte | 4x reduction |

*Notes: Benchmarks performed on 128-core ARM64 instances with 1M vectors of 768 dimensions.*

### Recall Performance

| Configuration | Recall@10 |
|--------------|-----------|
| Standard HNSW | 0.98 |
| TurboQuant (8-bit) | 0.95 |
| TurboQuant (4-bit) | 0.88 |

## Comparison with Other Quantizers

| Metric | Scalar (SQ8) | Product (PQ) | TurboQuant |
|--------|---------------|--------------|------------|
| Compression | 4x | 4-16x | **6-8x** |
| Recall | High | Medium | **High** |
| Search Speed | Fast | Slow | **Fast** |
| Accuracy | 99% | 85-95% | **95-98%** |

> [!NOTE]
> TurboQuant is mathematically optimized for high-dimensional embedding spaces where traditional Scalar Quantization suffers from the "curse of dimensionality".

## Technical Details

- **Random Rotation**: TQ uses a randomized orthogonal matrix to balance the variance across dimensions before quantization.
- **Fast Walsh-Hadamard Transform (FWHT)**: For higher dimensions, TQ employs FWHT to achieve efficient rotation.
- **SIMD Acceleration**: The TQ kernels are highly optimized using ARM NEON and x86 AVX-512 instructions.

## Use Cases

### When to Use TurboQuant

- **Memory-constrained environments**: 4-8x memory reduction
- **High-volume ingestion**: 2-3x faster than float32
- **Large-scale deployments**: Reduce infrastructure costs
- **Latency-sensitive workloads**: Higher QPS with maintained recall

### When NOT to Use TurboQuant

- **Maximum recall required**: Use float32 for highest accuracy
- **Low-dimensional data**: Scalar quantization may be sufficient
- **Binary/sparse vectors**: Consider alternative encodings

## Advanced Settings

### Custom Rotation Seed

```json
{
  "name": "my_dataset",
  "dimension": 768,
  "vector_type": "turboquant",
  "rotation_seed": 12345
}
```

### 4-bit Mode (Higher Compression)

```json
{
  "name": "my_dataset",
  "dimension": 768,
  "vector_type": "turboquant",
  "turboquant_bits": 4
}
```

## Troubleshooting

### Dimension Mismatch

If you see dimension errors, ensure the embedding model output matches the dataset dimension:

```
Error: dimension mismatch — expected 768, received 384
```

Recreate the dataset with the correct dimension or verify your embedding model configuration.

### Poor Recall

If recall is lower than expected:
- Try 8-bit mode instead of 4-bit
- Increase HNSW `ef_construction` parameter
- Adjust HNSW `m` parameter for more connections

## See Also

- [GPU Acceleration](gpu-acceleration.md) - Using TurboQuant with GPU
- [Performance Benchmarks](performance.md) - Full benchmark matrix
- [Vector Search](vectorsearch.md) - Search configuration options
