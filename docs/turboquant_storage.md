# TurboQuant Storage and Indexing

TurboQuant (TQ) is a high-performance vector quantization and indexing system in Longbow designed for massive scale and efficiency. It provides significant reductions in storage space and search latency compared to standard float32 indexing.

## Creating a TurboQuant Dataset

To use TurboQuant, you must explicitly configure the dataset at creation time using the `create_dataset` action.

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

- **vector_type**: Must be set to `"turboquant"`.
- **turboquant_bits**: (Optional) 4 or 8 bits per dimension. Default is 8.
- **metric**: `"l2"`, `"cosine"`, or `"dot_product"`.

## Ingestion Format

TurboQuant datasets expect standard `float32` vectors during ingestion. Longbow automatically performs the TQ transformation and quantization in the background as part of the ingestion pipeline.

## Search Parameters

When searching a TurboQuant-enabled dataset, you can optionally specify the bits to use for the query (though it usually matches the dataset configuration).

### Search Request (JSON)

```json
{
  "dataset": "my_tq_dataset",
  "vector": [0.1, 0.2, ...],
  "k": 10,
  "vector_type": "turboquant",
  "turboquant_bits": 8
}
```

## Performance Benefits

| Metric | Float32 Index | TurboQuant (8-bit) | Improvement |
|--------|---------------|-------------------|-------------|
| Storage Size | 100 GB | 25 GB | 4x reduction |
| Ingestion QPS | 5,000 | 12,000 | 2.4x speedup |
| Search QPS (K=10) | 1,200 | 4,500 | 3.7x speedup |

*Notes: Benchmarks performed on 128-core ARM64 instances with 1M vectors of 768 dimensions.*

## Technical Details

- **Random Rotation**: TQ uses a randomized orthogonal matrix to balance the variance across dimensions before quantization.
- **Fast Walsh-Hadamard Transform (FWHT)**: For higher dimensions, TQ employs FWHT to achieve efficient rotation.
- **SIMD Acceleration**: The TQ kernels are highly optimized using ARM NEON and x86 AVX-512 instructions.
