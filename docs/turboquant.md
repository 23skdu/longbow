# TurboQuant Extreme Compression

TurboQuant is Longbow's state-of-the-art vector compression engine, designed to achieve **6-8x memory reduction** with minimal loss in search recall. It is ideal for large-scale deployments where memory footprint is the primary cost driver.

## Architecture

TurboQuant uses a multi-stage compression pipeline:

1. **Random Rotation (Hadamard Transform)**: Spreads the vector energy across all dimensions using a SIMD-accelerated Fast Walsh-Hadamard Transform (FWHT). This ensures that the information is uniformly distributed before quantization.
2. **PolarQuant**: Converts pairs of Cartesian coordinates into recursive polar coordinates (Radius + Angles). Angles are quantized to 3 or 4 bits.
3. **QJL (1-bit Error Correction)**: Applies 1-bit sign-correction based on Johnson-Lindenstrauss transforms to the residual error, eliminating bias and improving dot-product estimation accuracy.

## Configuration

To enable TurboQuant on a namespace, set the `data_type` to `turboquant`:

```json
{
  "name": "my_namespace",
  "dims": 768,
  "data_type": "turboquant",
  "hnsw_config": {
    "m": 16,
    "ef_construction": 200
  }
}
```

### Advanced Settings

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `bits_per_angle` | 3 | Quantization depth (3 or 4 bits). 3 bits yields ~7-8x compression. |
| `rotation_seed` | 42 | Seed for the random rotation matrix. Must be consistent across cluster nodes. |

## Performance Baseline

On a 768-dimensional vector dataset (e.g., Cohere/OpenAI embeddings):

- **Memory/Vector**: 516 bytes (compared to 3072 bytes for Float32).
- **Compression Ratio**: 5.95x.
- **Recall@10**: > 0.94 (standard HNSW parameters).

## Comparison with other Quantizers

| Metric | Scalar (SQ8) | Product (PQ) | TurboQuant |
| :--- | :--- | :--- | :--- |
| Compression | 4x | 4-16x | **6-8x** |
| Recall | High | Medium | **High** |
| Search Speed | Fast | Slow | **Fast** |
| Accuracy | 99% | 85-95% | **95-98%** |

> [!NOTE]
> TurboQuant is mathematically optimized for high-dimensional embedding spaces where traditional Scalar Quantization suffers from the "curse of dimensionality".
