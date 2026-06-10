# TurboQuant

## Executive Summary

TurboQuant is Longbow's proprietary two-stage vector compression algorithm that delivers **4x-64x storage reduction** for float32 vectors while maintaining fast approximate nearest neighbor search capabilities. It combines **Polar Quantization** with **Quantized JL (QJL)** transformations to achieve extreme compression with tunable accuracy.

TurboQuant is the foundational vector compression technology that enables Longbow to deliver high-density embeddings storage at scale for production AI/ML workloads.

---

## Core Algorithm

TurboQuant implements a two-stage compression pipeline:

1. **Random Rotation (Hadamard Transform)**: Vectors are randomly rotated using a Walsh-Hadamard transform to decorrelate dimensions for effective quantization.

2. **Stage 1 - Recursive PolarQuant**: The rotated vector is converted to polar coordinates:
   - 1 radius value (float32)
   - (pow2-1) angles that are bit-packed (2, 3, 4, or 8 bits per angle)

3. **Stage 2 - QJL Correction**: A Quantized JL correction term that stores the sign bit of the reconstruction residual for improved accuracy.

4. **Packing Format**: `[Radius (4B)][Packed Angles (Variable)][QJL Bits (Variable)]`

---

## Features

### Encoding & Decoding
- **Configurable bit depth**: 2, 3, 4, or 8 bits per angle
- **Automatic power-of-2 padding** for dimensions not a power of 2
- **Lossy compression** with tunable accuracy vs. storage trade-off
- **Dimensions supported**: 128 to 3072 (non-power-of-2 dims like 384, 768 work correctly; the SIMD kernel truncates query vectors to the original dimension length, not the padded power-of-2 length)

### Search Integration
- **HNSW index support** for fast approximate k-NN search
- **CPU SIMD acceleration** using AVX2/NEON
- **GPU (CUDA) kernels** for accelerated distance computation
- **Distance metrics**: L2, Cosine supported

### Auto-Tuning
- **QuantizationTuner**: Automatic selection between float32/int8/PQ/TQ based on:
  - Memory pressure
  - Query load (QPS)
  - Recall requirements
- **Adaptive re-quantization** for live datasets

---

## Compression Ratios

| Original Dims | Bits per Angle | Original Size | TQ Size | Compression |
|--------------|--------------|--------------|---------|--------------|
| 128 | 4-bit | 512 bytes | ~128 bytes | **4x** |
| 384 | 4-bit | 1536 bytes | ~288 bytes | **5.3x** |
| 768 | 3-bit | 3072 bytes | ~385 bytes | **8x** |
| 768 | 2-bit | 3072 bytes | ~256 bytes | **12x** |
| 768 | 8-bit | 3072 bytes | ~640 bytes | **4.8x** |

---

## Usage

### Python SDK

```python
client.create_dataset(
    name="my_dataset",
    dimensions=768,
    vector_type="turboquant",
    turboquant_bits=4,
    metric="cosine"
)
```

### Arrow Flight Action

```json
{
  "name": "my_dataset",
  "dimension": 768,
  "vector_type": "turboquant",
  "turboquant_bits": 4,
  "metric": "cosine"
}
```

### CLI

```bash
longbow-cli create-namespace -name my_ns -dims 768 -data_type turboquant
```

---

## Supported Data Types

| String | Alias |
|--------|-------|
| `"turboquant"` | `"tq"` |
| `"turboquant2"` | - |
| `"turboquant4"` | - |
| `"turboquant8"` | - |

---

## Configuration

### Dataset Creation Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `vector_type` | string | - | Set to `"turboquant"` or `"tq"` |
| `turboquant_bits` | int | 4 | Bits per angle (2, 4, or 8) |
| `dimension` | int | - | Vector dimensions (128-3072) |
| `metric` | string | `"cosine"` | Distance metric |

---

## Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `longbow_turboquant_encoding_total` | Counter | `dataset`, `direction` | Encoding operations |
| `longbow_turboquant_encoding_latency_seconds` | Histogram | `dataset` | Server encoding latency |
| `longbow_turboquant_storage_bytes_total` | Gauge | `dataset` | Storage bytes used |
| `longbow_turboquant_search_total` | Counter | `dataset`, `bit_width` | Search count |
| `longbow_turboquant_search_latency_seconds` | Histogram | `dataset`, `bit_width` | Search latency |

---

## Architecture

```
┌─────────────────────────────────────┐
│         CLIENT SDK                  │
│  create_dataset(turboquant_bits=4)  │
└─────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│    ARROW FLIGHT ACTION               │
│  store_actions.go: create_dataset   │
└─────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│     DATASET CREATION                 │
│  Stores TQ config in metadata       │
└─────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│       HNSW INDEX                    │
│  Uses TurboQuantCompute             │
└─────────────────────────────────────┘
        │                   │
        ▼                   ▼
┌──────────────┐    ┌──────────────┐
│  ENCODING    │    │   SEARCH     │
│  (Ingest)    │    │   (Query)    │
└──────────────┘    └──────────────┘
        │                   │
        ▼                   ▼
┌──────────────┐    ┌──────────────┐
│  Encode()    │    │  CPU SIMD    │
│  Pack()     │    │  CUDA Kernel │
└──────────────┘    └──────────────┘
```

---

## File Reference

| File | Purpose |
|------|---------|
| `internal/store/internal/core/turboquant.go` | Primary encoder/decoder |
| `internal/store/internal/core/arrow_hnsw_compute_tq.go` | HNSW TQ compute |
| `internal/store/turboquant_storage.go` | Storage constants/helpers |
| `internal/gpu/cuda/kernels.cu` | CUDA distance kernel |
| `internal/store/quantization_tuner.go` | Auto-tuner |
| `internal/metrics/storage_metrics.go` | Prometheus metrics |