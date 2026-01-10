# Product Quantization (PQ) for Vector Index

## Overview

Product Quantization (PQ) compresses high-dimension vectors into compact codes by splitting them into sub-vectors and quantizing each part separately. This significantly reduces memory usage and speeds up distance calculations using lookup tables (ADC).

## Goals

1. **Compression**: Reduce vector storage footprint (e.g., 64x reduction for 1024 dims -> 16 bytes).
2. **Performance**: Accelerate HNSW distance computations using ADC (Asymmetric Distance Computation).
3. **Integration**: Seamlessly integrate with existing `ArrowHNSW` index.

## Implementation Details

### Data Structures

- **`PQEncoder`**: Handles training (K-Means) and encoding/decoding.
- **`QuantizedVectors`**: Stores compressed codes (e.g., `[]byte` or `Arrow` Buffer).

### HNSW Integration

- Modify `ArrowHNSW` to support a `Quantized` mode.
- In `Quantized` mode, the graph traversal uses PQ distance (ADC) instead of full L2/Cosine.
- Full vectors are still stored (optionally on disk) for re-ranking or eventual consistency, but the in-memory index uses PQ codes.

### Configuration

- `num_subvectors` (M): Number of sub-spaces (e.g., 16, 32, 64).
- `num_centroids` (k*): Number of centroids per sub-space (usually 256 for 1 byte codes).
- `training_sample_size`: Number of vectors to use for K-means training.

## Roadmap

1. **Core PQ Logic**: Implement Encoder, K-Means Clustering, and Distance Tables.
2. **Storage**: extend `VectorStore` to persist PQ codes.
3. **Search**: Implement ADC distance function for HNSW.
4. **Refactoring**: Update `ArrowHNSW` to switch between Raw and PQ modes.
