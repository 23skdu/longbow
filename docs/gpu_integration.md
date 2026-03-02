# GPU Integration Guide

This guide covers integrating GPU acceleration into your Longbow vector store applications.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [API Usage](#api-usage)
- [Configuration Options](#configuration-options)
- [Performance Tuning](#performance-tuning)
- [Examples](#examples)

## Overview

Longbow provides GPU-accelerated vector search through pluggable backends:

- **CUDA**: NVIDIA GPU acceleration on Linux
- **Metal**: Apple Silicon GPU acceleration on macOS
- **CPU**: Fallback CPU implementation

The GPU implementation is designed to be:

- **Transparent**: Works alongside existing CPU-based HNSW indexes
- **Flexible**: Automatic backend detection and fallback
- **Efficient**: Hybrid search combining GPU and CPU strengths

## Quick Start

### Basic GPU Initialization

```go
package main

import (
    "log"
    "github.com/23skdu/longbow/internal/gpu"
    "github.com/23skdu/longbow/internal/store"
)

func main() {
    // Create vector store
    vs, err := store.New("vectors", 128)
    if err != nil {
        log.Fatal(err)
    }

    // Enable GPU acceleration
    if hnsw, ok := vs.Index().(*store.ArrowHNSW); ok {
        err = hnsw.InitGPU(0, log.Logger)
        if err != nil {
            log.Printf("GPU init failed, using CPU: %v", err)
        }
    }

    // Use the store normally - GPU will be used automatically when available
}
```

### Backend Detection

```go
// Detect available GPU backend
backend := gpu.DetectGPUBackend()
switch backend {
case gpu.BackendCUDA:
    log.Println("Using NVIDIA CUDA")
case gpu.BackendMetal:
    log.Println("Using Apple Metal")
case gpu.BackendCPU:
    log.Println("Using CPU fallback")
}

// Check GPU requirements
available, reason, err := gpu.GetGPURequirements(gpu.BackendCUDA)
if err != nil {
    log.Printf("GPU check error: %v", err)
} else if !available {
    log.Printf("GPU not available: %s", reason)
}
```

## API Usage

### Creating a GPU Index

```go
import "github.com/23skdu/longbow/internal/gpu"

// Configuration
cfg := gpu.GPUConfig{
    Backend:       gpu.BackendCUDA,    // or BackendMetal
    DeviceID:      0,                  // GPU device ID
    Dimension:     128,                // Vector dimension
    Enabled:       true,               // Enable GPU
    SyncBatchSize: 1000,               // Batch size for GPU sync
}

// Create index with specific backend
index, err := gpu.NewIndexWithBackend(cfg, gpu.BackendCUDA)
if err != nil {
    log.Fatal(err)
}
defer index.Close()
```

### Using the Factory Pattern

```go
// Auto-detect best backend
index, err := gpu.NewIndex(cfg)
if err != nil {
    log.Fatal(err)
}

// Backend returns the actual backend being used
log.Printf("Using backend: %v", index.Backend())
```

### Adding Vectors

```go
// Prepare vectors and IDs
ids := []int64{1, 2, 3}
vectors := []float32{
    // Vector 1 (128 dimensions)
    0.1, 0.2, 0.3, /* ... 125 more values ... */
    
    // Vector 2
    0.4, 0.5, 0.6, /* ... 125 more values ... */
    
    // Vector 3
    0.7, 0.8, 0.9, /* ... 125 more values ... */
}

// Add to GPU index
err := index.Add(ids, vectors)
if err != nil {
    log.Fatal(err)
}
```

### Searching

```go
// Query vector (must match dimension)
query := []float32{0.1, 0.2, 0.3, /* ... 125 more values ... */}

// Search for 10 nearest neighbors
k := 10
ids, distances, err := index.Search(query, k)
if err != nil {
    log.Fatal(err)
}

// Process results
for i := 0; i < len(ids); i++ {
    log.Printf("ID: %d, Distance: %f", ids[i], distances[i])
}
```

### Hybrid Search with HNSW

```go
// Hybrid search combines GPU brute-force with CPU HNSW refinement
results, err := hnsw.SearchHybrid(ctx, query, k)
if err != nil {
    log.Fatal(err)
}

// Results are already sorted by distance
for _, result := range results {
    log.Printf("ID: %d, Score: %f", result.ID, result.Score)
}
```

### GPU Memory Management

```go
// Create memory pool for device 0
pool, err := gpu.NewGPUMemPool(gpu.BackendCUDA, 0)
if err != nil {
    log.Fatal(err)
}
defer pool.Close()

// Allocate GPU memory
size := int64(1024 * 1024 * 100) // 100MB
ptr, err := pool.AllocateGPU(size)
if err != nil {
    log.Fatal(err)
}
defer pool.FreeGPU(ptr)

// Copy data to GPU
hostData := make([]byte, size)
err = pool.MemcpyHostToDevice(unsafe.Pointer(&hostData[0]), ptr, size)
if err != nil {
    log.Fatal(err)
}

// Monitor memory usage
log.Printf("GPU Memory: %d/%d bytes used", 
    pool.GetUsedMemory(), 
    pool.GetTotalMemory())
```

## Configuration Options

### GPUConfig

```go
type GPUConfig struct {
    Backend       GPUBackend  // BackendCPU, BackendCUDA, BackendMetal
    DeviceID      int         // GPU device ID (0 for first GPU)
    Dimension     int         // Vector dimension (must match your data)
    Enabled       bool        // Enable GPU acceleration
    
    // CUDA-specific
    CUDAHome      string      // Path to CUDA installation
    FAISSHome     string      // Path to FAISS library
    
    // Metal-specific
    MetalUnifiedMemory bool   // Use unified memory on Apple Silicon
    
    // Synchronization
    SyncBatchSize int        // Batch size for GPU sync (default: 1000)
    SyncInterval  int        // Sync interval in seconds (default: 5)
}
```

### Environment Variables

```bash
# CUDA setup
export CUDA_HOME=/usr/local/cuda
export FAISS_HOME=/usr/local

# Force specific backend
export LONGBOW_GPU_BACKEND=cuda  # or metal, cpu

# Select GPU device
export LONGBOW_GPU_DEVICE=0
```

### Default Configuration

```go
// Get default configuration
cfg := gpu.DefaultGPUConfig()

// Modify as needed
cfg.Enabled = true
cfg.DeviceID = 1  // Use second GPU
```

## Performance Tuning

### Optimal Vector Dimensions

GPU acceleration is most effective for:

- **128-512 dimensions**: Good speedup (2-5x)
- **512-1536 dimensions**: Excellent speedup (5-15x)
- **1536+ dimensions**: Maximum speedup (10-50x)

### Batch Sizes

For best performance, batch multiple queries:

```go
// Bad: Individual queries
for _, query := range queries {
    index.Search(query, k)  // Overhead per query
}

// Good: Batch queries (if supported by backend)
// Use hybrid search or custom batching
```

### Memory Management

```go
// Monitor GPU memory
info, _ := index.GetMemoryInfo()
total, free, used, _ := index.GetMemoryInfo()
log.Printf("GPU Memory: %d used / %d total (%d free)", used, total, free)

// Adjust sync batch size based on available memory
cfg.SyncBatchSize = 5000  // Larger batches = less overhead
```

### Hybrid Search Tuning

```go
// Default configuration uses GPU for candidate generation
// CPU refines results with HNSW graph

// Increase candidate multiplier for better recall
// (more GPU work, better accuracy)
hnsw.SetHybridConfig(store.HybridSearchConfig{
    CandidateMultiplier: 20,  // Default: 10
    RefineTopK:         k,
})
```

## Examples

### Example 1: Basic GPU-Accelerated Search

```go
package main

import (
    "context"
    "log"
    "github.com/23skdu/longbow/internal/gpu"
    "github.com/23skdu/longbow/internal/store"
)

func main() {
    ctx := context.Background()
    
    // Create store
    vs, err := store.New("myindex", 128)
    if err != nil {
        log.Fatal(err)
    }
    defer vs.Close()
    
    // Add vectors
    for i := 0; i < 10000; i++ {
        vector := generateRandomVector(128)
        vs.Add(store.VectorID(i), vector)
    }
    
    // Enable GPU
    if hnsw, ok := vs.Index().(*store.ArrowHNSW); ok {
        if err := hnsw.InitGPU(0, log.Logger); err != nil {
            log.Printf("GPU not available: %v", err)
        }
    }
    
    // Search
    query := generateRandomVector(128)
    results, err := vs.Search(ctx, query, 10)
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Found %d results", len(results))
}
```

### Example 2: Multi-GPU Setup

```go
func setupMultiGPU() {
    // Get number of GPUs
count := gpu.GetDeviceCount()
    log.Printf("Found %d GPU devices", count)
    
    // Create index for each GPU
    indexes := make([]gpu.Index, count)
    for i := 0; i < count; i++ {
        cfg := gpu.GPUConfig{
            Backend:   gpu.BackendCUDA,
            DeviceID:  i,
            Dimension: 128,
            Enabled:   true,
        }
        
        idx, err := gpu.NewIndexWithBackend(cfg, gpu.BackendCUDA)
        if err != nil {
            log.Printf("Failed to init GPU %d: %v", i, err)
            continue
        }
        indexes[i] = idx
        
        // Get device info
        info, _ := idx.GetDeviceInfo()
        log.Printf("GPU %d: %s (%d MB)", i, info.Name, info.MemoryMB)
    }
}
```

### Example 3: Graceful Fallback

```go
func createIndexWithFallback(cfg gpu.GPUConfig) (gpu.Index, error) {
    // Try GPU first
    if cfg.Enabled {
        index, err := gpu.NewIndex(cfg)
        if err == nil {
            log.Printf("Using GPU backend: %v", index.Backend())
            return index, nil
        }
        log.Printf("GPU initialization failed: %v", err)
    }
    
    // Fall back to CPU
    log.Println("Falling back to CPU backend")
    cfg.Backend = gpu.BackendCPU
    return gpu.NewCPUIndex(cfg)
}
```

### Example 4: GPU Metrics

```go
import "github.com/23skdu/longbow/internal/metrics"

func monitorGPU() {
    // GPU search latency
    metrics.VectorSearchGPULatencySeconds.WithLabelValues("search").Observe(duration)
    
    // GPU operations count
    metrics.VectorSearchGPUOperationsTotal.WithLabelValues("search", "success").Inc()
    
    // GPU fallback count
    metrics.VectorSearchGPUFallbackTotal.Inc()
}
```

## Best Practices

1. **Always use defer for cleanup**:

   ```go
   index, err := gpu.NewIndex(cfg)
   if err != nil {
       return err
   }
   defer index.Close()  // Important!
   ```

2. **Handle GPU unavailability gracefully**:

   ```go
   err := hnsw.InitGPU(0, logger)
   if err != nil {
       log.Printf("GPU not available, continuing with CPU: %v", err)
   }
   ```

3. **Monitor GPU memory usage**:

   ```go
   total, free, used, _ := index.GetMemoryInfo()
   if float64(used)/float64(total) > 0.9 {
       log.Println("Warning: GPU memory usage > 90%")
   }
   ```

4. **Use appropriate vector dimensions**:
   - GPU acceleration works best with 128+ dimensions
   - Consider dimensionality reduction for lower-dimensional data

5. **Batch operations when possible**:
   - Synchronize GPU updates in batches
   - Avoid frequent small transfers

## See Also

- [GPU Setup Guide](gpu_setup.md) - Installation and configuration
- [API Reference](https://pkg.go.dev/github.com/23skdu/longbow) - Complete API documentation
- [Performance Tuning Guide](performance.md) - Optimization strategies
