# Longbow Systems Architecture

## Overview

Longbow is a distributed, high-performance vector database designed for low-latency retrieval and high-throughput ingestion. It leverages a hybrid storage engine, modern hardware optimizations, and a resilient distributed mesh.

## System Architecture Diagram

```mermaid
graph TB
    subgraph Client["Client Applications"]
        Python["Python SDK"]
        Go["Go SDK"]
        Flight["Arrow Flight (gRPC)"]
    end

    subgraph Longbow["Longbow Cluster"]
        subgraph Node1["Node 1"]
            FlightSrv["Flight Server:3000"]
            MetaSrv["Meta Server:3001"]
            Metrics["Metrics:9090"]
            VectorStore1["Vector Store"]
            HNSW1["HNSW Index"]
            WAL1["WAL"]
        end

        subgraph NodeN["Node N"]
            FlightSrvN["Flight Server:3000"]
            MetaSrvN["Meta Server:3001"]
            VectorStoreN["Vector Store"]
            HNSW_N["HNSW Index"]
            WALN["WAL"]
        end

        subgraph Mesh["Distributed Mesh"]
            Gossip["Gossip Protocol"]
            Ring["Consistent Hash Ring"]
        end

        subgraph Storage["Shared Storage"]
            Snapshots["Parquet Snapshots"]
            WALLog["WAL Logs"]
        end
    end

    Python --> Flight
    Go --> Flight
    Flight --> FlightSrv
    FlightSrv --> VectorStore1
    VectorStore1 --> HNSW1
    HNSW1 --> WAL1
    VectorStore1 --> Ring
    Ring <--> Gossip
    Gossip <--> MetaSrvN
    WAL1 --> WALLog
    WALLog --> Snapshots
```

## Core Components

### 1. Vector Engine

- **Hybrid Indexing**:
  - **Dense**: HNSW (Hierarchical Navigable Small World) for approximate nearest neighbor search.
  - **Sparse**: Inverted Index (BM25/Sparse) for keyword matching.
  - **Auto-Sharding**: Transparently upgrades standard indices to **ShardedHNSW** (lock-striped) when thresholds are met.
- **Interim Sharding**: Uses a temporary sharded index during migration to eliminate double-indexing overhead.
- **Zero-Copy**: Utilizes Apache Arrow for zero-copy data representation, minimizing serialization overhead.
- **Concurrency**: Concurrent HNSW with fine-grained locking per node/level to maximize throughput.
- **SIMD**: Optimized for modern CPU architectures with SIMD instructions (AVX2/AVX-512).

### 2. Storage Layer

- **WAL-Backed Engine**: Log-structured storage with an in-memory primary store and background record-batch compaction.
- **Durability**: Periodic checkpoints and snapshots ensure data durability.

---

## GPU Architecture: NVIDIA CUDA

```mermaid
graph TB
    subgraph Host["CPU Host"]
        FlightSrv["Flight Server"]
        VectorStore["Vector Store"]
        WAL["WAL"]
        CPUSearch["CPU Search Path"]
    end

    subgraph GPU["NVIDIA GPU (CUDA)"]
        GPUIndex["GPU HNSW Index"]
        GPUMem["GPU Memory Pool"]
        CUDA["CUDA Runtime"]
        cuBLAS["cuBLAS"]
        Memcpy["Memcpy H2D/D2H"]
        
        subgraph GPUCompute["GPU Compute"]
            Distance["Distance Kernels"]
            GraphTraverse["Graph Traversal"]
            TopK["Top-K Selection"]
        end
    end

    subgraph Memory["Memory Hierarchy"]
        HostMem["Host Memory"]
        VRAM["VRAM (HBM2/GDDR)"]
    end

    VectorStore --> GPUIndex
    GPUIndex --> GPUMem
    GPUMem --> CUDA
    CUDA --> cuBLAS
    CUDA --> GPUCompute
    
    VectorStore --> Memcpy
    Memcpy --> GPUIndex
    Memcpy --> VRAM
    VRAM --> HostMem
    
    CPUSearch --> VectorStore
```

### CUDA Data Flow

```mermaid
sequenceDiagram
    participant Client
    participant Flight
    participant VectorStore
    participant GPUMem
    participant GPUKernel
    
    Client->>Flight: DoGet (query vector)
    Flight->>VectorStore: Route to dataset
    
    Note over VectorStore,GPUMem: 1. Transfer query to GPU
    VectorStore->>GPUMem: MemcpyHostToDevice(query)
    
    Note over GPUMem,GPUKernel: 2. GPU Distance Calculation
    GPUMem->>GPUKernel: Launch distance kernel
    GPUKernel->>GPUKernel: Batch distance computation
    GPUKernel->>GPUMem: Store intermediate results
    
    Note over GPUMem,GPUKernel: 3. Graph Traversal
    GPUMem->>GPUKernel: HNSW traversal on GPU
    GPUKernel->>GPUMem: Collect candidates
    
    Note over GPUMem,VectorStore: 4. Top-K Reduction
    GPUMem->>GPUKernel: Reduce to top-k
    GPUKernel->>GPUMem: Final results
    
    Note over VectorStore,Client: 5. Transfer results back
    GPUMem->>VectorStore: MemcpyDeviceToHost(results)
    VectorStore->>Client: Flight RecordBatch
```

### CUDA Configuration

```bash
# Enable GPU support
GPU_ENABLED=true
GPU_DEVICE_ID=0

# Memory configuration
GPU_MEMORY_LIMIT=8589934592  # 8GB

# Hybrid search (GPU + CPU refinement)
HYBRID_SEARCH_ENABLED=true
HYBRID_ALPHA=0.5  # Balance GPU/CPU work
```

### CUDA Memory Layout

```
┌─────────────────────────────────────────┐
│           GPU Memory (VRAM)             │
├─────────────────────────────────────────┤
│  ┌─────────────────────────────────────┐│
│  │  GPU HNSW Graph                     ││
│  │  - Node positions (float32)         ││
│  │  - Edge lists (int32)               ││
│  │  - Level offsets                    ││
│  └─────────────────────────────────────┘│
│  ┌─────────────────────────────────────┐│
│  │  Distance Buffers                   ││
│  │  - Query vector (shared)            ││
│  │  - Batch distances                  ││
│  │  - Top-K heap                       ││
│  └─────────────────────────────────────┘│
│  ┌─────────────────────────────────────┐│
│  │  CUDA Events/Synchronization        ││
│  └─────────────────────────────────────┘│
└─────────────────────────────────────────┘
```

---

## GPU Architecture: Apple Metal

```mermaid
graph TB
    subgraph Mac["macOS System"]
        subgraph CPU["CPU Layer"]
            FlightSrv["Flight Server"]
            VectorStore["Vector Store"]
            WAL["WAL"]
        end

        subgraph Unified["Unified Memory Architecture"]
            CPUAlloc["CPU Allocations"]
            GPUAlloc["GPU Allocations"]
            SharedMem["Shared System Memory"]
        end

        subgraph GPU["Apple GPU (Metal)"]
            MetalCmd["Metal Command Buffer"]
            MetalLib["Metal Shaders"]
            
            subgraph MPS["Metal Performance Shaders"]
                MPSDistance["MPSDistance"]
                MPSReduce["MPSReduce"]
                MPSSort["MPSSort"
            end
            
            subgraph GPUCompute["GPU Kernels"]
                SIMD["SIMD Grid"]
                Threadgroups["Threadgroups"]
            end
        end
    end

    FlightSrv --> VectorStore
    VectorStore --> GPUAlloc
    GPUAlloc --> MetalCmd
    MetalCmd --> MetalLib
    MetalLib --> MPS
    MPS --> GPUCompute
```

### Metal Data Flow

```mermaid
sequenceDiagram
    participant Client
    participant Flight
    participant VectorStore
    participant MetalCmd
    participant MPS
    
    Client->>Flight: DoGet (query vector)
    Flight->>VectorStore: Route to dataset
    
    Note over VectorStore,MetalCmd: 1. Create Metal Command Buffer
    VectorStore->>MetalCmd: MTLCommandBufferCreate()
    
    Note over MetalCmd,MPS: 2. Encode Distance Kernel
    MetalCmd->>MPS: encodeDistancekernel()
    
    Note over MPS,MetalCmd: 3. Execute on GPU
    MPS->>MetalCmd: MPSDistanceCompute()
    
    Note over MetalCmd,MPS: 4. Top-K Reduction
    MetalCmd->>MPS: encodeTopKReduction()
    
    Note over VectorStore,Client: 5. Readback Results
    MetalCmd->>VectorStore: waitUntilCompleted()
    VectorStore->>Client: Flight RecordBatch
```

### Metal Optimization Features

```mermaid
graph LR
    subgraph UnifiedMemory["Unified Memory"]
        A[CPU Access] -->|Zero Copy| U[Shared Buffer]
        G[GPU Access] -->|Zero Copy| U
    end
    
    subgraph Shaders["Metal Shaders"]
        SIMD[SIMD Operations]
        Vector[Vector Math]
        Atomic[Atomic Operations]
    end
    
    subgraph MPS["Metal Performance Shaders"]
        Distance["MPSVectorDistance"]
        Reduce["MPSReduce"]
        Histogram["MPSHistogram"]
    end
    
    U --> SIMD
    U --> Vector
    MPS --> Distance
    MPS --> Reduce
```

### Metal Configuration

```bash
# Metal is auto-detected on Apple Silicon
GPU_ENABLED=true
GPU_DEVICE_ID=0

# Metal-specific optimizations
# - Unified memory eliminates explicit copies
# - MPS provides optimized kernels
# - Compute shaders for custom operations
```

### Metal Memory Model

```
┌─────────────────────────────────────────┐
│       Unified Memory Architecture       │
├─────────────────────────────────────────┤
│  ┌─────────────────────────────────────┐│
│  │  Shared Buffer (CPU/GPU accessible) ││
│  │  - No explicit memcpy needed        ││
│  │  - Memory coherence automatic       ││
│  └─────────────────────────────────────┘│
│  ┌─────────────────────────────────────┐│
│  │  Metal Resource Buffers             ││
│  │  - MTLBuffer with storage mode      ││
│  │  - Shared: CPU + GPU access         ││
│  │  - Private: GPU only                ││
│  └─────────────────────────────────────┘│
└─────────────────────────────────────────┘
```

---

## Data Flow

### Ingestion (DoPut)

```mermaid
graph LR
    Client --> FlightSrv
    FlightSrv --> Proxy
    Proxy --> Hash[Consistent Hash]
    Hash --> Local[Local Node]
    Hash --> Remote[Remote Node]
    Local --> WAL[WAL]
    Local --> MemTable[MemTable]
    WAL --> Compactor[Compactor]
    MemTable --> Compactor
    Compactor --> Snapshot[Snapshot]
```

### Retrieval (DoGet)

```mermaid
graph LR
    Client --> FlightSrv
    FlightSrv --> SmartClient
    SmartClient --> Coordinator
    Coordinator --> Filter[Filter Evaluation]
    Filter --> HNSW[HNSW Search]
    HNSW --> Candidates[Candidate Set]
    Candidates --> Rerank[Re-ranking]
    Rerank --> Results[Results]
    Results --> Client
```

---

## GPU Selection Matrix

| Feature | CPU | NVIDIA CUDA | Apple Metal |
|---------|-----|-------------|-------------|
| **HNSW Search** | AVX2/AVX-512 | cuBLAS + Custom | MPS + Shaders |
| **Distance Metrics** | SIMD kernels | GPU kernels | MPSDistance |
| **Memory Model** | System RAM | VRAM (separate) | Unified Memory |
| **Hybrid Search** | N/A | GPU + CPU fallback | GPU + CPU fallback |
| **Unified Memory** | N/A | requires explicit copy | Automatic |
| **Tensor Cores** | No | FP16/INT8 support | No (Apple Silicon) |
| **Max Dimensions** | 3072 | 4096+ | 3072 |

---

## Configuration

See [Configuration Guide](configuration.md) for details on:

- `LONGBOW_STORAGE_USE_IOURING`
- `LONGBOW_GOSSIP_ENABLED`
- `LONGBOW_GPU_ENABLED`
- `LONGBOW_HYBRID_SEARCH_ENABLED`
