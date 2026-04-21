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
  - **Dense**: HNSW (Hierarchical Navigable Small World) for approximate nearest neighbor search. Support for FP32, SQ8 (Scalar Quantization), and BQ (Binary Quantization).
  - **Sparse**: Inverted Index (BM25/Sparse) for keyword matching.
  - **Auto-Sharding**: Transparently upgrades standard indices to **ShardedHNSW** (lock-striped) when thresholds are met.
- **Adaptive Indexing**: Automated migration from flat linear scans to HNSW indexing based on collection size and growth acceleration. Worker-pool lifecycle is managed to ensure zero-downtime during background construction.
- **Interim Sharding**: Uses a temporary sharded index during migration to eliminate double-indexing overhead.
- **Zero-Copy**: Utilizes Apache Arrow for zero-copy data representation, minimizing serialization overhead.
- **Concurrency**: Concurrent HNSW with fine-grained locking per node/level to maximize throughput.
- **SIMD**: Optimized for modern CPU architectures with SIMD instructions (AVX2/AVX-512/NEON).

### 2. Storage Layer

- **WAL-Backed Engine**: Log-structured storage with an in-memory primary store and background record-batch compaction.
- **High-Throughput Snapshots**: Reflection-free Arrow-to-Parquet serialization using `parquet-go` for maximum disk throughput. Operates directly on `io.ReaderAt` compatible buffers.
- **Durability**: Periodic checkpoints and snapshots ensure data durability across node restarts.

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
                MPSSort["MPSSort"]
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

---

## Observability: OpenTelemetry Tracing

```mermaid
graph TB
    subgraph Client["Client"]
        Tracer["OpenTelemetry Tracer"]
    end

    subgraph TraceContext["W3C Trace Context"]
        Propagator["Trace Context Propagator"]
    end

    subgraph Node1["Node 1"]
        IngestSpan["DoPut Span"]
        SearchSpan["DoGet Search Span"]
    end

    subgraph Node2["Node 2"]
        GlobalSpan["Global Search Span"]
        RemoteSpan["Remote Query Span"]
    end

    subgraph Exporters["Exporters"]
        Jaeger["Jaeger"]
        Zipkin["Zipkin"]
        Tempo["Grafana Tempo"]
    end

    Client --> Tracer
    Tracer --> Propagator
    Propagator --> IngestSpan
    IngestSpan --> SearchSpan
    SearchSpan --> GlobalSpan
    GlobalSpan --> RemoteSpan
    RemoteSpan --> Jaeger
    RemoteSpan --> Zipkin
    RemoteSpan --> Tempo
```

### Traced Operations

| Operation | Span Attributes |
|-----------|-----------------|
| `DoPut` | dataset, batch_size, vector_dim |
| `DoGet` | dataset, k, filters, latency_ms |
| `GlobalSearch` | nodes_queried, results_merged |
| `HybridSearch` | bm25_score, vector_score, alpha |
| `Rerank` | candidates, model, top_k |

---

## Semantic Query Cache

```mermaid
graph LR
    Query["Query Vector"] --> Cache[Query Cache]
    Cache -->|Hit| Results["Cached Results"]
    Cache -->|Miss| Embed[Embedding Model]
    Embed --> Search[HNSW Search]
    Search --> Results
    Results -->|Cache| Cache
```

### Cache Features

- **LRU with TTL**: Configurable expiration per dataset
- **Similarity-Based Invalidation**: Invalidates on dataset mutations
- **Cache Warming**: Pre-loads frequent queries
- **Metrics**: Hit rate, latency improvement tracked

---

## Global Search (Scatter-Gather)

```mermaid
graph TB
    subgraph Coordinator["Global Search Coordinator"]
        Hedge["Replica Hedging"]
        Merge["Heap Merge (Top-K)"]
    end

    subgraph Nodes["Longbow Nodes"]
        Node1["Node 1"]
        Node2["Node 2"]
        Node3["Node 3"]
    end

    Query["Query"] --> Coordinator
    Coordinator --> Hedge
    Hedge -->|Parallel| Node1
    Hedge -->|Parallel| Node2
    Hedge -->|Parallel| Node3
    Node1 -->|Results| Merge
    Node2 -->|Results| Merge
    Node3 -->|Results| Merge
    Merge --> Final["Top-K Results"]
```

---

## Auto-Scaling

```mermaid
graph TB
    subgraph AutoScaler["AutoScaler"]
        QPS["Search QPS Monitor"]
        Latency["Latency Monitor"]
        Reconcile["Reconciler"]
    end

    subgraph Admission["Admission Controller"]
        Memory["Memory Tracker"]
        Backpressure["Backpressure Signals"]
    end

    subgraph Workers["Worker Pools"]
        IndexWorkers["Indexing Workers"]
        IngestWorkers["Ingestion Workers"]
    end

    QPS --> Reconcile
    Latency --> Reconcile
    Reconcile --> IndexWorkers
    Reconcile --> IngestWorkers
    Memory --> Backpressure
    Backpressure -->|Reject| Admission
```

### Scaling Triggers

| Metric | Scale Up | Scale Down |
|--------|----------|------------|
| Search QPS | > 80% capacity | < 20% capacity |
| Latency | p99 > 100ms | p99 < 10ms |
| Memory | > 90% used | < 50% used |

---

## Multi-Tenancy

```mermaid
graph TB
    subgraph Namespaces["Namespace Isolation"]
        NS1["Namespace A"]
        NS2["Namespace B"]
        NS3["Namespace C"]
    end

    subgraph Resources["Per-Namespace Resources"]
        Quota["Resource Quotas"]
        Cache["Tenant Cache"]
        Metrics["Per-Tenant Metrics"]
    end

    NS1 --> Quota
    NS2 --> Quota
    NS3 --> Quota
    NS1 --> Cache
    NS2 --> Cache
    NS1 --> Metrics
    NS2 --> Metrics
```

---

## Disk-Based Indexing (DiskANN)

```mermaid
graph TB
    subgraph Hot["Hot Tier (RAM)"]
        HNSW["HNSW Index"]
        MemVectors["Hot Vectors"]
    end

    subgraph Warm["Warm Tier (SSD)"]
        DiskANN["DiskANN Index"]
        DiskVectors["Warm Vectors"]
    end

    subgraph Cold["Cold Tier (Object Store)"]
        Parquet["Parquet Snapshots"]
        WAL["Archived WAL"]
    end

    Query["Query"] --> HNSW
    HNSW -->|Miss| DiskANN
    DiskVectors --> DiskANN
    DiskANN --> Parquet
```

---

## Rich Payload Filtering

```mermaid
graph TB
    subgraph Filters["Filter Types"]
        Numeric["Numeric (=, >, <, >=, <=)"]
        Keyword["Keyword (exact match)"]
        Boolean["Boolean"]
        Date["Date64/Timestamp (range, before, after)"]
        Composite["Composite (AND, OR, NOT)"]
    end

    subgraph Index["Bitmap Indexes"]
        Roaring["Roaring Bitmaps"]
        Selectivity["Selectivity Estimator"]
    end

    subgraph Pushdown["Filter Pushdown"]
        Compile["Compile to Bitmap"]
        Apply["Apply to Candidates"]
        Optimize["Index Hints"]
    end

    Filters --> Index
    Index --> Pushdown
    Pushdown --> HNSW[HNSW Search]
```

### Supported Field Types

| Type | Operators | Index Type |
|------|-----------|------------|
| `int64` | `=`, `>`, `<`, `>=`, `<=`, `range` | Roaring Bitmap |
| `float32` | `=`, `>`, `<`, `>=`, `<=` | Roaring Bitmap |
| `string` | `=`, `IN` | Hash Map |
| `bool` | `=` | Bitmap |
| `timestamp` | `>`, `<`, `range`, `before`, `after` | Roaring Bitmap |
| `date64` | `>`, `<`, `range`, `before`, `after` | Roaring Bitmap |

---

## Hybrid Search + Cross-Encoder Reranking

```mermaid
graph TB
    subgraph Query["Query Processing"]
        VecQuery["Vector Query"]
        TextQuery["Text Query (BM25)"]
    end

    subgraph Fusion["RRF Fusion"]
        BM25["BM25 Score"]
        Vector["Vector Score"]
        RRF["Reciprocal Rank Fusion"]
    end

    subgraph Rerank["Cross-Encoder Reranking"]
        Candidates["Top-K Candidates"]
        CrossEncoder["Cross-Encoder Model"]
        ReScore["Re-scored Results"]
    end

    VecQuery --> BM25
    TextQuery --> Vector
    BM25 --> RRF
    Vector --> RRF
    RRF --> Candidates
    Candidates --> CrossEncoder
    CrossEncoder --> ReScore
```
