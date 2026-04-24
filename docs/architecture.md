# Longbow Systems Architecture

Longbow is a distributed, high-performance vector database designed for low-latency retrieval and high-throughput ingestion. It leverages a hybrid storage engine, modern hardware optimizations, and a resilient distributed mesh.

---

## 1. System Overview

Longbow follows a "Dynamo-style" decentralized architecture where nodes coordinate via a gossip protocol and data is partitioned using consistent hashing.

### High-Level Architecture Diagram

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

---

## 2. Core Components

### Flight Servers
Longbow separates data and control traffic to prevent head-of-line blocking:
- **Data Server (Port 3000)**: Implements Arrow Flight for `DoPut` (ingestion) and `DoGet` (search).
- **Meta Server (Port 3001)**: Handles `ListFlights` and `DoAction` for cluster management.

### In-Memory Vector Store
- **SlabArena**: Off-heap memory management using 1MB slabs to eliminate Go GC overhead.
- **Auto-Sharding Index**: Dynamically transitions from a flat index to a lock-striped **ShardedHNSW** index as datasets grow.
- **Leveled Compaction**: Incremental merging of Arrow RecordBatches to maintain read performance without full index rebuilds.

### Distance & SIMD Engine
Vector distance calculations are optimized for modern CPU architectures:
- **AVX2/AVX-512**: For x86_64 systems.
- **NEON**: For ARM64 systems.
- **TurboQuant**: SIMD-accelerated bit-packing for extreme throughput.

---

## 3. Storage & Persistence

### Write-Ahead Log (WAL)
Every mutation is logged to a CRC32-protected WAL.
- **Double-Buffering**: Uses a swap-buffer strategy for zero-allocation logging.
- **Async Flush**: Ingestion continues while the WAL is periodically synced to disk.

### Snapshotting
Full index states are persisted as Parquet files. Snapshotting is triggered by WAL size limits or time intervals, and utilizes zero-copy Arrow-to-Parquet conversion.

---

## 4. Hardware Acceleration

### NVIDIA CUDA Architecture
Supports GPU-accelerated HNSW traversal and distance kernels.

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

    VectorStore --> GPUIndex
    GPUIndex --> GPUMem
    GPUMem --> CUDA
    CUDA --> cuBLAS
    CUDA --> GPUCompute
    
    VectorStore --> Memcpy
    Memcpy --> GPUIndex
```

### Apple Metal Architecture
Leverages Unified Memory on Apple Silicon for zero-copy CPU/GPU sharing.

```mermaid
graph TB
    subgraph Mac["macOS System"]
        subgraph CPU["CPU Layer"]
            FlightSrv["Flight Server"]
            VectorStore["Vector Store"]
        end

        subgraph Unified["Unified Memory Architecture"]
            SharedMem["Shared System Memory"]
        end

        subgraph GPU["Apple GPU (Metal)"]
            MetalCmd["Metal Command Buffer"]
            MPS["Metal Performance Shaders"]
        end
    end

    FlightSrv --> VectorStore
    VectorStore --> SharedMem
    SharedMem --> MetalCmd
    MetalCmd --> MPS
```

### Google TPU Architecture
Utilizes the HBM (High Bandwidth Memory) and VMEM scratchpad for massive vector operations.
- **Backend**: `BackendTPU` (Ironwood).
- **Optimization**: Optimized for petabyte-scale retrieval in GCP environments.

---

## 5. Advanced Search Features

### Hybrid Search & Reranking
Combines dense HNSW search with sparse BM25 indexing using Reciprocal Rank Fusion (RRF).

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

### Geospatial & Temporal Search
- **Quadtree Indexing**: For efficient spatial range and radius queries.
- **Temporal Versioning**: Maintains historical versions of vectors with TTL-based retention.
