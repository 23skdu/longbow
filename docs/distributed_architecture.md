# Longbow Distributed Architecture

**Version**: 0.2.0-dev
**Status**: Implemented (Phases 7-9)

## Overview

Longbow transitions from a single-node vector store to a distributed, horizontally scalable system
using a "Dynamo-style" architecture. This document details the core components implemented during
Phases 7, 8, and 9.

## Core Components

### 1. Consistent Hashing Ring

- **Logic**: Implemented in `internal/sharding/ring.go`.
- **Mechanism**:
  - Nodes are placed on a hash ring using SHA-256 of their node ID.
  - Each node is represented by `20` virtual nodes (vnodes) to ensure uniform data distribution.
- **Key Assignment**: Keys are hashed and assigned to the first node encountered moving clockwise on the ring.

### 2. Cluster Membership (Gossip)

- **Protocol**: SWIM (Scalable Weakly-consistent Infection-style Process Group Membership).
- **Implementation**: `internal/mesh/gossip.go`.
- **Integration**:
  - The `RingManager` (`internal/sharding/manager.go`) subscribes to gossip events via the `EventDelegate` interface.
  - **Join**: New node added to Ring.
  - **Leave/Fail**: Node removed from Ring; keys automatically rebalanced.

### 3. Partition Proxy (Request Routing)

- **Component**: `PartitionProxyInterceptor` (`internal/sharding/proxy.go`).
- **Function**: Intercepts every incoming gRPC unary request.
- **Routing Decision**:
    1. Extracts routing key (`x-longbow-key` metadata).
    2. Queries `RingManager` for ownership.
    3. **Local**: Pass to handler.
    4. **Remote**: Forward to correct node.

### 4. Smart Request Forwarding

- **Component**: `RequestForwarder` (`internal/sharding/forwarder.go`).
- **Behavior**:
  - Maintains a connection pool to all nodes in the ring.
  - Resolves Node ID -> Network Address using `RingManager`.
  - **Transparent Proxying**: The server automatically forwards requests to the correct owner node
    via gRPC interceptors.
  - **Smart Client Protocol**: Returns `FORWARD_REQUIRED` error with target address hint for clients
    that want to minimize latency by connecting directly to the owner.

## Data Flow

1. **Client** sends request `Put(Key="A")` to Node 1.
2. **Node 1 Proxy** hashes "A" -> Owner is Node 2.
3. **Node 1** transparently forwards the request to Node 2 and returns the result to the client.
4. **Alternative**: If strict client-side routing is preferred, Node 1 can return `FORWARD_REQUIRED: target=Node2 addr=10.0.0.2:3000`.
5. **Node 2** accepts request and writes to WAL.

## Replication Strategy

- **Logic**: `GetPreferenceList(key, n)` returns `N` distinct nodes walking the ring.
- **Consistency**: Future implementations will support Tunable Consistency (W+R > N). Currently, strict ownership is enforced.

## Hardware Acceleration (Research)

- **GPU**: Scaffolding in `internal/gpu` supports optional CUDA builds for HNSW offloading.
- **Linux I/O**: `io_uring` prototype verified for high-throughput WAL writes.
