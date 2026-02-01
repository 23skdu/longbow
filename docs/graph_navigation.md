# Graph Navigation API

Longbow provides an advanced Graph Navigation and Pathfinding engine optimized for large-scale HNSW graphs. This allows for multi-hop graph traversals beyond simple nearest-neighbor search.

## Overview

The Graph Navigation engine supports:

- **Multi-strategy Pathfinding**: Automatically selects between BFS, A*, and Parallel BFS based on query characteristics.
- **Concurrent Execution**: Optimized for high-throughput multi-core systems using lock-free data access and parallel worker pools.
- **Persistent Caching**: Zero-copy caching of navigation results with version-based invalidation.
- **Dequantization-Aware**: Seamlessly works with quantized vectors (SQ8, BQ, F16) for distance-based pruning.

## API Usage

### Finding a Path

```go
h := store.NewArrowHNSW(dataset, config)
// ... initialize and build ...

ctx := context.Background()
query := store.NavigatorQuery{
    StartID:  0,
    TargetID: 100,
    MaxHops:  10,
}

path, err := h.Navigate(ctx, query)
if err != nil {
    log.Fatal(err)
}

if path.Found {
    fmt.Printf("Path: %v (Hops: %d)\n", path.Path, path.Hops)
}
```

### Navigator Configuration

The `NavigatorConfig` allows tuning the behavior of the navigation engine:

| Option | Description | Default |
|--------|-------------|---------|
| `MaxHops` | Hard limit on the number of hops in a traversal. | 10 |
| `Concurrency` | Number of workers for Parallel BFS strategy. | 8 |
| `EarlyTerminate` | Enable distance-based pruning for sparse graphs. | true |
| `DistanceThreshold` | Prune paths where next-hop distance exceeds this value. | 0 (disabled) |
| `MaxNodesVisited` | Hard limit on the total number of unique nodes explored. | 0 (unlimited) |
| `EnableCaching` | Enable version-aware navigation result caching. | false |

## Persistence & Versioning

Longbow's navigation engine is fully integrated with its Arrow-based persistence layer.

- **Version Awareness**: Every `GraphData` modification increments a `GlobalVersion`. The navigator's cache is automatically invalidated when the version changes.
- **Zero-Copy**: Navigation works directly on memory-mapped Arrow buffers.

## Observability

Comprehensive Prometheus metrics are exposed under the `longbow_graph_navigation_` namespace:

- `longbow_graph_navigation_operations_total`: Count of operations by strategy and result.
- `longbow_graph_navigation_latency_seconds`: Execution time histogram.
- `longbow_graph_navigation_hops_total`: Distribution of path lengths.
- `longbow_graph_navigation_nodes_visited_total`: Exploration breadth tracking.
- `longbow_graph_navigation_strategy_selection_total`: Query planner decision tracking.

## Performance Tuning

1. **Strategy Selection**: The `QueryPlanner` automatically picks `AStar` for directed searches (start != target) and `ParallelBFS` for large undirected explorations (MaxHops > 4).
2. **Parallel Scaling**: For dense graphs, increasing `Concurrency` in `ParallelBFS` can significantly reduce latency at the cost of higher CPU usage.
3. **Caching**: Enable `EnableCaching` for workloads with frequent repeat queries on slowly changing graphs.
