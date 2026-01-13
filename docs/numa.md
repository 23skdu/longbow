# NUMA Architecture Support

Longbow includes native support for Non-Uniform Memory Access (NUMA) architectures.

> [!CAUTION]
> **Status**: Experimental. While topology detection and pinning logic are implemented in
> `internal/store/numa_pin_linux.go`, they are not currently integrated into the standard server startup path.

## Overview

On multi-socket systems, accessing memory attached to a different CPU socket (remote access) is
significantly slower than accessing local memory. Longbow mitigates this by:

1. **Topology Detection**: Automatically detecting the NUMA layout (nodes and CPUs) at startup.
2. **Worker Pinning**: Binding indexing worker goroutines to specific CPU cores.
3. **Local Allocation**: Ensuring memory used by workers is allocated on the same NUMA node.

## How It Works

### 1. Topology Detection

Longbow inspects the `/sys/devices/system/node` directory on Linux to determine the number of NUMA nodes
and the CPU cores associated with each.

- **Linux**: Full detection supported.
- **macOS/Windows**: Stubs return a single-node topology.

### 2. Worker Pinning

When `VectorStore` initializes, it spawns a pool of indexing workers. If multiple NUMA nodes are detected:

- The worker pool is partitioned across nodes.
- Each worker is locked to an OS thread (`runtime.LockOSThread`).
- The thread is pinned to the CPUSet of a specific NUMA node using `sched_setaffinity`.

### 3. Memory Allocation

Longbow uses a custom `NUMAAllocator` (wrapping the Go allocator or C-based allocator if extended)
to hint preference for node-local memory. Since Go's memory allocator is generally thread-local
cache aware, pinning the goroutine implicitly improves allocation locality.

## Configuration & Verification

NUMA support is currently **manual/opt-in** by calling pinning functions in custom server distributions.
It is not yet active in the default `cmd/longbow/main.go`.

### verifying Activation

Check the startup logs for the following lines:

```text
INFO: NUMA topology detected nodes=2 topology="2 NUMA nodes: Node 0: CPUs [0-15], Node 1: CPUs [16-31]"
INFO: Started NUMA indexing workers count=32 nodes=2
```

If these lines are absent or show `nodes=1`, NUMA support is inactive or the system is detected as
single-socket.

### Metrics

Monitor the distribution of workers:

- `longbow_numa_worker_distribution{node="0"}`
- `longbow_numa_worker_distribution{node="1"}`

## Troubleshooting

- **"Failed to pin to NUMA node"**: Ensure the user running the process has permissions to set thread
  affinity (usually standard user permissions are sufficient, but constrained environments like
  containers might restrict this).
- **Docker/Kubernetes**: Ensure the container has access to the host's topology or is allocated
  exclusive CPUs (`static` policy in K8s) to benefit from NUMA pinning.
