# High Memory Usage Troubleshooting Runbook

This runbook provides step-by-step guidance for diagnosing and resolving high memory usage issues in Longbow production deployments.

## Table of Contents

1. [Immediate Actions](#immediate-actions)
2. [Diagnosis Steps](#diagnosis-steps)
3. [Common Scenarios](#common-scenarios)
4. [Memory Architecture](#memory-architecture)
5. [Long-Term Prevention](#long-term-prevention)

---

## Immediate Actions

### 1. Verify Memory Pressure is Real

```bash
# Check current RSS and compare to container limit
kubectl top <pod-name> -c "echo 'Memory Usage:'"
```

**Expected:** RSS should be close to memory limit if pressure is real.
**False Positive:** High RSS due to memory-mapped files (check with `pmap`).

### 2. Check for Leaky Datasets

```bash
# List all datasets and their memory usage
curl -s localhost:8080/metrics | grep -E "longbow_dataset_memory_bytes|longbow_total_datasets"
```

**Action:** Identify datasets with unusually high memory (>2x average).

### 3. Check Goroutine Count

```bash
curl -s localhost:8080/metrics | grep "go_goroutines"
```

**Expected:** 50-200 goroutines for normal operation.
**Warning:** >500 goroutines or constant upward trend.

### 4. Review Recent Deployment Changes

```bash
# Check for recent deployments that might have changed indexing or compaction settings
kubectl rollout history
```

---

## Diagnosis Steps

### Step 1: Identify Memory Hotspots

**Memory Profiling:**
```bash
# Collect 30-second CPU and memory profile
curl -s localhost:8080/debug/pprof/profile?seconds=30 -o mem.pprof

# Analyze locally
go tool pprof -http :8080/ localhost:8080/debug/pprof/profile -top
```

**Key Metrics to Review:**
- `SlabArena` allocations (should be majority of heap)
- Vector storage overhead
- HNSW graph size (nodes, edges)

**Red Flags:**
- >10GB in Arrow record batches
- >5GB in single HNSW graph
- Goroutine count increasing linearly

### Step 2: Check Compaction Status

```bash
# Check compaction metrics
curl -s localhost:8080/metrics | grep -E "compaction_duration_seconds|compaction_operations_total"
```

**Indicators:**
- High compaction duration → fragmentation issue
- Low compaction events → compaction not running
- Fragmentation ratio >30% → compaction needed

### Step 3: Verify GC Behavior

```bash
# Check GC stats
curl -s localhost:8080/metrics | grep -E "go_gc_duration_seconds|go_goroutines"
```

**Analysis:**
- Frequent long GC pauses (>100ms) → memory pressure
- High GOGC setting → may be too aggressive
- Ineffective GC → may indicate arena retention issue

### Step 4: Examine HNSW Graph Health

```bash
# Check HNSW-specific metrics
curl -s localhost:8080/metrics | grep -E "hnsw_|graph_"
```

**Look for:**
- High deleted node count (>10% of total)
- Large number of layers (>10)
- Memory per node significantly above expected (should be ~40-80 bytes for 128D vectors)

---

## Common Scenarios

### Scenario 1: Memory Usage Keeps Growing

**Symptoms:**
- RSS increases continuously over hours
- No proportional increase in data size
- Restart reduces memory temporarily

**Diagnosis Steps:**
1. Check for goroutine leaks (see Goroutine Leaks scenario)
2. Verify compaction is running
3. Check if arena memory is being released

**Solutions:**
```yaml
# Enable aggressive compaction
compaction_enabled: true
compaction_threshold: 0.3  # Compact when >30% fragmentation

# Increase GC aggressiveness
GOGC_aggressive: 10  # Trigger GC more frequently

# Check for stuck HNSW graphs
curl -X POST localhost:8080/admin/vacuum
```

### Scenario 2: Memory Spikes During Indexing

**Symptoms:**
- Memory usage jumps significantly during batch imports
- Takes hours to return to baseline
- Compaction doesn't reclaim memory

**Diagnosis Steps:**
1. Review batch size and dimensions
2. Check if pre-fetching is allocating too much
3. Verify SlabArena is releasing unused memory

**Solutions:**
```yaml
# Reduce batch size temporarily
import_batch_size: 500  # Instead of 4096

# Enable incremental compaction
compaction_interval: 30s  # Compact more frequently during import

# Check for HNSW graph over-allocations
hnsw_max_connections: 32  # Limit neighbor connections
```

### Scenario 3: High RSS But Low Memory Usage

**Symptoms:**
- RSS is high (>80% of limit)
- Go heap usage is much lower (e.g., 40GB RSS, 20GB heap)
- Vector count doesn't justify memory usage

**Diagnosis Steps:**
1. Check for memory-mapped file descriptors
2. Verify Arrow RecordBatches are properly released
3. Check for slab fragmentation

**Solutions:**
```bash
# Check for memory-mapped files
ls -la /proc/<pid>/fd | grep "mem"

# Force record batch release
curl -X POST localhost:8080/admin/compact

# Check slab pool metrics
curl -s localhost:8080/metrics | grep "slab_pool"
```

### Scenario 4: Goroutine Leaks

**Symptoms:**
- Goroutine count increases linearly over time
- High `go_goroutines` metric (>500)
- Some goroutines never complete

**Diagnosis Steps:**
```bash
# Get goroutine dump
curl localhost:8080/debug/pprof/goroutine?debug=2

# Look for long-running goroutines
grep -E "insert|index|repair" goroutine dump.txt
```

**Solutions:**
```yaml
# Check compaction worker status
curl -X POST localhost:8080/admin/compaction/stop

# Restart stuck workers
kubectl rollout restart <deployment>

# Enable goroutine leak detection tests
curl -X POST localhost:8080/admin/test/leak
```

### Scenario 5: Failed Compaction

**Symptoms:**
- Compaction runs but doesn't reclaim memory
- Memory continues to grow
- High error rate in compaction logs

**Diagnosis Steps:**
1. Check compaction worker logs
2. Verify arena compaction is triggered
3. Check HNSW graph compaction status

**Solutions:**
```bash
# Check compaction metrics
curl -s localhost:8080/metrics | grep compaction_events_total

# Force manual compaction
curl -X POST localhost:8080/admin/compact

# Check for stuck compaction
kubectl logs <pod-name> --tail=100 | grep compaction
```

---

## Memory Architecture

### SlabArena Memory Management

**How it Works:**
1. Slabs are allocated from OS in fixed-size chunks (1MB default, configurable)
2. Allocations are appended sequentially to current slab
3. When slab is full, new slab is allocated
4. Memory is released back to OS via `madvise(MADV_DONTNEED)` (Linux) or `VirtualFree(MEM_DECOMMIT)` (Windows)

**When Memory is Not Released:**
- Slabs are fragmented (lots of small allocations mixed with large ones)
- Live data prevents slab from being fully released
- Global references prevent release

**Compaction Strategy:**
- `TypedArena.Compact()` identifies live data and copies to new arena
- HNSW `CompactGraph()` rebuilds graph with tighter memory layout
- Release old arenas after compaction completes

### Memory Pressure Indicators

| Metric | Normal | Warning | Critical |
|--------|--------|--------|----------|
| Heap Usage % | <60% | 60-80% | >80% |
| GC Pause Frequency | <10/min | 10-30/min | >30/min |
| Slab Fragmentation | <20% | 20-50% | >50% |
| Goroutine Count | <200 | 200-500 | >500 |

---

## Long-Term Prevention

### 1. Regular Monitoring

Set up alerts for:
- Memory usage >80% of limit (with 5m lookback)
- Goroutine count >500
- Compaction error rate >10%
- GC pause duration >500ms

### 2. Scheduled Maintenance

Daily checks:
- Verify compaction is running
- Check for dataset leaks
- Review memory growth trends

Weekly tasks:
- Analyze memory profiles for new hotspots
- Review and tune compaction thresholds
- Audit goroutine usage patterns

### 3. Capacity Planning

Based on workload type:
- **High-dimensional vectors (1536D, 3072D)**: Allocate 2-3x memory
- **High QPS workload**: Use smaller batches, more frequent compaction
- **Large datasets**: Enable partitioning and distributed search

### 4. Testing Changes

Before deploying:
- Run memory soak tests with production-like workload
- Test with `GOGC=10` (aggressive) and verify heap stabilizes
- Verify compaction reclaims expected 20-30% memory
- Confirm goroutine count returns to baseline after test

---

## Escalation Path

### When to Escalate

**Immediate Escalation (<30 min):**
- Memory usage >95% for 10+ minutes
- Pod killed by OOMKilled
- Unexplained memory growth >5GB/hour
- Goroutine count >1000

**Standard Escalation (<2 hours):**
- Memory usage consistently >80% for 2+ hours
- Cannot resolve with this runbook
- Unusual error patterns in logs

**Emergency (immediate):**
- Complete OOMKilled cascade across replicas
- Unbounded memory growth (no cap)
- Security incident related to memory

### Information to Collect When Escalating

1. **Environment:**
   - Kubernetes version
   - Node type and resources
   - Container memory limit
   - Number of replicas

2. **Metrics:**
   - Current RSS and heap usage
   - Goroutine count
   - Top 5 largest datasets by memory usage
   - Recent compaction events and errors
   - GC statistics (pauses, duration)

3. **Recent Changes:**
   - Deployments in last 24 hours
   - Configuration changes
   - Schema evolution events

4. **Logs:**
   - Recent error logs (last 100 lines from longbow)
   - Suspicious patterns (repeated errors)
   - Compaction worker logs

### First Response Actions

1. **Immediate Mitigation:**
   ```bash
   # Scale down or stop ingestion temporarily
   kubectl scale deployment --replicas=0
   
   # Or increase memory limit if possible
   kubectl patch deployment -p '{"spec":{"template":{"spec":{"containers":[{"name":"longbow","resources":{"limits":{"memory":"<new-limit>"}}}}}}'
   ```

2. **Force Compaction:**
   ```bash
   # Trigger manual compaction for all datasets
   curl -X POST localhost:8080/admin/compact-all
   
   # Or compact specific large dataset
   curl -X POST localhost:8080/admin/compact -H <large-dataset>
   ```

3. **Enable Debug Mode:**
   ```yaml
   # Increase logging temporarily
   log_level: debug
   
   # Enable detailed metrics
   enable_profiling: true
   ```

### Recovery Verification

After implementing fixes:
1. Monitor for 1-2 hours
2. Verify RSS is stable or decreasing
3. Check goroutine count is back to baseline
4. Confirm compaction is reducing memory

---

## Additional Resources

- [Memory Architecture](../internal/memory/README.md) - Detailed technical documentation
- [Metrics Dashboard](/) - Current memory and performance metrics
- [Compaction Monitoring](/) - Compaction status and history
- [Alerting Guide](/docs/runbooks/alerting.md) - Alert configuration and thresholds

---

## Related Documentation

- [High Memory Usage Diagnosis](../../troubleshooting/high_memory_usage.md)
- [Goroutine Leak Detection](../../troubleshooting/goroutine_leaks.md)
- [Compaction Guide](../../troubleshooting/compaction_guide.md)
- [Configuration Reference](../../configuration/README.md)
