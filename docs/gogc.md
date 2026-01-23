# GOGC Auto-Tuning

Longbow includes a dynamic Garbage Collection (GC) tuner to optimize CPU usage and memory utilization.

## Overview

The Go runtime's default GC policy (`GOGC=100`) triggers a collection when the heap grows by 100% relative
to the live heap size. This works well generally but can leave a significant amount of memory unused on
systems with large RAM, or cause Out-Of-Memory (OOM) errors if the heap spikes.

Longbow's **GCTuner** dynamically adjusts `GOGC` based on the current heap usage relative to
a configured **soft memory limit**.

- **Low Memory Usage**: Increases `GOGC` (e.g., to 100-500). This reduces GC frequency, saving CPU
  cycles for application logic.
- **High Memory Usage**: Decreases `GOGC` (e.g., to 10-50). This forces more frequent GCs to reclaim
  memory aggressively and prevent OOMs.

This feature leverages Go 1.19+'s `debug.SetMemoryLimit` as a hard safety net and `debug.SetGCPercent` for tuning.

## Configuration

The tuner is configured programmatically (or via flags in the main entry point):

| Parameter | Description | Default |
| :--- | :--- | :--- |
| `MemoryLimit` | Total memory available (bytes). Should be set to ~90% of container limit. | `0` (Disabled) |
| `HighGOGC` | Target GOGC when utilization is < 50%. | `100` |
| `LowGOGC` | Target GOGC when utilization is > 90%. | `10` |

### Tuning Logic

The tuner monitors `runtime.MemStats` periodically:

1. **Usage Ratio** = `HeapInuse` / `MemoryLimit`
2. **Target GOGC**:
    - If Ratio < 50%: `HighGOGC`
    - If Ratio > 90%: `LowGOGC`
    - Otherwise: Linearly interpolated between `HighGOGC` and `LowGOGC`.

## Metrics

The tuner exposes the following Prometheus metrics:

| Metric | Type | Description |
| :--- | :--- | :--- |
| `longbow_gc_tuner_target_gogc` | Gauge | The current GOGC value set by the tuner. |
| `longbow_gc_tuner_heap_utilization` | Gauge | Current heap utilization ratio (0.0 - 1.0). |

## Usage

In your main application setup (`cmd/longbow/main.go`):

```go
import (
    lbmem "github.com/23skdu/longbow/internal/memory"
)

func run() error {
    // ... setup ...

    // Initialize GC Tuner (e.g., 4GB limit from config)
    tuner := lbmem.NewGCTuner(cfg.MaxMemory, cfg.GOGC, 10, &logger)
    
    // Start background monitoring
    go tuner.Start(ctx, 2*time.Second)

    // ... run app ...
}
```
