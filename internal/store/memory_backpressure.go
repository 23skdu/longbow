package store

import (
"context"
"runtime"
"sync"
"sync/atomic"
"time"

"github.com/23skdu/longbow/internal/metrics"
)

// PressureLevel indicates the current memory pressure state.
type PressureLevel int32

const (
PressureNone PressureLevel = iota
PressureSoft
PressureHard
)

// BackpressureConfig configures memory backpressure behavior.
type BackpressureConfig struct {
SoftLimitBytes    uint64
HardLimitBytes    uint64
CheckInterval     time.Duration
SoftPressureDelay time.Duration
}

// MemoryBackpressureController manages memory backpressure for the store.
type MemoryBackpressureController struct {
config        BackpressureConfig
pressureLevel atomic.Int32
acquireCount  atomic.Uint64
releaseCount  atomic.Uint64
rejectCount   atomic.Uint64
mu            sync.Mutex
cond          *sync.Cond
}

// NewMemoryBackpressureController creates a new backpressure controller.
func NewMemoryBackpressureController(cfg BackpressureConfig) *MemoryBackpressureController {
ctrl := &MemoryBackpressureController{
config: cfg,
}
ctrl.cond = sync.NewCond(&ctrl.mu)
return ctrl
}

// GetSoftLimit returns the configured soft limit.
func (c *MemoryBackpressureController) GetSoftLimit() uint64 {
return c.config.SoftLimitBytes
}

// GetHardLimit returns the configured hard limit.
func (c *MemoryBackpressureController) GetHardLimit() uint64 {
return c.config.HardLimitBytes
}

// CheckPressure evaluates current memory usage and returns pressure level.
func (c *MemoryBackpressureController) CheckPressure() PressureLevel {
var m runtime.MemStats
runtime.ReadMemStats(&m)
heapInUse := m.HeapInuse

var level PressureLevel
switch {
case heapInUse >= c.config.HardLimitBytes:
level = PressureHard
case heapInUse >= c.config.SoftLimitBytes:
level = PressureSoft
default:
level = PressureNone
}

c.pressureLevel.Store(int32(level))
metrics.MemoryPressureLevel.Set(float64(level))
metrics.MemoryHeapInUse.Set(float64(heapInUse))
return level
}

// GetPressureLevel returns the current pressure level without re-checking.
func (c *MemoryBackpressureController) GetPressureLevel() PressureLevel {
return PressureLevel(c.pressureLevel.Load())
}

// SetPressureLevel manually sets the pressure level (for testing).
func (c *MemoryBackpressureController) SetPressureLevel(level PressureLevel) {
c.pressureLevel.Store(int32(level))
}

// Acquire blocks until memory pressure allows proceeding.
func (c *MemoryBackpressureController) Acquire(ctx context.Context) error {
level := c.GetPressureLevel()

switch level {
case PressureHard:
// Wait for pressure to reduce or context to cancel
for {
select {
case <-ctx.Done():
c.rejectCount.Add(1)
metrics.MemoryBackpressureRejectsTotal.Inc()
return ctx.Err()
case <-time.After(c.config.CheckInterval):
if c.CheckPressure() != PressureHard {
c.acquireCount.Add(1)
metrics.MemoryBackpressureAcquiresTotal.Inc()
return nil
}
}
}
case PressureSoft:
// Add delay to slow down ingestion
if c.config.SoftPressureDelay > 0 {
select {
case <-ctx.Done():
return ctx.Err()
case <-time.After(c.config.SoftPressureDelay):
}
}
}

c.acquireCount.Add(1)
metrics.MemoryBackpressureAcquiresTotal.Inc()
return nil
}

// Release signals completion of a memory-intensive operation.
func (c *MemoryBackpressureController) Release() {
c.releaseCount.Add(1)
metrics.MemoryBackpressureReleasesTotal.Inc()
}

// GetAcquireCount returns the total number of successful acquires.
func (c *MemoryBackpressureController) GetAcquireCount() uint64 {
return c.acquireCount.Load()
}

// GetRejectCount returns the total number of rejected acquires.
func (c *MemoryBackpressureController) GetRejectCount() uint64 {
return c.rejectCount.Load()
}
