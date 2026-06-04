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
	// PressureNone indicates no memory pressure.
	PressureNone PressureLevel = iota
	// PressureSoft indicates memory usage is above the soft limit; operations are delayed.
	PressureSoft
	// PressureHard indicates memory usage is above the hard limit; operations are blocked.
	PressureHard
)

// BackpressureConfig configures memory backpressure behavior thresholds and delays.
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
	signal        chan struct{} // closed to broadcast pressure relief; recreated after close
	stopChan      chan struct{}
	stopOnce      sync.Once
}

// NewMemoryBackpressureController creates a new backpressure controller.
func NewMemoryBackpressureController(cfg BackpressureConfig) *MemoryBackpressureController {
	ctrl := &MemoryBackpressureController{
		config:  cfg,
		signal:  make(chan struct{}),
		stopChan: make(chan struct{}),
	}
	return ctrl
}

// broadcast wakes all goroutines blocked in Acquire and prepares
// a fresh signal channel for the next round of waiting.
func (c *MemoryBackpressureController) broadcast() {
	close(c.signal)
	c.signal = make(chan struct{})
}

// Start begins the background memory monitoring.
func (c *MemoryBackpressureController) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(c.config.CheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopChan:
				return
			case <-ticker.C:
				prevLevel := c.GetPressureLevel()
				newLevel := c.CheckPressure()

				// If pressure relieved (Hard -> Soft/None), wake up waiters
				if prevLevel == PressureHard && newLevel != PressureHard {
					c.mu.Lock()
					c.broadcast()
					c.mu.Unlock()
				}
			}
		}
	}()
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
// This is expensive (runtime.ReadMemStats) and should usually be called by the background ticker.
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
	prev := c.GetPressureLevel()
	c.pressureLevel.Store(int32(level))
	if prev == PressureHard && level != PressureHard {
		c.mu.Lock()
		c.broadcast()
		c.mu.Unlock()
	}
}

// Acquire blocks until memory pressure allows proceeding.
func (c *MemoryBackpressureController) Acquire(ctx context.Context) error {
	// Optimistic check without lock first
	level := c.GetPressureLevel()

	if level == PressureHard {
		c.mu.Lock()
		for c.GetPressureLevel() == PressureHard {
			signal := c.signal
			c.mu.Unlock()

			select {
			case <-signal:
				// Pressure may have dropped; re-check
			case <-ctx.Done():
				c.rejectCount.Add(1)
				metrics.MemoryBackpressureRejectsTotal.Inc()
				return ctx.Err()
			}

			c.mu.Lock()
		}
		c.mu.Unlock()
	} else if level == PressureSoft {
		if c.config.SoftPressureDelay > 0 {
			timer := time.NewTimer(c.config.SoftPressureDelay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
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
	// A release may have freed enough memory to relieve pressure.
	// Broadcast so blocked acquirers re-check promptly instead of
	// waiting for the next background tick.
	c.mu.Lock()
	c.broadcast()
	c.mu.Unlock()
}

// GetAcquireCount returns the total number of successful acquires.
func (c *MemoryBackpressureController) GetAcquireCount() uint64 {
	return c.acquireCount.Load()
}

// GetRejectCount returns the total number of rejected acquires.
func (c *MemoryBackpressureController) GetRejectCount() uint64 {
	return c.rejectCount.Load()
}

// Stop halts the background monitoring.
func (c *MemoryBackpressureController) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopChan)
	})
}
