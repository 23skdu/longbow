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
	stopChan      chan struct{}
	stopOnce      sync.Once
}

// NewMemoryBackpressureController creates a new backpressure controller.
func NewMemoryBackpressureController(cfg BackpressureConfig) *MemoryBackpressureController {
	ctrl := &MemoryBackpressureController{
		config: cfg,
	}
	ctrl.cond = sync.NewCond(&ctrl.mu)
	ctrl.stopChan = make(chan struct{})
	return ctrl
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
					c.cond.Broadcast()
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
		c.cond.Broadcast()
	}
}

// Acquire blocks until memory pressure allows proceeding.
func (c *MemoryBackpressureController) Acquire(ctx context.Context) error {
	// Optimistic check without lock first
	level := c.GetPressureLevel()

	if level == PressureHard {
		c.mu.Lock()
		for c.GetPressureLevel() == PressureHard {
			// Wait uses Cond.Wait, which releases lock and re-acquires
			// We need to watch context cancellation too.
			// Cond.Wait doesn't support context, so we use a wait with timeout/polling or
			// separate channel. Since we have a Ticker in background, relying on Broadcast is fine.
			// But for Context cancellation, we can't easily break Wait().
			//
			// Alternative: Use a loop with occasional polling or use a channel-based verify.
			// Given this is Hard pressure (rare), a simple polling wait or broadcast is okay.
			// But if we want to respect context immediately:

			// We can use a channel based approach or simply checking context before Wait.
			// But Wait blocks indefinitely.
			//
			// Safe approach with Context:
			// Use a select with signal channel.
			// But current design uses Cond.

			// Let's stick to Cond but we must ensure we don't hang if ctx is canceled.
			// Actually Cond.Wait is not cancelable.
			// We should probably convert this to channel-based notification or just poll.
			//
			// Let's modify to use a loop with short sleeps if we can't strictly use Cond with Context easily
			// OR spawn a waiter.
			//
			// Actually, the background ticker broadcasts on relief.
			// The issue is simply `c.cond.Wait()` ignores context.
			// A common pattern:

			c.mu.Unlock() // Release lock to check context

			select {
			case <-ctx.Done():
				c.rejectCount.Add(1)
				metrics.MemoryBackpressureRejectsTotal.Inc()
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
				// Poll / Wait
			}

			// Re-check
			if c.GetPressureLevel() != PressureHard {
				break
			}
			// Loop and check context again
			// Note: We are not holding lock here, so we might miss a broadcast but we have polling.
			// This is a hybrid approach. Effectively we poll every 100ms when under hard pressure.
			continue
		}
		// If we broke out, pressure is relieved (or never was hard)
	} else if level == PressureSoft { // Existing soft limit logic
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

// Stop halts the background monitoring.
func (c *MemoryBackpressureController) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopChan)
	})
}
