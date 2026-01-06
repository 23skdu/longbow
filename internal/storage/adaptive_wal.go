package storage

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// AdaptiveWALConfig configures adaptive batching behavior
type AdaptiveWALConfig struct {
	MinInterval   time.Duration // Minimum flush interval under high load (e.g., 1ms)
	MaxInterval   time.Duration // Maximum flush interval under low load (e.g., 100ms)
	TargetLatency time.Duration // Target p99 latency for writes (e.g., 5ms)
	Enabled       bool          // Whether adaptive batching is enabled
}

// NewAdaptiveWALConfig returns sensible defaults for adaptive batching
func NewAdaptiveWALConfig() AdaptiveWALConfig {
	return AdaptiveWALConfig{
		MinInterval:   1 * time.Millisecond,
		MaxInterval:   100 * time.Millisecond,
		TargetLatency: 5 * time.Millisecond,
		Enabled:       true,
	}
}

// Validate checks the configuration for consistency
func (c *AdaptiveWALConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.MinInterval <= 0 {
		return errors.New("MinInterval must be positive")
	}
	if c.MaxInterval <= 0 {
		return errors.New("MaxInterval must be positive")
	}
	if c.MinInterval > c.MaxInterval {
		return errors.New("MinInterval must be less than or equal to MaxInterval")
	}
	if c.TargetLatency <= 0 {
		return errors.New("TargetLatency must be positive")
	}
	return nil
}

// AdaptiveIntervalCalculator computes optimal flush intervals based on load
type AdaptiveIntervalCalculator struct {
	cfg AdaptiveWALConfig
}

// NewAdaptiveIntervalCalculator creates a new calculator with the given config
func NewAdaptiveIntervalCalculator(cfg AdaptiveWALConfig) *AdaptiveIntervalCalculator {
	return &AdaptiveIntervalCalculator{cfg: cfg}
}

// CalculateInterval returns the optimal flush interval for the given write rate
func (c *AdaptiveIntervalCalculator) CalculateInterval(writeRate float64) time.Duration {
	if writeRate <= 0 {
		metrics.WalAdaptiveIntervalMs.Set(float64(c.cfg.MaxInterval.Milliseconds()))
		return c.cfg.MaxInterval
	}

	minNs := float64(c.cfg.MinInterval.Nanoseconds())
	maxNs := float64(c.cfg.MaxInterval.Nanoseconds())
	rangeNs := maxNs - minNs

	const decayConstant = 0.0046
	decay := math.Exp(-decayConstant * writeRate)
	intervalNs := minNs + rangeNs*decay

	interval := time.Duration(intervalNs)

	if interval < c.cfg.MinInterval {
		metrics.WalAdaptiveIntervalMs.Set(float64(c.cfg.MinInterval.Milliseconds()))
		return c.cfg.MinInterval
	}
	if interval > c.cfg.MaxInterval {
		metrics.WalAdaptiveIntervalMs.Set(float64(c.cfg.MaxInterval.Milliseconds()))
		return c.cfg.MaxInterval
	}
	metrics.WalAdaptiveIntervalMs.Set(float64(interval.Milliseconds()))
	return interval
}

// WriteRateTracker tracks write rate using exponential moving average
type WriteRateTracker struct {
	window     time.Duration
	lastTime   atomic.Int64
	writeCount atomic.Int64
	rate       atomic.Uint64
	mu         sync.Mutex
}

// NewWriteRateTracker creates a tracker with the given averaging window
func NewWriteRateTracker(window time.Duration) *WriteRateTracker {
	t := &WriteRateTracker{window: window}
	t.lastTime.Store(time.Now().UnixNano())
	return t
}

// RecordWrite records a single write operation
func (t *WriteRateTracker) RecordWrite() {
	t.writeCount.Add(1)
	t.maybeUpdateRate()
}

func (t *WriteRateTracker) maybeUpdateRate() {
	now := time.Now().UnixNano()
	last := t.lastTime.Load()
	elapsed := time.Duration(now - last)

	if elapsed < 10*time.Millisecond {
		return
	}

	if !t.mu.TryLock() {
		return
	}
	defer t.mu.Unlock()

	last = t.lastTime.Load()
	elapsed = time.Duration(now - last)
	if elapsed < 10*time.Millisecond {
		return
	}

	count := t.writeCount.Swap(0)
	instantRate := float64(count) / elapsed.Seconds()

	alpha := math.Min(1.0, elapsed.Seconds()/t.window.Seconds())
	oldRate := math.Float64frombits(t.rate.Load())
	newRate := alpha*instantRate + (1-alpha)*oldRate

	t.rate.Store(math.Float64bits(newRate))
	// Emit write rate metric
	metrics.WalWriteRatePerSecond.Set(newRate)
	t.lastTime.Store(now)
}

// GetRate returns the current estimated write rate (writes per second)
func (t *WriteRateTracker) GetRate() float64 {
	now := time.Now().UnixNano()
	last := t.lastTime.Load()
	elapsed := time.Duration(now - last)

	rate := math.Float64frombits(t.rate.Load())

	if elapsed > 10*time.Millisecond {
		decay := math.Exp(-elapsed.Seconds() / t.window.Seconds())
		rate *= decay
	}
	return rate
}
