package store

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"golang.org/x/sync/semaphore"
)

// RequestSemaphoreConfig configures the request semaphore for limiting
// concurrent DoGet/DoPut operations to prevent thread-thrashing.
type RequestSemaphoreConfig struct {
	// MaxConcurrent is the maximum number of concurrent requests.
	// Default: runtime.GOMAXPROCS(0) (number of CPU cores)
	MaxConcurrent int

	// AcquireTimeout is how long to wait before timing out on acquire.
	// Default: 30 seconds
	AcquireTimeout time.Duration

	// Enabled controls whether the semaphore is active.
	// When disabled, Acquire always succeeds immediately.
	Enabled bool
}

// DefaultRequestSemaphoreConfig returns a configuration with sensible defaults.
func DefaultRequestSemaphoreConfig() RequestSemaphoreConfig {
	return RequestSemaphoreConfig{
		MaxConcurrent:  runtime.GOMAXPROCS(0),
		AcquireTimeout: 30 * time.Second,
		Enabled:        true,
	}
}

// Validate checks if the configuration is valid.
func (c RequestSemaphoreConfig) Validate() error {
	if !c.Enabled {
		return nil // Skip validation when disabled
	}
	if c.MaxConcurrent <= 0 {
		return NewConfigError("RequestSemaphoreConfig", "MaxConcurrent", "<=0", "must be positive when enabled")
	}
	if c.AcquireTimeout <= 0 {
		return NewConfigError("RequestSemaphoreConfig", "AcquireTimeout", "<=0", "must be positive when enabled")
	}
	return nil
}

// SemaphoreStats holds statistics about semaphore usage.
type SemaphoreStats struct {
	TotalAcquires int64
	TotalReleases int64
	TotalTimeouts int64
	CurrentActive int64
	MaxConcurrent int
}

// RequestSemaphore limits concurrent DoGet/DoPut operations using a weighted semaphore.
type RequestSemaphore struct {
	sem    *semaphore.Weighted
	config RequestSemaphoreConfig

	// Statistics
	acquires int64
	releases int64
	timeouts int64
	active   int64
}

// NewRequestSemaphore creates a new request semaphore with the given configuration.
func NewRequestSemaphore(cfg RequestSemaphoreConfig) *RequestSemaphore {
	var sem *semaphore.Weighted
	if cfg.Enabled && cfg.MaxConcurrent > 0 {
		sem = semaphore.NewWeighted(int64(cfg.MaxConcurrent))
	}
	return &RequestSemaphore{
		sem:    sem,
		config: cfg,
	}
}

// Acquire blocks until a slot is available or timeout/context cancellation occurs.
func (rs *RequestSemaphore) Acquire(ctx context.Context) error {
	if !rs.config.Enabled {
		return nil // Bypass when disabled
	}

	// Track waiting requests
	metrics.SemaphoreWaitingRequests.Inc()
	start := time.Now()

	// Create timeout context
	ctx, cancel := context.WithTimeout(ctx, rs.config.AcquireTimeout)
	defer cancel()

	err := rs.sem.Acquire(ctx, 1)

	// Done waiting - record metrics
	metrics.SemaphoreWaitingRequests.Dec()
	metrics.SemaphoreQueueDurationSeconds.Observe(time.Since(start).Seconds())

	if err != nil {
		atomic.AddInt64(&rs.timeouts, 1)
		metrics.SemaphoreTimeoutsTotal.Inc()
		return NewResourceExhaustedError("semaphore", "acquire timeout or context cancelled")
	}

	atomic.AddInt64(&rs.acquires, 1)
	atomic.AddInt64(&rs.active, 1)
	metrics.SemaphoreAcquiredTotal.Inc()
	metrics.SemaphoreActiveRequests.Inc()
	return nil
}

// TryAcquire attempts to acquire a slot without blocking.
// Returns true if successful, false if no slots available.
func (rs *RequestSemaphore) TryAcquire() bool {
	if !rs.config.Enabled {
		return true // Always succeed when disabled
	}

	if rs.sem.TryAcquire(1) {
		atomic.AddInt64(&rs.acquires, 1)
		atomic.AddInt64(&rs.active, 1)
		return true
	}
	return false
}

// Release releases a slot back to the semaphore.
func (rs *RequestSemaphore) Release() {
	if !rs.config.Enabled {
		return // No-op when disabled
	}

	rs.sem.Release(1)
	atomic.AddInt64(&rs.releases, 1)
	atomic.AddInt64(&rs.active, -1)
}

// MaxConcurrent returns the maximum concurrent requests allowed.
func (rs *RequestSemaphore) MaxConcurrent() int {
	return rs.config.MaxConcurrent
}

// Available returns the number of available slots.
func (rs *RequestSemaphore) Available() int {
	if !rs.config.Enabled {
		return rs.config.MaxConcurrent
	}
	return rs.config.MaxConcurrent - int(atomic.LoadInt64(&rs.active))
}

// Stats returns current statistics about semaphore usage.
func (rs *RequestSemaphore) Stats() SemaphoreStats {
	return SemaphoreStats{
		TotalAcquires: atomic.LoadInt64(&rs.acquires),
		TotalReleases: atomic.LoadInt64(&rs.releases),
		TotalTimeouts: atomic.LoadInt64(&rs.timeouts),
		CurrentActive: atomic.LoadInt64(&rs.active),
		MaxConcurrent: rs.config.MaxConcurrent,
	}
}
