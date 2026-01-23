package store

import (
	"context"

	"golang.org/x/time/rate"
)

// RateLimiter wraps a token bucket limiter to provide easy integration
// with bandwidth-heavy background operations like compaction and snapshotting.
type RateLimiter struct {
	limiter *rate.Limiter
}

// NewRateLimiter creates a new limiter with the specified bytes per second limit.
// If limitBytes is <= 0, it returns a no-op limiter (infinite rate).
func NewRateLimiter(limitBytes int64) *RateLimiter {
	if limitBytes <= 0 {
		return &RateLimiter{limiter: nil}
	}
	// rate.Limit is tokens per second. We interpret limitBytes as bytes/sec.
	// Burst is set to limitsBytes (1 second worth of burst) or a reasonable default.
	return &RateLimiter{
		limiter: rate.NewLimiter(rate.Limit(limitBytes), int(limitBytes)),
	}
}

// Wait blocks until enough tokens are available for N bytes.
// If the limiter is disabled (nil), it returns immediately.
func (rl *RateLimiter) Wait(ctx context.Context, n int) error {
	if rl.limiter == nil {
		return nil
	}
	// If the request exceeds the burst, WaitN would normally error out immediately.
	// We want to force it to wait anyway.
	if n > rl.limiter.Burst() {
		rl.limiter.SetBurst(n)
	}
	return rl.limiter.WaitN(ctx, n)
}

// Limit returns the current limit in bytes per second.
func (rl *RateLimiter) Limit() float64 {
	if rl.limiter == nil {
		return 0
	}
	return float64(rl.limiter.Limit())
}

// SetLimit updates the rate limit.
func (rl *RateLimiter) SetLimit(limitBytes int64) {
	if rl.limiter == nil && limitBytes > 0 {
		// Can't enable if nil, would need to re-create or support hot-swapping.
		// For now simple wrapper.
		return
	}
	if rl.limiter != nil {
		rl.limiter.SetLimit(rate.Limit(limitBytes))
		rl.limiter.SetBurst(int(limitBytes))
	}
}
