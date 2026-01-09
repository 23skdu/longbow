package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimiter_Basic(t *testing.T) {
	// Limit to 100 bytes per second
	limit := int64(100)
	rl := NewRateLimiter(limit)

	assert.Equal(t, float64(100), rl.Limit())

	ctx := context.Background()
	start := time.Now()

	// Consume 10 bytes (should be immediate if burst allows)
	// The burst is set to limit (100), so 10 should be immediate.
	err := rl.Wait(ctx, 10)
	require.NoError(t, err)

	// Check it didn't block significantly
	assert.WithinDuration(t, start, time.Now(), 10*time.Millisecond)
}

func TestRateLimiter_Throttling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throttling test in short mode")
	}

	// Limit to 100 bytes per second with burst of 100
	limit := int64(100)
	rl := NewRateLimiter(limit)

	// Drain the burst first
	ctx := context.Background()
	err := rl.Wait(ctx, 100)
	require.NoError(t, err)

	start := time.Now()
	// Now request 50 more bytes. Should take ~0.5 seconds
	err = rl.Wait(ctx, 50)
	require.NoError(t, err)

	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(450), "should throttle for at least 0.45s")
}

func TestRateLimiter_Disabled(t *testing.T) {
	rl := NewRateLimiter(0)
	ctx := context.Background()
	start := time.Now()

	// Request huge amount, should pass immediately
	err := rl.Wait(ctx, 1000000)
	require.NoError(t, err)
	assert.WithinDuration(t, start, time.Now(), 5*time.Millisecond)
}
