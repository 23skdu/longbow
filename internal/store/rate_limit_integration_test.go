package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestCompaction_RateLimit runs the test compaction_rate limit operation.
func TestCompaction_RateLimit(t *testing.T) {
	cfg := DefaultCompactionConfig()
	cfg.RateLimitBytesPerSec = 1024

	rl := NewRateLimiter(cfg.RateLimitBytesPerSec)

	start := time.Now()
	err := rl.Wait(context.Background(), 1024)
	assert.NoError(t, err)

	err = rl.Wait(context.Background(), 1024)
	assert.NoError(t, err)

	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, time.Second)
}
