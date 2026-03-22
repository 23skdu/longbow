package store

import (
	"testing"
)

func TestCompaction_RateLimit(t *testing.T) {
	// Test depends on NewVectorStoreWithCompaction constructor and RateLimitBytesPerSec config
	// field, which do not currently exist in the codebase. The rate limiting mechanism
	// (GOGC tuner, LONGBOW_MAX_MEMORY) is controlled externally rather than per-store.
	// Re-enable when NewVectorStoreWithCompaction and rate limit config are implemented.
	t.Skip("Skipping: NewVectorStoreWithCompaction and RateLimitBytesPerSec config do not exist")
}
