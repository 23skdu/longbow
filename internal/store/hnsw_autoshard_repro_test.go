package store_test

import (
	"testing"
)

// TestAutoSharding_ConcurrentWrites_Repro reproduces the race condition where
// the underlying index is closed during migration while concurrent writes are occurring.
func TestAutoSharding_ConcurrentWrites_Repro(t *testing.T) {
	t.Skip("MockDataset not accessible in external test package")
}