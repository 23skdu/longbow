package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_DropDataset_FastPath(t *testing.T) {
	// 1. Setup Store
	s := createDropTestStore()
	// Check Shutdown signature in shutdown.go. Assuming Shutdown(ctx) or Shutdown() based on common patterns.
	// If shutdown.go shows Shutdown(ctx), use it.
	// Using defer with best guess, will correct if needed based on view_file
	// For now, assuming Shutdown(ctx) based on error message "want (context.Context)"
	defer s.Shutdown(context.Background())

	ctx := context.Background()

	// 2. Inject Dataset using internal helper if available, or public API
	dsName := "test_drop_heavy"
	ds := &Dataset{Name: dsName}

	// Use updateDatasets if available (package private)
	s.updateDatasets(func(m map[string]*Dataset) {
		m[dsName] = ds
	})

	// Verify it exists
	d, err := s.GetDataset(dsName) // Public GetDataset? or getDataset?
	// TestEviction uses getDataset (private).
	d, ok := s.getDataset(dsName)
	require.True(t, ok)
	require.NotNil(t, d)

	// 3. Measure Drop Time
	start := time.Now()
	err = s.DropDataset(ctx, dsName)
	require.NoError(t, err)
	duration := time.Since(start)

	// Assert Fast Path
	assert.Less(t, duration, 50*time.Millisecond)

	// 4. Verify Unlink
	dAfter, okAfter := s.getDataset(dsName)
	assert.False(t, okAfter, "Dataset should be removed from map")
	assert.Nil(t, dAfter)
}

func createDropTestStore() *VectorStore {
	return NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1024*1024, 0, 0)
}
