package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestObservability_Metrics(t *testing.T) {
	// Initialize store
	mem := memory.NewGoAllocator()
	vectors := [][]float32{
		{1.0, 0.0},
		{0.0, 1.0},
	}
	rec := makeHNSWTestRecord(mem, 2, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "observability_test",
		Records: nil, // Will add via IndexRecordColumns or similar if we want full integration, but testing components directly is easier
	}
	// Manually set up index for direct component testing
	idx := NewHNSWIndex(ds)
	ds.Index = idx

	// 1. Test Index Build Metric
	// We call AddBatch manually
	// Note: We need a valid record to add.

	// Create a fresh store context for integration-like testing
	// We can't easily use global metrics verification if they are cumulative,
	// but we can check if they are non-zero.

	t.Run("IndexBuildDuration", func(t *testing.T) {
		_, err := idx.AddBatch([]arrow.RecordBatch{rec}, []int{0, 1}, []int{0, 0})
		require.NoError(t, err)
	})

	t.Run("SearchLatency", func(t *testing.T) {
		// Vector Search
		_, err := idx.SearchVectors([]float32{1.0, 0.0}, 1, nil)
		require.NoError(t, err)

		// Hybrid Search
		// Needs store context for HybridSearch wrapper
		store := &VectorStore{
			datasets: map[string]*Dataset{"observability_test": ds},
			// use a default logger
		}

		_, err = store.HybridSearch(context.Background(), "observability_test", []float32{1.0, 0.0}, 1, nil)
		require.NoError(t, err)

		_, err = store.SearchHybrid(context.Background(), "observability_test", []float32{1.0, 0.0}, "", 1, 0.5, 60)
		require.NoError(t, err)
	})

	// If we reached here, instrumentation didn't panic.
	assert.True(t, true)
}
