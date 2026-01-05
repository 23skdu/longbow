package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_Metrics(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Vectors: [1, 0] and [0, 1]
	vectors := [][]float32{
		{1.0, 0.0},
		{0.0, 1.0},
	}
	dims := 2
	rec := makeHNSWTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "arrow_metrics_test",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	t.Run("Cosine", func(t *testing.T) {
		config := DefaultArrowHNSWConfig()
		config.M = 16
		config.EfConstruction = 100
		config.Metric = MetricCosine

		idx := NewArrowHNSW(ds, config, nil)

		// Add vectors 0 and 1
		// AddBatch or AddByRecord
		// AddByRecord maps to AddByLocation(batchIdx, rowIdx)
		// For simplicity, we use AddByLocation assuming we have locationStore or default behavior
		// Dataset has 1 batch (index 0).

		_, err := idx.AddByLocation(0, 0)
		require.NoError(t, err)
		_, err = idx.AddByLocation(0, 1)
		require.NoError(t, err)

		// Search for [1.0, 0.0]
		res, err := idx.SearchVectors([]float32{1.0, 0.0}, 2, nil)
		require.NoError(t, err)
		require.Len(t, res, 2)

		// Sort results to be sure
		// res[0] should be self (dist 0)
		// res[1] should be other (dist 1)

		// Find ID 0
		foundSelf := false
		for _, r := range res {
			if r.ID == 0 {
				assert.InDelta(t, 0.0, r.Score, 1e-6)
				foundSelf = true
			}
		}
		assert.True(t, foundSelf, "Should find self")
	})
}
