package index

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCacheBlockedTraversal_MetricIncremented verifies that the
// CacheBlockedTraversalChunksTotal metric is incremented during HNSW
// search when neighbors are processed in 64-vector cache-blocked chunks.
func TestCacheBlockedTraversal_MetricIncremented(t *testing.T) {
	mem := memory.NewGoAllocator()
	dim := 128

	// Insert enough vectors to guarantee multi-chunk neighbor traversal.
	numVectors := 256
	vectors := make([][]float32, numVectors)
	for i := range vectors {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(i*dim+j) * 0.01
		}
		vectors[i] = v
	}

	rec := MakeBatchTestRecord(mem, dim, vectors)
	defer rec.Release()

	ds := &MockDataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewTestHNSWIndex(ds)

	// Insert all vectors
	for i := 0; i < numVectors; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// Record metric before search
	before := testutil.ToFloat64(metrics.CacheBlockedTraversalChunksTotal)

	// Search with ef=128 to force wide traversal
	q := make([]float32, dim)
	for j := range q {
		q[j] = float32(j) * 0.01
	}
	result, err := idx.SearchVectors(context.Background(), q, 32, nil, types.SearchOptions{
		Ef: 128,
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Greater(t, len(result), 0, "search should return results")

	// Verify cache-blocked chunks were processed
	after := testutil.ToFloat64(metrics.CacheBlockedTraversalChunksTotal)
	assert.Greater(t, after, before,
		"CacheBlockedTraversalChunksTotal should increase during search")
}
