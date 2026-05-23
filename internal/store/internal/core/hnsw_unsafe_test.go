package core_test

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/store/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSW_UnsafeAccess(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := [][]float32{{1.0, 2.0}, {3.0, 4.0}}
	rec := core.MakeBatchTestRecord(mem, 2, vectors)
	defer rec.Release()

	ds := &core.MockDataset{
		Name:    "test_unsafe",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}
	idx := core.NewTestHNSWIndex(ds)

	_, err := idx.AddByLocation(context.Background(), 0, 0)
	require.NoError(t, err)
	_, err = idx.AddByLocation(context.Background(), 0, 1)
	require.NoError(t, err)

	// Verify we can access the graph data
	data := idx.GetData()
	require.NotNil(t, data)

	// Check vector in first chunk
	chunk := data.GetVectorsChunk(0)
	require.NotNil(t, chunk)

	paddedDims := data.GetPaddedDimsForType(types.VectorTypeFloat32)
	assert.Equal(t, float32(1.0), chunk[0])
	assert.Equal(t, float32(2.0), chunk[1])

	// Node 1 starts at paddedDims
	require.True(t, len(chunk) >= paddedDims+2)
	assert.Equal(t, float32(3.0), chunk[paddedDims])
	assert.Equal(t, float32(4.0), chunk[paddedDims+1])
}
