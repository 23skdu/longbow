package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_AddBatch_Parallel_Dense_Packed(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 128
	numVectors := 1000

	arrowConfig := DefaultArrowHNSWConfig()
	arrowConfig.Dims = dims
	arrowConfig.SQ8Enabled = false // Test Dense first
	arrowConfig.PackedAdjacencyEnabled = true

	idx := NewArrowHNSW(nil, arrowConfig, nil)

	// Generate vectors
	vecBuilder := array.NewFixedSizeListBuilder(mem, int32(dims), arrow.PrimitiveTypes.Float32)
	defer vecBuilder.Release()
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numVectors; i++ {
		vecBuilder.Append(true)
		for j := 0; j < dims; j++ {
			valBuilder.Append(float32(i+j) * 0.01)
		}
	}
	vecArray := vecBuilder.NewArray()
	defer vecArray.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "vector", Type: vecArray.DataType(), Nullable: true}}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{vecArray}, int64(numVectors))
	defer rec.Release()

	recs := []arrow.RecordBatch{rec}
	rowIdxs := make([]int, numVectors)
	batchIdxs := make([]int, numVectors)
	for i := 0; i < numVectors; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}

	// AddBatch (Bulk)
	ids, err := idx.AddBatch(context.Background(), recs, rowIdxs, batchIdxs)
	require.NoError(t, err)
	assert.Equal(t, numVectors, len(ids))

	// Search for node 50
	qVec := make([]float32, dims)
	for j := 0; j < dims; j++ {
		qVec[j] = float32(50+j) * 0.01
	}

	res, err := idx.SearchVectors(context.Background(), qVec, 10, nil, SearchOptions{})
	require.NoError(t, err)
	assert.NotEmpty(t, res)
	fmt.Printf("Top 1 ID: %d, Score: %f\n", res[0].ID, res[0].Score)
	assert.Equal(t, uint32(50), uint32(res[0].ID))
}
