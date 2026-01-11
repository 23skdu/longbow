package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArrowHNSW_AddBatch_Parallel_SQ8 verifies that AddBatch works correctly
// (and safely) when SQ8 is enabled, which now uses parallel insertion.
func TestArrowHNSW_AddBatch_Parallel_SQ8(t *testing.T) {
	mem := memory.NewGoAllocator()

	// 1. Create vectors (Dims=128)
	numVectors := 1000
	dims := 128

	builder := array.NewFloat32Builder(mem)
	defer builder.Release()

	for i := 0; i < numVectors*dims; i++ {
		builder.Append(float32(i) * 0.001)
	}
	values := builder.NewFloat32Array()
	defer values.Release()

	// We have to append the values to the underlying value builder of the list builder?
	// Actually easier: use makeHNSWTestRecord helper if available, or build manually.
	// Since we are adding a new file, we can't rely on unexported helpers unless we are in the same package (we are 'package store').

	// Let's assume we can build a record batch manually.
	// Re-do strictly using available builders.

	vecBuilder := array.NewFixedSizeListBuilder(mem, int32(dims), arrow.PrimitiveTypes.Float32)
	defer vecBuilder.Release()
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numVectors; i++ {
		vecBuilder.Append(true)
		for j := 0; j < dims; j++ {
			valBuilder.Append(float32(i+j) * 0.01) // Simple pattern
		}
	}
	vecArray := vecBuilder.NewArray()
	defer vecArray.Release()

	// Create RecordBatch
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	rec := array.NewRecordBatch(schema, []arrow.Array{vecArray}, int64(numVectors))
	defer rec.Release()

	// 2. Setup Index
	ds := &Dataset{
		Name:    "test_parallel_sq8",
		Schema:  schema,
		Records: []arrow.RecordBatch{rec},
	}

	cfg := DefaultArrowHNSWConfig()
	cfg.M = 16
	cfg.EfConstruction = 100
	cfg.SQ8Enabled = true
	// Training threshold low to trigger training immediately
	cfg.SQ8TrainingThreshold = 100

	// Create Index
	idx := NewArrowHNSW(ds, cfg, nil)
	defer func() { _ = idx.Close() }()

	// 3. Call AddBatch
	// We'll simulate adding the entire batch
	recs := []arrow.RecordBatch{rec}
	rowIdxs := make([]int, numVectors)
	batchIdxs := make([]int, numVectors)
	for i := 0; i < numVectors; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}

	// 4. Run AddBatch (this will be parallel now)
	ids, err := idx.AddBatch(recs, rowIdxs, batchIdxs)
	require.NoError(t, err)
	assert.Equal(t, numVectors, len(ids))
	assert.Equal(t, numVectors, idx.Size())

	// 5. Verify basic search works (graph correctness)
	// Search for vector 50
	qVec := make([]float32, dims)
	for j := 0; j < dims; j++ {
		qVec[j] = float32(50+j) * 0.01
	}
	res, err := idx.SearchVectors(qVec, 10, nil, SearchOptions{})
	require.NoError(t, err)
	assert.NotEmpty(t, res)
	assert.Equal(t, uint32(50), uint32(res[0].ID))
}
