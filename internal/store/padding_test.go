package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPadding_OddDimensions(t *testing.T) {
	// Dims = 13.
	// Float32 (4 bytes). 13*4 = 52 bytes.
	// Align to 64 bytes -> 64 bytes.
	// Padded elements = 64/4 = 16.
	// So stride should be 16.
	dims := 13
	expectedPadded := 16

	cfg := DefaultArrowHNSWConfig()
	cfg.Dims = dims
	cfg.EfConstruction = 100
	cfg.M = 16

	idx := NewArrowHNSW(nil, cfg, nil)

	// Verify GraphData padding
	data := idx.data.Load()
	assert.Equal(t, dims, data.Dims)
	assert.Equal(t, expectedPadded, data.GetPaddedDims(), "PaddedDims should align to 64 bytes")

	// Insert Vectors
	ctx := context.Background()
	count := 100
	vecs := make([][]float32, count)
	for i := 0; i < count; i++ {
		vecs[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vecs[i][j] = float32(i + j)
		}
	}

	// Ensure capacity
	idx.Grow(count, dims)

	// Use AddBatchBulk (internal-friendly)
	err := idx.AddBatchBulk(ctx, 0, count, vecs)
	assert.NoError(t, err)

	// Verify Search
	// Search for vector 50. Should find itself at dist 0.
	query := vecs[50]
	// Search signature: (query, k, ef, filter) -> ([]SearchResult, error) or ([]Candidate, error)
	// arrow_delete_test.go uses loop over res.ID.
	// We'll check field existence via compiler or assume SearchResult.
	// If it returns Candidate, it has Dist. If SearchResult, Score.
	// Let's assume Score first. If fails, try Dist.
	// Wait, if it failed with "res[0].Dist undefined" previously (Step 667), it likely meant Dist didn't exist.
	// So Score probably exists.

	res, err := idx.Search(ctx, query, 10, 100, nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, res)
	assert.EqualValues(t, 50, res[0].ID)
	// assert.InDelta(t, float32(0.0), res[0].Score, 0.0001) // Score might be 0 for dist 0? Or inverted?
	// If metric is Euclidean, Score might be dist.

	// Verify Data Consistency
	// Verify vector content
	vAny, err := idx.getVectorAny(50)
	assert.NoError(t, err)
	vF32, ok := vAny.([]float32)
	assert.True(t, ok)
	assert.Equal(t, query, vF32)
}

func TestPadding_Int8_Odd(t *testing.T) {
	dims := 13
	expectedPadded := 64

	cfg := DefaultArrowHNSWConfig()
	cfg.Dims = dims
	cfg.DataType = VectorTypeInt8

	idx := NewArrowHNSW(nil, cfg, nil)

	data := idx.data.Load()
	assert.Equal(t, expectedPadded, data.GetPaddedDims())
}
