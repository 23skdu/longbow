package store

import (
	"math/rand"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQ8Indexing(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(16, arrow.PrimitiveTypes.Float32)},
	}, nil)
	ds := NewDataset("test_sq8", schema)

	cfg := DefaultArrowHNSWConfig()
	cfg.M = 16
	cfg.EfConstruction = 50
	cfg.SQ8Enabled = true
	cfg.SQ8TrainingThreshold = 100
	// To test quantization, we need vectors that are not 0.

	idx := NewArrowHNSW(ds, cfg, nil)

	// insert 500 vectors
	n := 500
	vecs := make([][]float32, n)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	vecBuilder := builder.Field(0).(*array.FixedSizeListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	// Generate batch with range [-10, 10]
	for i := 0; i < n; i++ {
		vec := make([]float32, 16)
		for j := range vec {
			vec[j] = rng.Float32()*20 - 10
		}
		vecs[i] = vec

		vecBuilder.Append(true)
		floatBuilder.AppendValues(vec, nil)
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()

	ds.Records = append(ds.Records, rec)
	rec.Retain()

	// Add batch
	ids, err := idx.AddBatch([]arrow.RecordBatch{rec}, makeRangeHelper(n), make([]int, n))
	require.NoError(t, err)

	// Verify that VectorsSQ8 is populated
	data := idx.data.Load()
	// Length check on VectorsSQ8 slice doesn't work directly if it's sparse/atomic?
	// But it is []uint64 so len works.
	assert.Greater(t, len(data.VectorsSQ8), 0, "VectorsSQ8 should be populated")
	if len(data.VectorsSQ8) > 0 {
		chunk := data.GetVectorsSQ8Chunk(0)
		if len(chunk) == 0 {
			t.Error("SQ8 vectors not encoded")
		}
	}
	// Capacity-based check
	// With chunked storage, VectorsSQ8 is []uint64 (number of chunks)
	// Default capacity 1000 -> 1 chunk
	numChunks := (data.Capacity + ChunkSize - 1) / ChunkSize
	assert.Equal(t, numChunks, len(data.VectorsSQ8))
	// Check size of the first chunk
	if len(data.VectorsSQ8) > 0 {
		chunk := data.GetVectorsSQ8Chunk(0)
		if chunk != nil {
			assert.Equal(t, ChunkSize*16, len(chunk))
		}
	}

	// Search
	// Pick vector 10 as query
	query := vecs[10]
	targetID := ids[10]
	// Approximate search
	res, err := idx.SearchVectors(query, 10, nil, SearchOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, res, "Search results should not be empty")

	// Expect vector 10 to be in top results
	found := false
	for _, r := range res {
		if uint32(r.ID) == targetID {
			found = true
			break
		}
	}
	assert.True(t, found, "Query vector should be found in search results with SQ8")

	t.Logf("Top Result Score: %f", res[0].Score)
	if uint32(res[0].ID) == targetID {
		assert.Equal(t, float32(0), res[0].Score, "Distance should be 0 for exact match in SQ8")
	}
}

func TestSQ8Refinement(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(16, arrow.PrimitiveTypes.Float32)},
	}, nil)
	ds := NewDataset("test_sq8_refine", schema)

	cfg := DefaultArrowHNSWConfig()
	cfg.M = 16
	cfg.EfConstruction = 50
	cfg.SQ8Enabled = true
	cfg.RefinementFactor = 5.0 // Fetch 5x candidates and re-rank

	idx := NewArrowHNSW(ds, cfg, nil)

	// insert 500 vectors
	n := 500
	vecs := make([][]float32, n)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	vecBuilder := builder.Field(0).(*array.FixedSizeListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	// Generate batch with range [-10, 10]
	for i := 0; i < n; i++ {
		vec := make([]float32, 16)
		for j := range vec {
			vec[j] = rng.Float32()*20 - 10
		}
		vecs[i] = vec

		vecBuilder.Append(true)
		floatBuilder.AppendValues(vec, nil)
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()

	ds.Records = append(ds.Records, rec)
	rec.Retain()

	// Add batch
	ids, err := idx.AddBatch([]arrow.RecordBatch{rec}, makeRangeHelper(n), make([]int, n))
	require.NoError(t, err)

	// Search
	query := vecs[10]
	targetID := ids[10]
	// Refined search
	res, err := idx.SearchVectors(query, 10, nil, SearchOptions{})
	require.NoError(t, err)

	// Expect vector 10 to be in top results
	found := false
	for _, r := range res {
		if uint32(r.ID) == targetID {
			found = true
			break
		}
	}
	assert.True(t, found, "Query vector should be found")

	// Verify scores are NOT integers (unless 0).
	// SQ8 distances are integers. Exact L2 are floats.
	hasFraction := false
	for _, r := range res {
		if r.Score > 0 && r.Score != float32(int(r.Score)) {
			hasFraction = true
			break
		}
	}
	// Small change that quantization noise makes integer? Unlikely.
	// But 0 is integer.
	assert.True(t, hasFraction, "Results should have fractional scores indicating exact re-ranking (unless all 0)")
}

func makeRangeHelper(maxVal int) []int {
	a := make([]int, maxVal)
	for i := range a {
		a[i] = i
	}
	return a
}
