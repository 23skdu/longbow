package store

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestHNSW_BulkInsert(t *testing.T) {
	// Setup
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dims := 16 // Small dims for speed
	numVectors := 5000

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	// Generate Data
	bId := array.NewInt64Builder(mem)
	defer bId.Release()

	bVec := array.NewFixedSizeListBuilder(mem, int32(dims), arrow.PrimitiveTypes.Float32)
	defer bVec.Release()
	bVecValues := bVec.ValueBuilder().(*array.Float32Builder)

	rng := rand.New(rand.NewSource(42))

	// Keep track of a known vector to search for
	var queryVec []float32
	targetDesc := "vector_2500"

	for i := 0; i < numVectors; i++ {
		bId.Append(int64(i))
		bVec.Append(true)

		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = rng.Float32()
		}
		bVecValues.AppendValues(vec, nil)

		if i == 2500 {
			queryVec = vec
		}
	}

	arrId := bId.NewArray()
	defer arrId.Release()
	arrVec := bVec.NewArray()
	defer arrVec.Release()

	batch := array.NewRecordBatch(schema, []arrow.Array{arrId, arrVec}, int64(numVectors))
	defer batch.Release()

	// Initialize Index
	ds := NewDataset("test_bulk", schema)
	// IMPORTANT: Add batch to dataset so Index can resolve vectors
	batch.Retain()
	ds.Records = append(ds.Records, batch)

	defer func() {
		for _, r := range ds.Records {
			r.Release()
		}
	}()

	cfg := DefaultArrowHNSWConfig()
	cfg.Dims = dims
	cfg.M = 16
	cfg.EfConstruction = 100
	// Ensure auto-sharding doesn't mess with us (though we use raw HNSW here)

	idx := NewArrowHNSW(ds, cfg, nil)
	defer func() { _ = idx.Close() }()

	// Insert Batch
	t.Logf("Dataset records: %d", len(ds.Records))
	start := time.Now()
	// Mock rowIdxs and batchIdxs
	rowIdxs := make([]int, numVectors)
	batchIdxs := make([]int, numVectors)
	for i := 0; i < numVectors; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}

	ids, err := idx.AddBatch([]arrow.RecordBatch{batch}, rowIdxs, batchIdxs)
	require.NoError(t, err)
	require.Len(t, ids, numVectors)

	duration := time.Since(start)
	t.Logf("Inserted %d vectors in %v (%.2f vec/s)", numVectors, duration, float64(numVectors)/duration.Seconds())

	// Verify Size
	require.Equal(t, numVectors, idx.Len())

	// Verify Search (Recall Check)
	// We expect to find the exact vector we inserted (distance ~0)
	t.Logf("Starting Search for target")
	results, err := idx.SearchVectors(context.Background(), queryVec, 10, nil, SearchOptions{})
	t.Logf("Finished Search, err=%v, len=%d", err, len(results))
	require.NoError(t, err)
	require.NotEmpty(t, results)

	found := false
	for _, res := range results {
		// ID 2500 corresponds to the insertion index 2500 (since we started empty)
		// Wait, AddBatch returns assigned IDs. In empty store, they should match 0..N-1?
		// Note: AddBatch calls locationStore.BatchAppend.
		// If store is empty, IDs start at 0.
		if uint32(res.ID) == 2500 {
			found = true
			require.InDelta(t, 0.0, res.Score, 1e-5, "Expected distance to self to be 0")
			break
		}
	}
	require.True(t, found, "Target vector %s not found in search results", targetDesc)
}
