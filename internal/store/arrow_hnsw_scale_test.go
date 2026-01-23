package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// makeScaleTestRecord creates a large Arrow RecordBatch for testing
func makeScaleTestRecord(mem memory.Allocator, dims, numVectors int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()

	listBuilder := array.NewFixedSizeListBuilder(mem, int32(dims), arrow.PrimitiveTypes.Float32)
	defer listBuilder.Release()

	vecBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)
	// We do NOT release vecBuilder as it is owned by listBuilder

	// Pre-reserve to avoid frequent reallocations
	idBuilder.Reserve(numVectors)
	listBuilder.Reserve(numVectors)
	vecBuilder.Reserve(numVectors * dims)

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(int64(i))
		listBuilder.Append(true)
		// deterministic vector: [i, 0.5, 0.0, ... ] normalized? No, just raw.
		// Use simple pattern: i % 100
		val := float32(i % 100)
		vecBuilder.Append(val)
		for j := 1; j < dims; j++ {
			vecBuilder.Append(0.0)
		}
	}

	return array.NewRecordBatch(schema, []arrow.Array{idBuilder.NewArray(), listBuilder.NewArray()}, int64(numVectors))
}

func TestArrowHNSW_LargeBatchIngestion_30k(t *testing.T) {
	mem := memory.NewGoAllocator()
	ds := NewDataset("scale_test_30k", nil)

	// Config
	count := 30000
	dims := 128

	cfg := DefaultArrowHNSWConfig()
	cfg.M = 16
	cfg.EfConstruction = 100
	cfg.Dims = dims
	// Ensure Bulk Threshold is met (default 1000)

	idx := NewArrowHNSW(ds, cfg, nil)
	defer func() { _ = idx.Close() }()

	// 1. Create Batch
	startGen := time.Now()
	rec := makeScaleTestRecord(mem, dims, count)
	defer rec.Release()
	t.Logf("Generated %d vectors in %v", count, time.Since(startGen))

	// 2. Prepare Inputs for AddBatch
	recs := []arrow.RecordBatch{rec}
	rowIdxs := make([]int, count)
	batchIdxs := make([]int, count)
	for i := 0; i < count; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0 // All in first batch
	}

	// 3. Ingest
	startIngest := time.Now()
	ids, err := idx.AddBatch(context.Background(), recs, rowIdxs, batchIdxs)
	require.NoError(t, err)
	ingestDur := time.Since(startIngest)
	t.Logf("Ingested %d vectors in %v (%.2f vec/s)", count, ingestDur, float64(count)/ingestDur.Seconds())

	// 4. Verification
	require.Equal(t, count, len(ids))
	require.Equal(t, count, idx.Len())

	// 5. Search correctness check
	// Search for ID=500, vector should match [500%100, 0...] -> [0, 0...] if 500%100==0
	// 500 % 100 = 0.
	// Let's pick 501: 501 % 100 = 1.
	targetID := 501
	targetVal := float32(targetID % 100) // 1.0

	qVec := make([]float32, dims)
	qVec[0] = targetVal

	// Perform Search
	res, err := idx.SearchVectors(context.Background(), qVec, 10, nil, SearchOptions{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(res), 1)

	// We expect multiple matches because modulo 100 implies 300 duplicates for each value.
	// But ID 501 should be among them or close.
	// Actually all vectors with i%100 == 1 are IDENTICAL.
	// Distance between identical vectors is 0.
	// So we should find one of them.

	found := false
	for _, r := range res {
		// checking exact match
		_ = r.Score
		// Check against expected pattern
		// To be rigorous, retrieving the vector for result ID would be better checking logic.
		// But simpler: just assert distance is near 0
		if r.Score < 0.0001 { // Assuming L2 distance
			found = true
			break
		}
	}
	// Distance metric default is L2.
	// SearchVectors result Score IS distance for L2.

	require.True(t, found, "Should find exact match (distance ~0)")
}
