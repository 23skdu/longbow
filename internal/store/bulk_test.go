package store

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestBulkDeferredConnections(t *testing.T) {
	// 1. Setup
	dims := 128
	numVecs := 2000 // > 1000 to trigger AddBatchBulk
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// 2. Generate Data
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Build Arrow RecordBatch
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	idBuilder := b.Field(0).(*array.Int32Builder)
	vecBuilder := b.Field(1).(*array.FixedSizeListBuilder)
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	vectors := make([][]float32, numVecs)

	for i := 0; i < numVecs; i++ {
		idBuilder.Append(int32(i))

		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = rng.Float32()
		}
		vectors[i] = vec

		vecBuilder.Append(true)
		valBuilder.AppendValues(vec, nil)
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	// 3. Initialize Index
	ds := NewDataset("bulk_test", schema)

	// Add batch to dataset so Index can resolve vectors during search
	rec.Retain()
	ds.Records.UpdateInPlace([]arrow.RecordBatch{rec})

	config := DefaultArrowHNSWConfig()
	config.M = 32
	config.EfConstruction = 100
	config.Dims = dims
	config.DataType = VectorTypeFloat32
	config.InitialCapacity = numVecs

	idx := NewArrowHNSW(ds, &config, nil)
	defer func() { _ = idx.Close() }()

	// 4. Perform Bulk Insert
	rowIdxs := make([]int, numVecs)
	batchIdxs := make([]int, numVecs)
	for i := 0; i < numVecs; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0 // All from first batch
	}

	start := time.Now()

	ids, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	require.NoError(t, err)
	require.Len(t, ids, numVecs)

	duration := time.Since(start)
	t.Logf("Bulk Inserted %d vectors in %v (%.2f vec/s)", numVecs, duration, float64(numVecs)/duration.Seconds())

	// 5. Verify Integrity
	require.Equal(t, numVecs, idx.Len())

	// 6. Verify Search (Self-Recall)
	// Sample 10 random vectors and verify most are found (HNSW is approximate)
	foundCount := 0
	searchCount := 10
	for k := 0; k < searchCount; k++ {
		i := rng.Intn(numVecs)
		query := vectors[i]

		res, err := idx.SearchVectors(context.Background(), query, 10, nil, types.SearchOptions{Ef: 500})
		require.NoError(t, err)
		require.NotEmpty(t, res)

		// Check if we found ourselves (distance ~0)
		for _, c := range res {
			if int(c.ID) == i && c.Distance < 1e-4 {
				foundCount++
				break
			}
		}
	}

	// Expect at least 80% recall (8 out of 10)
	require.GreaterOrEqual(t, foundCount, 8, "Expected at least 80%% recall, got %d/%d", foundCount, searchCount)
}
