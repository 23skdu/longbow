package store

import (
	"math/rand"
	"testing"
	"time"

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
	ds := &Dataset{
		Name:   "bulk_test",
		Schema: schema,
	}

	config := DefaultArrowHNSWConfig()
	config.M = 16
	config.EfConstruction = 64
	config.Dims = dims
	config.DataType = VectorTypeFloat32
	config.InitialCapacity = numVecs

	idx := NewArrowHNSW(ds, config, NewChunkedLocationStore())
	defer idx.Close()

	// 4. Perform Bulk Insert
	// AddBatch expects []RecordBatch
	rowIdxs := make([]int, numVecs)
	batchIdxs := make([]int, numVecs)
	for i := 0; i < numVecs; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0 // All from first batch
	}

	start := time.Now()
	// Need to populate h.vectorColIdx or let it discover?
	// AddBatch discovers if < 0. defaulted in NewArrowHNSW to -1.

	ids, err := idx.AddBatch([]arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	require.NoError(t, err)
	require.Len(t, ids, numVecs)

	duration := time.Since(start)
	t.Logf("Bulk Inserted %d vectors in %v (%.2f vec/s)", numVecs, duration, float64(numVecs)/duration.Seconds())

	// 5. Verify Integrity
	require.Equal(t, numVecs, idx.Len())

	// 6. Verify Search (Self-Recall)
	// Sample 20 random vectors
	for k := 0; k < 20; k++ {
		i := rng.Intn(numVecs)
		query := vectors[i]

		res, err := idx.Search(query, 10, 100, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res)

		// Check if we found ourselves (distance ~0)
		found := false
		for _, c := range res {
			if int(c.ID) == i {
				found = true
				require.InDelta(t, 0.0, c.Score, 1e-4)
				break
			}
		}
		require.True(t, found, "Vector %d not found in search results", i)
	}
}
