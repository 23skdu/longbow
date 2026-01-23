package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHNSWZeroCopyAccess verifies the unsafe zero-copy vector access path
func TestHNSWZeroCopyAccess(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()

	// Initialize VectorStore
	vs := NewVectorStore(mem, logger, 1024*1024*1024, 100*1024*1024, 0)
	defer func() { _ = vs.Close() }()

	// Define schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Create test data
	vectors := [][]float32{
		{1.1, 2.2, 3.3, 4.4},
		{5.5, 6.6, 7.7, 8.8},
	}

	b := array.NewFixedSizeListBuilder(mem, 4, arrow.PrimitiveTypes.Float32)
	defer b.Release()
	vb := b.ValueBuilder().(*array.Float32Builder)

	for _, v := range vectors {
		b.Append(true)
		vb.AppendValues(v, nil)
	}

	vecArr := b.NewArray()
	defer vecArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{vecArr}, int64(len(vectors)))
	defer rec.Release()

	// Store data
	ctx := context.Background()
	err := vs.StoreRecordBatch(ctx, "test_zc", rec)
	require.NoError(t, err)

	// Wait for data to become visible (Async Ingestion)
	var ds *Dataset
	require.Eventually(t, func() bool {
		var ok bool
		ds, ok = vs.getDataset("test_zc")
		if !ok {
			return false
		}
		ds.dataMu.RLock()
		defer ds.dataMu.RUnlock()
		return len(ds.Records) > 0
	}, 5*time.Second, 10*time.Millisecond, "Dataset should eventually have records")

	// Manually initialize HNSW index
	hnswIdx := NewHNSWIndex(ds)
	ds.dataMu.Lock()
	ds.Index = hnswIdx
	ds.dataMu.Unlock()

	// Index vectors
	for i := 0; i < len(vectors); i++ {
		_, err := hnswIdx.Add(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// Verify getVectorUnsafe
	t.Run("getVectorUnsafe_Correctness", func(t *testing.T) {
		// Test Vector 0
		vec0, release0 := hnswIdx.getVectorUnsafe(VectorID(0))
		require.NotNil(t, vec0)
		require.NotNil(t, release0)
		defer release0()

		assert.Len(t, vec0, 4)
		assert.Equal(t, float32(1.1), vec0[0])
		assert.Equal(t, float32(4.4), vec0[3])
		// Verify unsafe pointer logic (optional, but good for sanity)
		// We expect checks ideally implicitly done by accessing data

		// Test Vector 1
		vec1, release1 := hnswIdx.getVectorUnsafe(VectorID(1))
		require.NotNil(t, vec1)
		defer release1()
		assert.Equal(t, float32(5.5), vec1[0])
	})

	t.Run("getVectorUnsafe_OutOfBounds", func(t *testing.T) {
		vec, release := hnswIdx.getVectorUnsafe(VectorID(999))
		assert.Nil(t, vec)
		assert.Nil(t, release)
	})
}
