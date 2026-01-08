package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	qry "github.com/23skdu/longbow/internal/query"
)

func TestSearchVectorsCorrectness(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()

	// Initialize VectorStore
	vs := NewVectorStore(mem, logger, 1024*1024*1024, 100*1024*1024, 0)

	// Define schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Create test data
	vectors := [][]float32{
		{1, 0, 0, 0},
		{0, 1, 0, 0},
		{0, 0, 1, 0},
		{0, 0, 0, 1},
		{1, 1, 0, 0},
		{0, 1, 1, 0},
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
	err := vs.StoreRecordBatch(ctx, "test", rec)
	require.NoError(t, err)

	ds, ok := vs.getDataset("test")
	require.True(t, ok)
	require.NotNil(t, ds)

	// Manually initialize HNSW index
	hnswIdx := NewHNSWIndex(ds)
	ds.Index = hnswIdx

	// Manually index vectors
	for i := 0; i < len(vectors); i++ {
		_, _ = hnswIdx.Add(0, i) // Batch 0, Row i
		require.NoError(t, err)
	}

	query := []float32{1, 0.1, 0, 0}

	// Test SearchVectors (Batched)
	results, err := hnswIdx.SearchVectors(query, 2, nil)
	require.NoError(t, err)
	require.Len(t, results, 2)
	// Closer to [1,0,0,0] (ID 0) and [1,1,0,0] (ID 4)
	assert.Equal(t, VectorID(0), results[0].ID)
	assert.Equal(t, VectorID(4), results[1].ID)

	// Test SearchVectorsWithBitmap (Batched)
	filter := qry.NewBitset()
	filter.Set(1)
	filter.Set(2)
	filter.Set(3)

	resultsBitmap := hnswIdx.SearchVectorsWithBitmap(query, 2, filter)
	require.Len(t, resultsBitmap, 2)
	// ID 1 and 2 are closest among filtered {1, 2, 3}
	assert.Equal(t, VectorID(1), resultsBitmap[0].ID)
	assert.Equal(t, VectorID(2), resultsBitmap[1].ID)
}

func BenchmarkSearchVectorsBatched(b *testing.B) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	ds := NewDataset("bench", schema)
	idx := NewHNSWIndex(ds)
	ds.Index = idx
	idx.dims = 128

	// Add 10,000 vectors
	const n = 10000
	builder := array.NewFixedSizeListBuilder(mem, 128, arrow.PrimitiveTypes.Float32)
	defer builder.Release()
	vb := builder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < n; i++ {
		builder.Append(true)
		vec := make([]float32, 128)
		for j := 0; j < 128; j++ {
			vec[j] = float32(i + j)
		}
		vb.AppendValues(vec, nil)
	}

	vecArr := builder.NewArray()
	defer vecArr.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{vecArr}, int64(n))
	defer rec.Release()

	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	rec.Retain()
	ds.dataMu.Unlock()

	for i := 0; i < n; i++ {
		_, _ = idx.Add(0, i)
	}

	query := make([]float32, 128)
	for j := 0; j < 128; j++ {
		query[j] = 500.0
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = idx.SearchVectors(query, 100, nil)
	}
}

func BenchmarkSearchVectorsWithBitmapBatched(b *testing.B) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	ds := NewDataset("bench", schema)
	idx := NewHNSWIndex(ds)
	ds.Index = idx
	idx.dims = 128

	const n = 10000
	builder := array.NewFixedSizeListBuilder(mem, 128, arrow.PrimitiveTypes.Float32)
	defer builder.Release()
	vb := builder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < n; i++ {
		builder.Append(true)
		vec := make([]float32, 128)
		for j := 0; j < 128; j++ {
			vec[j] = float32(i + j)
		}
		vb.AppendValues(vec, nil)
	}

	vecArr := builder.NewArray()
	defer vecArr.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{vecArr}, int64(n))
	defer rec.Release()

	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	rec.Retain()
	ds.dataMu.Unlock()

	for i := 0; i < n; i++ {
		_, _ = idx.Add(0, i)
	}

	filter := qry.NewBitset()
	for i := 0; i < n; i += 2 {
		filter.Set(i)
	}

	query := make([]float32, 128)
	for j := 0; j < 128; j++ {
		query[j] = 500.0
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.SearchVectorsWithBitmap(query, 100, filter)
	}
}
