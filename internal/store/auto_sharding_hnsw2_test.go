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

func TestAutoShardingIndex_UseHNSW2(t *testing.T) {
	t.Setenv("LONGBOW_USE_HNSW2", "true")

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	ds := NewDataset("hnsw2_test", schema)
	require.True(t, ds.UseHNSW2(), "HNSW2 should be enabled by env var")

	config := DefaultAutoShardingConfig()
	config.IndexConfig = nil // Ensure we trigger default HNSW2 path

	idx := NewAutoShardingIndex(ds, config)

	// Verify underlying type
	// AutoShardingIndex struct fields are private, but we can check indirectly via behavior or interface
	// Actually we can access private fields in same package test
	_, ok := idx.current.(*ArrowHNSW)
	require.True(t, ok, "Underlying index should be ArrowHNSW")

	// Create a record batch with 4D vectors
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	// Add 3 orthogonal vectors
	idBuilder := b.Field(0).(*array.StringBuilder)
	vecBuilder := b.Field(1).(*array.FixedSizeListBuilder)
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	idBuilder.AppendValues([]string{"1", "2", "3"}, nil)

	// Vec 1: [1, 0, 0, 0]
	vecBuilder.Append(true)
	valBuilder.AppendValues([]float32{1, 0, 0, 0}, nil)

	// Vec 2: [0, 1, 0, 0]
	vecBuilder.Append(true)
	valBuilder.AppendValues([]float32{0, 1, 0, 0}, nil)

	// Vec 3: [0, 0, 1, 0]
	vecBuilder.Append(true)
	valBuilder.AppendValues([]float32{0, 0, 1, 0}, nil)

	rec := b.NewRecordBatch()
	ds.dataMu.Lock()
	rec.Retain() // Dataset owns one reference
	ds.Records = append(ds.Records, rec)
	ds.dataMu.Unlock()

	defer rec.Release() // Release local reference

	// Pre-set dimension (simulating what flushPutBatch does)
	idx.SetInitialDimension(4)

	// Ingest
	ids, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, []int{0, 1, 2}, []int{0, 0, 0})
	require.NoError(t, err)
	require.Len(t, ids, 3)

	// Search
	// Query close to Vec 1: [0.9, 0.1, 0, 0]
	q := []float32{0.9, 0.1, 0, 0}
	res, err := idx.SearchVectors(context.Background(), q, 10, nil, SearchOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, res)

	// ID 0 (which is "1") should be top result
	assert.Equal(t, uint32(ids[0]), uint32(res[0].ID))

	fmt.Printf("Top result ID: %d, Score: %f\n", res[0].ID, res[0].Score)
}
