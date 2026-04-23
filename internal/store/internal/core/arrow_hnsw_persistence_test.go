package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestArrowHNSW_PersistenceRefactor(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Create MockDataset
	ds := &MockDataset{
		Name:    "test_persistence",
		Schema:  schema,
		Records: []arrow.RecordBatch{},
	}

	// Create Index
	cfg := types.DefaultArrowHNSWConfig()
	cfg.EfConstruction = 40
	cfg.M = 16
	idx := NewArrowHNSW(ds, &cfg, nil)
	idx.SetDimension(4)

	// Add Vectors
	ctx := context.Background()
	// Use simplified AddByLocation for test (mocking dataset presence via manual location store update if needed,
	// but AddByRecord is easier if we Mock the record)

	// Construct record
	b := array.NewFixedSizeListBuilder(pool, 4, arrow.PrimitiveTypes.Float32)
	vb := b.ValueBuilder().(*array.Float32Builder)

	count := 10
	for i := 0; i < count; i++ {
		b.Append(true)
		vb.AppendValues([]float32{float32(i), float32(i), float32(i), float32(i)}, nil)
	}
	rec := b.NewArray()
	rb := array.NewRecordBatch(schema, []arrow.Array{rec}, int64(count))
	defer rb.Release()
	defer rec.Release()
	defer b.Release()

	// Update dataset records so GetVector works (needed for validation)
	ds.Records = append(ds.Records, rb)
	rb.Retain() // MockDataset keeps a ref

	// Add
	for i := 0; i < count; i++ {
		_, err := idx.AddByRecord(ctx, rb, i, 0)
		assert.NoError(t, err)
	}

	// Verify Size
	assert.Equal(t, count, idx.Size())

	// Export State
	stateBytes, err := idx.ExportState()
	assert.NoError(t, err)
	assert.NotEmpty(t, stateBytes)

	// Create New Index and Import
	idx2 := NewArrowHNSW(ds, &cfg, nil)
	idx2.SetDimension(4)
	err = idx2.ImportState(stateBytes)
	assert.NoError(t, err)

	// Verify State
	assert.Equal(t, idx.Size(), idx2.Size())

	// Verify Vector presence via LocationStore (synced)
	// And verify types.GraphData logic (search)
	results, err := idx2.SearchVectors(ctx, []float32{0, 0, 0, 0}, 1, nil, nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, results)
	assert.Equal(t, types.VectorID(0), results[0].ID)

	// Check internal loc
	loc, ok := idx2.GetLocation(0)
	assert.True(t, ok)
	locO, ok := loc.(core.Location)
	assert.True(t, ok)
	assert.Equal(t, 0, locO.RowIdx)
}
