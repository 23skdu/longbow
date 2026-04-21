package sharding

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamAggregator_MergeAndSort(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "distance", Type: arrow.PrimitiveTypes.Float32},
		},
		nil,
	)

	// Result from Shard 1
	b1 := array.NewRecordBuilder(pool, schema)
	defer b1.Release()
	b1.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	b1.Field(1).(*array.Float32Builder).AppendValues([]float32{0.1, 0.5}, nil)
	rec1 := b1.NewRecord()
	defer rec1.Release()

	// Result from Shard 2
	b2 := array.NewRecordBuilder(pool, schema)
	defer b2.Release()
	b2.Field(0).(*array.Int64Builder).AppendValues([]int64{3, 4}, nil)
	b2.Field(1).(*array.Float32Builder).AppendValues([]float32{0.05, 0.2}, nil)
	rec2 := b2.NewRecord()
	defer rec2.Release()

	agg := NewStreamAggregator(pool, zerolog.Nop())
	
	inputs := []arrow.RecordBatch{rec1, rec2}
	// mergeAndSort takes ownership of inputs and releases them, so we retain if we want to use them later
	// but here we just pass them.
	rec1.Retain()
	rec2.Retain()

	final, err := agg.mergeAndSort(inputs, 10)
	require.NoError(t, err)
	require.Len(t, final, 1)
	defer final[0].Release()
	
	assert.Equal(t, int64(4), final[0].NumRows())
	
	// Verify sorting (Ascending distance: 0.05, 0.1, 0.2, 0.5)
	distances := final[0].Column(1).(*array.Float32).Float32Values()
	assert.Equal(t, []float32{0.05, 0.1, 0.2, 0.5}, distances)
}
