package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestZeroCopyRecordBatch(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f1", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	original := b.NewRecordBatch()

	// Create independent ref counted batch
	zeroCopy, err := ZeroCopyRecordBatch(pool, original, nil)
	assert.NoError(t, err)

	// Original can be released, zeroCopy should still be valid
	original.Release()

	assert.Equal(t, int64(3), zeroCopy.NumRows())
	assert.Equal(t, int64(1), zeroCopy.NumCols())
	// Clean up
	zeroCopy.Release()
}

func TestRetainRecordBatch(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f1", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(10)

	original := b.NewRecordBatch()
	defer original.Release()

	// Should increment ref count
	RetainRecordBatch(original)
	original.Release() // Release one reference

	// Batch should still be valid (we don't have easy way to check ref count directly via public API easily without unsafe,
	// but usage shouldn't panic)
	assert.Equal(t, int64(1), original.NumRows())

	// Release second reference
	original.Release()
}
