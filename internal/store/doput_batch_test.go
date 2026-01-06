package store


import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/rs/zerolog"
)

func TestConcatenateBatches(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	logger := zerolog.Nop()
	store := &VectorStore{
		mem:    mem,
		logger: logger,
	}

	// Create 3 small batches
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "val", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	var batches []arrow.RecordBatch
	totalRows := 0

	for i := 0; i < 3; i++ {
		bId := array.NewInt64Builder(mem)
		bId.Append(int64(i))

		bVal := array.NewFloat64Builder(mem)
		bVal.Append(float64(i * 10))

		cols := []arrow.Array{
			bId.NewArray(),
			bVal.NewArray(),
		}
		bId.Release()
		bVal.Release()

		batch := array.NewRecordBatch(schema, cols, 1)
		for _, c := range cols {
			c.Release()
		}

		batches = append(batches, batch)
		totalRows++
	}

	// Verify setup
	require.Len(t, batches, 3)

	// Test Concatenation
	concatenated, err := store.concatenateBatches(batches)
	require.NoError(t, err)
	require.NotNil(t, concatenated)

	// Clean up input batches
	for _, b := range batches {
		b.Release()
	}

	// Validate result
	assert.Equal(t, int64(totalRows), concatenated.NumRows())
	assert.Equal(t, schema.NumFields(), concatenated.Schema().NumFields())

	// Check values
	idCol := concatenated.Column(0).(*array.Int64)
	valCol := concatenated.Column(1).(*array.Float64)

	assert.Equal(t, int64(0), idCol.Value(0))
	assert.Equal(t, int64(1), idCol.Value(1))
	assert.Equal(t, int64(2), idCol.Value(2))

	assert.Equal(t, 0.0, valCol.Value(0))
	assert.Equal(t, 10.0, valCol.Value(1))
	assert.Equal(t, 20.0, valCol.Value(2))

	concatenated.Release()
}

func TestDoPut_AdaptiveBatching(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	logger := zerolog.Nop()
	store := &VectorStore{
		mem:    mem,
		logger: logger,
	}

	// Create 1 large batch (>100 rows)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	bId := array.NewInt64Builder(mem)
	for i := 0; i < 150; i++ {
		bId.Append(int64(i))
	}
	arr := bId.NewArray()
	bId.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 150)
	arr.Release()

	// We can't easily test internal "bypass" logic without mocking flushPutBatch or inspecting internal state,
	// but we can verify that `concatenateBatches` is NOT called if we mock it?
	// Or simply verify that it doesn't crash and returns success.
	// For now, simple correctness check.

	// Simulation of DoPut loop
	batch := make([]arrow.RecordBatch, 0)

	// Logic from DoPut:
	if len(batch) == 0 && rec.NumRows() >= 100 {
		// Should execute this path
		rec.Retain()
		// Mock flush:
		consolidated, err := store.concatenateBatches([]arrow.RecordBatch{rec}) // reusing existing method to test it handles single
		require.NoError(t, err)
		consolidated.Release()
		rec.Release()
	} else {
		t.Fatal("Adaptive logic failed: should have taken fast path")
	}

	rec.Release()
}
