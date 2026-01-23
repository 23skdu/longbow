package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FuzzExtractVectorGeneric fuzzes the ExtractVectorGeneric function
func FuzzExtractVectorGeneric(f *testing.F) {
	// Seed corpus with valid inputs
	// We can't easily seed complex Arrow structures in go-fuzz directly without serialization
	// So we'll fuzz the builder interactions or raw bytes if possible.
	// However, ExtractVectorGeneric takes a RecordBatch.
	// Fuzzing RecordBatch creation is complex.
	// Instead, we fuzz the input values used to build the batch.

	f.Add(int32(384), []byte("random data"))

	f.Fuzz(func(t *testing.T, dim int32, data []byte) {
		if dim <= 0 || dim > 1024 {
			return // skip invalid dims
		}

		mem := memory.NewGoAllocator()
		bld := array.NewFixedSizeListBuilder(mem, dim, arrow.PrimitiveTypes.Float32)
		defer bld.Release()

		// Try to build a vector from data
		vb := bld.ValueBuilder().(*array.Float32Builder)

		// Map bytes to floats
		floats := make([]float32, len(data)/4)
		for i := 0; i < len(floats); i++ {
			// crude conversion
			floats[i] = float32(data[i*4]) // just taking byte as value for simplicity
		}

		// Ensure we have enough for at least one vector or check handling
		if len(floats) < int(dim) {
			return
		}

		bld.Append(true)
		vb.AppendValues(floats[:dim], nil)

		arr := bld.NewArray()
		defer arr.Release()

		// Create RecordBatch
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(dim, arrow.PrimitiveTypes.Float32)},
		}, nil)

		batch := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
		defer batch.Release()

		// Extract
		vecs, err := ExtractVectorGeneric[float32](batch, 0, 0)
		if err != nil {
			// error is acceptable, panic is not
			return
		}

		if len(vecs) != int(dim) {
			t.Errorf("expected length %d, got %d", dim, len(vecs))
		}
	})
}
