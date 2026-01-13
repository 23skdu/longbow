//go:build go1.18

package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func FuzzArrow_Extraction(f *testing.F) {
	mem := memory.DefaultAllocator

	f.Add(int(arrow.FLOAT32), 128, true)
	f.Add(int(arrow.INT8), 64, false)
	f.Add(int(arrow.FLOAT16), 16, true)

	f.Fuzz(func(t *testing.T, typeID int, dims int, valid bool) {
		if dims <= 0 || dims > 3072 {
			return
		}

		tID := arrow.Type(typeID % 38) // Map to actual Arrow type IDs
		if tID == arrow.DICTIONARY || tID == arrow.EXTENSION || tID == arrow.SPARSE_UNION {
			return
		}

		// Create a mock RecordBatch with ONE column of type FixedSizeList
		var dt arrow.DataType
		switch tID {
		case arrow.INT8:
			dt = arrow.PrimitiveTypes.Int8
		case arrow.FLOAT32:
			dt = arrow.PrimitiveTypes.Float32
		case arrow.FLOAT16:
			dt = arrow.FixedWidthTypes.Float16
		case arrow.INT16:
			dt = arrow.PrimitiveTypes.Int16
		case arrow.UINT8:
			dt = arrow.PrimitiveTypes.Uint8
		default:
			return // Skip other types for now to keep builder simple
		}

		builder := array.NewFixedSizeListBuilder(mem, int32(dims), dt)
		defer builder.Release()

		// Fill some dummy data
		vb := builder.ValueBuilder()
		for i := 0; i < dims; i++ {
			// Just append zeros or dummy values based on type
			switch b := vb.(type) {
			case *array.Float32Builder:
				b.Append(0)
			case *array.Int8Builder:
				b.Append(0)
			}
		}
		builder.Append(valid)

		arr := builder.NewArray()
		defer arr.Release()

		schema := arrow.NewSchema([]arrow.Field{{Name: "vector", Type: arr.DataType()}}, nil)
		rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
		defer rec.Release()

		// Test extraction
		_, _ = ExtractVectorAny(rec, 0, 0)
		_, _ = ExtractVectorFromArrow(rec, 0, 0)
	})
}
