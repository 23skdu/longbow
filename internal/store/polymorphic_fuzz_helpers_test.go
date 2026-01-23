package store

import (
	"math/rand"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

// Polymorphic types for fuzzing
const (
	FuzzTypeFloat32 = iota
	FuzzTypeFloat16
	FuzzTypeComplex64
	FuzzTypeComplex128
)

// GenerateRandomPolymorphicSchema creates a schema with a specific vector type
func GenerateRandomPolymorphicSchema(rng *rand.Rand, vecType, dims int) *arrow.Schema {
	fields := make([]arrow.Field, 2)
	fields[0] = arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64}

	var dtype arrow.DataType
	switch vecType {
	case FuzzTypeFloat16:
		dtype = arrow.FixedSizeListOf(int32(dims), arrow.FixedWidthTypes.Float16)
	case FuzzTypeComplex64:
		// Complex64 stored as 2x Float32
		dtype = arrow.FixedSizeListOf(int32(dims*2), arrow.PrimitiveTypes.Float32)
	case FuzzTypeComplex128:
		// Complex128 stored as 2x Float64 ? Or 16 bytes?
		// Typically Arrow doesn't have Complex128. We follow the established pattern.
		// If Longbow uses FixedSizeListOf(dims*2, Float64), use that.
		// Assuming 2x Float64 for now based on previous context.
		dtype = arrow.FixedSizeListOf(int32(dims*2), arrow.PrimitiveTypes.Float64)
	default:
		dtype = arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)
	}
	fields[1] = arrow.Field{Name: "vector", Type: dtype}

	return arrow.NewSchema(fields, nil)
}

// GenerateRandomPolymorphicBatch creates a batch for the given type
func GenerateRandomPolymorphicBatch(mem arrowmem.Allocator, rng *rand.Rand, vecType, dims, numRows int) arrow.RecordBatch {
	schema := GenerateRandomPolymorphicSchema(rng, vecType, dims)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	// ID Column
	ib := bldr.Field(0).(*array.Int64Builder)
	for i := 0; i < numRows; i++ {
		ib.Append(int64(i))
	}

	// Vector Column
	listB := bldr.Field(1).(*array.FixedSizeListBuilder)

	switch vecType {
	case FuzzTypeFloat16:
		vb := listB.ValueBuilder().(*array.Float16Builder)
		for i := 0; i < numRows; i++ {
			listB.Append(true)
			for j := 0; j < dims; j++ {
				vb.Append(float16.New(float32(rng.NormFloat64())))
			}
		}
	case FuzzTypeComplex64:
		vb := listB.ValueBuilder().(*array.Float32Builder)
		for i := 0; i < numRows; i++ {
			listB.Append(true)
			for j := 0; j < dims*2; j++ {
				vb.Append(float32(rng.NormFloat64()))
			}
		}
	case FuzzTypeComplex128:
		vb := listB.ValueBuilder().(*array.Float64Builder)
		for i := 0; i < numRows; i++ {
			listB.Append(true)
			for j := 0; j < dims*2; j++ {
				vb.Append(rng.NormFloat64())
			}
		}
	default: // Float32
		vb := listB.ValueBuilder().(*array.Float32Builder)
		for i := 0; i < numRows; i++ {
			listB.Append(true)
			for j := 0; j < dims; j++ {
				vb.Append(float32(rng.NormFloat64()))
			}
		}
	}

	return bldr.NewRecordBatch()
}
