package store


import (
	"fmt"
	"math/rand"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// GenerateRandomSchema creates a schema with random fields
func GenerateRandomSchema(rng *rand.Rand) *arrow.Schema {
	numFields := rng.Intn(5) + 1 // 1 to 5 fields
	fields := make([]arrow.Field, numFields)

	// Always include an "id" field and "vector" field for meaningful tests
	fields[0] = arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64}

	// Create other fields
	for i := 1; i < numFields; i++ {
		name := fmt.Sprintf("col_%d", i)
		var dtype arrow.DataType

		switch rng.Intn(4) {
		case 0:
			dtype = arrow.PrimitiveTypes.Int64
		case 1:
			dtype = arrow.PrimitiveTypes.Float64
		case 2:
			dtype = arrow.BinaryTypes.String
		case 3:
			// Vector column
			dim := (rng.Intn(4) + 1) * 32 // 32, 64, 96, 128
			dtype = arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)
			name = "vector" // Name it vector specially
		}

		fields[i] = arrow.Field{Name: name, Type: dtype}
	}

	return arrow.NewSchema(fields, nil)
}

// GenerateRandomRecordBatch creates a batch matching the schema with random data
func GenerateRandomRecordBatch(mem memory.Allocator, rng *rand.Rand, schema *arrow.Schema, numRows int) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	for i, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64:
			fb := bldr.Field(i).(*array.Int64Builder)
			for j := 0; j < numRows; j++ {
				if rng.Intn(10) == 0 { // 10% null
					fb.AppendNull()
				} else {
					fb.Append(rng.Int63())
				}
			}
		case arrow.FLOAT64:
			fb := bldr.Field(i).(*array.Float64Builder)
			for j := 0; j < numRows; j++ {
				if rng.Intn(10) == 0 {
					fb.AppendNull()
				} else {
					fb.Append(rng.NormFloat64())
				}
			}
		case arrow.STRING:
			fb := bldr.Field(i).(*array.StringBuilder)
			for j := 0; j < numRows; j++ {
				if rng.Intn(10) == 0 {
					fb.AppendNull()
				} else {
					length := rng.Intn(20)
					bytes := make([]byte, length)
					rng.Read(bytes)
					fb.Append(string(bytes))
				}
			}
		case arrow.FIXED_SIZE_LIST:
			// Assumed vector (float32)
			fb := bldr.Field(i).(*array.FixedSizeListBuilder)
			vb := fb.ValueBuilder().(*array.Float32Builder)
			listSize := field.Type.(*arrow.FixedSizeListType).Len()

			for j := 0; j < numRows; j++ {
				fb.Append(true) // Not null list
				for k := 0; k < int(listSize); k++ {
					vb.Append(float32(rng.NormFloat64()))
				}
			}
		}
	}

	return bldr.NewRecordBatch()
}
