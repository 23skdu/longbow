package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAddBatch_Bulk_Typed verifies that AddBatch correctly handles bulk insertion
// for various data types, triggering the bulk path (n >= 1000).
func TestAddBatch_Bulk_Typed(t *testing.T) {
	pool := memory.NewGoAllocator()

	tests := []struct {
		desc     string
		dataType VectorDataType
		dims     int
	}{
		{"Int8", VectorTypeInt8, 16},
		{"Float64", VectorTypeFloat64, 8},
		{"Complex64", VectorTypeComplex64, 8}, // 8 complex = 16 float32 components
		{"Complex128", VectorTypeComplex128, 4},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			// Setup Index
			config := DefaultArrowHNSWConfig()
			config.M = 16
			config.EfConstruction = 100
			config.DataType = tt.dataType
			config.Dims = tt.dims

			idx := NewArrowHNSW(nil, config, nil)
			defer func() { _ = idx.Close() }()

			// Generate 1100 vectors to ensure Bulk Path (> 1000)
			numVecs := 1100

			// Build Record Batch
			builder := array.NewRecordBuilder(pool, arrow.NewSchema(
				[]arrow.Field{
					{Name: "vector", Type: arrow.FixedSizeListOf(int32(tt.dims), getArrowType(tt.dataType))},
				}, nil,
			))
			defer builder.Release()

			listB := builder.Field(0).(*array.FixedSizeListBuilder)

			// Populate Data
			for i := 0; i < numVecs; i++ {
				listB.Append(true)
				switch valB := listB.ValueBuilder().(type) {
				case *array.Int8Builder:
					for j := 0; j < tt.dims; j++ {
						// Encode i into first 2 bytes to ensure uniqueness
						val := int8(0)
						switch j {
						case 0:
							val = int8(i & 0xFF)
						case 1:
							val = int8((i >> 8) & 0xFF)
						}
						valB.Append(val)
					}
				case *array.Float64Builder:
					if tt.dataType == VectorTypeFloat64 {
						for j := 0; j < tt.dims; j++ {
							valB.Append(float64(i) + float64(j)*0.1)
						}
					} else {
						// Complex128
						for j := 0; j < tt.dims; j++ {
							valB.Append(float64(i) + float64(j)*0.1)       // Real
							valB.Append(float64(i) + float64(j)*0.1 + 0.5) // Imag
						}
					}
				case *array.Float32Builder:
					// Complex64
					for j := 0; j < tt.dims; j++ {
						valB.Append(float32(i) + float32(j)*0.1)       // Real
						valB.Append(float32(i) + float32(j)*0.1 + 0.5) // Imag
					}
				}
			}

			rec := builder.NewRecordBatch()
			defer rec.Release()

			// AddBatch
			rowIdxs := make([]int, numVecs)
			batchIdxs := make([]int, numVecs)
			for i := 0; i < numVecs; i++ {
				rowIdxs[i] = i
				batchIdxs[i] = 0 // Single batch
			}

			ids, err := idx.AddBatch([]arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
			require.NoError(t, err)
			assert.Len(t, ids, numVecs)
			assert.Equal(t, numVecs, idx.Len())

			// Verify Retrievablity of one vector
			qID := uint32(500)
			vecAny, err := idx.getVectorAny(qID)
			require.NoError(t, err)
			require.NotNil(t, vecAny)

			// Verify Type
			switch tt.dataType {
			case VectorTypeInt8:
				_, ok := vecAny.([]int8)
				assert.True(t, ok, "Expected []int8")
			case VectorTypeFloat64:
				_, ok := vecAny.([]float64)
				assert.True(t, ok, "Expected []float64")
			}

			// Verify Search (sanity check)
			// Search with the vector itself should return itself as top 1 (distance 0)
			res, err := idx.SearchVectors(context.Background(), vecAny, 10, nil, SearchOptions{})
			require.NoError(t, err)
			require.NotEmpty(t, res)
			assert.Equal(t, VectorID(qID), res[0].ID)
		})
	}
}

func getArrowType(dt VectorDataType) arrow.DataType {
	switch dt {
	case VectorTypeInt8:
		return arrow.PrimitiveTypes.Int8
	case VectorTypeFloat16:
		return arrow.FixedWidthTypes.Float16
	case VectorTypeFloat32:
		return arrow.PrimitiveTypes.Float32
	case VectorTypeFloat64:
		return arrow.PrimitiveTypes.Float64
	case VectorTypeComplex64:
		return arrow.PrimitiveTypes.Float32 // Logical mapping
	case VectorTypeComplex128:
		return arrow.PrimitiveTypes.Float64 // Logical mapping
	default:
		return arrow.PrimitiveTypes.Float32
	}
}
