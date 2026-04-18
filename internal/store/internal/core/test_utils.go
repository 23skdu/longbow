package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// GenerateTestVectors creates n vectors of the specified dimension
func GenerateTestVectors(n, dims int) [][]float32 {
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = float32(i) + float32(j)*0.1
		}
	}
	return vectors
}

// MakeBatchTestRecord builds an Arrow RecordBatch from a slice of vectors
func MakeBatchTestRecord(mem memory.Allocator, dims int, vectors [][]float32) arrow.RecordBatch {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		}, nil,
	)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	listBuilder := builder.Field(0).(*array.FixedSizeListBuilder)
	floatBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)

	for _, vec := range vectors {
		listBuilder.Append(true)
		for _, v := range vec {
			floatBuilder.Append(v)
		}
	}

	return builder.NewRecordBatch()
}

// NewTestHNSWIndex creates a standard HNSW index for testing
func NewTestHNSWIndex(ds types.IndexDataProvider) *ArrowHNSW {
	config := types.DefaultArrowHNSWConfig()
	config.M = 16
	config.EfConstruction = 100
	return NewArrowHNSW(ds, &config)
}
