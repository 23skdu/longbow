package store

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func makeTestRecord(mem memory.Allocator, val int64) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).Append(val)
	return b.NewRecordBatch()
}

func makeBatchTestRecord(mem memory.Allocator, dims int, vectors [][]float32) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	vecBuilder := b.Field(0).(*array.FixedSizeListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for _, vec := range vectors {
		vecBuilder.Append(true)
		for _, v := range vec {
			floatBuilder.Append(v)
		}
	}

	return b.NewRecordBatch()
}
