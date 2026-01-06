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
