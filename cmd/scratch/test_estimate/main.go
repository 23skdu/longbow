package main

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func estimateBatchSize(rec arrow.RecordBatch) int64 {
	if rec == nil {
		return 0
	}
	size := int64(0)
	for _, col := range rec.Columns() {
		for _, buf := range col.Data().Buffers() {
			if buf != nil {
				size += int64(buf.Len())
			}
		}
		for _, child := range col.Data().Children() {
			for _, buf := range child.Buffers() {
				if buf != nil {
					size += int64(buf.Len())
				}
			}
		}
	}
	return size
}

func main() {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(384, arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	vecB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < 2000; i++ {
		vecB.Append(true)
		for j := 0; j < 384; j++ {
			valB.Append(1.0)
		}
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	size := estimateBatchSize(rec)
	fmt.Printf("Size: %d\n", size)
	fmt.Printf("Avg: %d\n", size/2000)
}
