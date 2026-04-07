package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkFilterPushdown_Vs_PostFilter(b *testing.B) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	numRows := 10000
	categoryValues := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		categoryValues[i] = int64(i % 100)
	}

	b.Run("PostFilter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var matches int
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				if categoryValues[rowIdx] == 42 {
					matches++
				}
			}
		}
	})

	idx := NewColumnInvertedIndex()
	colIndex := []string{"category"}

	b.Run("Pushdown", func(b *testing.B) {
		rec := array.NewRecordBuilder(mem, schema)
		arr := rec.Field(1).(*array.Int64Builder)
		arr.AppendValues(categoryValues, nil)
		record := rec.NewRecord()
		defer record.Release()

		idx.IndexRecord("test", 0, record, colIndex)

		for i := 0; i < b.N; i++ {
			results := idx.Lookup("test", "category", "42")
			_ = results
		}
	})
}

func BenchmarkCompositeFilter_And(b *testing.B) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "status", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	numRows := 10000
	catValues := make([]int64, numRows)
	statusValues := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		catValues[i] = int64(i % 100)
		statusValues[i] = int64(i % 10)
	}

	rec := array.NewRecordBuilder(mem, schema)
	rec.Field(0).(*array.Int64Builder).AppendValues(catValues, nil)
	rec.Field(1).(*array.Int64Builder).AppendValues(statusValues, nil)
	record := rec.NewRecord()
	defer record.Release()

	idx := NewColumnInvertedIndex()
	idx.IndexRecord("test", 0, record, []string{"category", "status"})

	b.ResetTimer()
	b.Run("Separate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			catResults := idx.Lookup("test", "category", "42")
			statusResults := idx.Lookup("test", "status", "1")
			_ = len(catResults) + len(statusResults)
		}
	})
}
