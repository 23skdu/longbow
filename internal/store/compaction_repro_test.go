package store


import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestAppendValue_Panic_Reproduction(t *testing.T) {
	mem := memory.NewGoAllocator()

	// 1. Mismatch FixedSizeList (Int64 instead of Float32)
	t.Run("mismatch_fixed_size_list_should_panic_or_fail", func(t *testing.T) {
		dt := arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int64)
		schema := arrow.NewSchema([]arrow.Field{{Name: "vec_int", Type: dt}}, nil)

		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		lb := b.Field(0).(*array.FixedSizeListBuilder)
		vb := lb.ValueBuilder().(*array.Int64Builder)

		lb.Append(true)
		vb.AppendValues([]int64{10, 20}, nil)

		rec := b.NewRecordBatch()
		defer rec.Release()

		// New builder
		b2 := array.NewRecordBuilder(mem, schema)
		defer b2.Release()

		// This call previously failed by skipping the row (length 0).
		// Now it should append the row correctly (length 1).
		appendValue(b2.Field(0), rec.Column(0), 0)

		assert.Equal(t, 1, b2.Field(0).Len(), "Fixed: appendValue correctly handled valid row for recursive FixedSizeList")
	})

	// 2. Unsupported Type (e.g. Time32)
	t.Run("unsupported_type_should_succeed", func(t *testing.T) {
		dt := arrow.FixedWidthTypes.Time32ms
		schema := arrow.NewSchema([]arrow.Field{{Name: "time", Type: dt}}, nil)

		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		b.Field(0).(*array.Time32Builder).Append(1000)

		rec := b.NewRecordBatch()
		defer rec.Release()

		b2 := array.NewRecordBuilder(mem, schema)
		defer b2.Release()

		appendValue(b2.Field(0), rec.Column(0), 0)

		assert.Equal(t, 1, b2.Field(0).Len(), "Fixed: appendValue correctly handled Time32")
	})
}
