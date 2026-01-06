package store


import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplication_ZeroCopy(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("map_type", func(t *testing.T) {
		// Map is a list of structs {key, value}
		dt := arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Float32)
		schema := arrow.NewSchema([]arrow.Field{{Name: "metadata", Type: dt}}, nil)

		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		mb := b.Field(0).(*array.MapBuilder)
		kb := mb.KeyBuilder().(*array.Int32Builder)
		ib := mb.ItemBuilder().(*array.Float32Builder)

		mb.Append(true)
		kb.Append(1)
		ib.Append(1.1)
		kb.Append(2)
		ib.Append(2.2)

		mb.Append(true)
		kb.Append(3)
		ib.Append(3.3)

		rec := b.NewRecordBatch()
		defer rec.Release()

		// Marshal
		payload, err := MarshalZeroCopy("test_map", rec)
		require.NoError(t, err)
		assert.Equal(t, int64(2), payload.RowCount)

		// Unmarshal
		rec2, err := UnmarshalZeroCopy(payload, mem)
		require.NoError(t, err)
		defer rec2.Release()

		assert.True(t, rec.Schema().Equal(rec2.Schema()))
		assert.Equal(t, rec.NumRows(), rec2.NumRows())

		// Deep check
		m1 := rec.Column(0).(*array.Map)
		m2 := rec2.Column(0).(*array.Map)
		assert.Equal(t, m1.Len(), m2.Len())
	})

	t.Run("struct_type", func(t *testing.T) {
		dt := arrow.StructOf(
			arrow.Field{Name: "f1", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
		)
		schema := arrow.NewSchema([]arrow.Field{{Name: "nested", Type: dt}}, nil)

		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		sb := b.Field(0).(*array.StructBuilder)
		f1b := sb.FieldBuilder(0).(*array.Int64Builder)
		f2b := sb.FieldBuilder(1).(*array.Float64Builder)

		sb.Append(true)
		f1b.Append(100)
		f2b.Append(100.1)

		rec := b.NewRecordBatch()
		defer rec.Release()

		payload, err := MarshalZeroCopy("test_struct", rec)
		require.NoError(t, err)

		rec2, err := UnmarshalZeroCopy(payload, mem)
		require.NoError(t, err)
		defer rec2.Release()

		assert.Equal(t, rec.NumRows(), rec2.NumRows())
	})
}
