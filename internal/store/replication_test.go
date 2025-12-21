package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestZeroCopyRoundTrip(t *testing.T) {
	pool := memory.NewGoAllocator()

	// 1. Build a complex RecordBatch
	// Schema: [ID: Int64, Vector: FixedSizeList<Float32>[4], Desc: String]
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
			{Name: "desc", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	// Add data
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)

	// Vector builder
	vb := b.Field(1).(*array.FixedSizeListBuilder)
	valBuilder := vb.ValueBuilder().(*array.Float32Builder)

	// Row 1: [1.1, 1.2, 1.3, 1.4]
	vb.Append(true)
	valBuilder.AppendValues([]float32{1.1, 1.2, 1.3, 1.4}, nil)

	// Row 2: [2.1, 2.2, 2.3, 2.4]
	vb.Append(true)
	valBuilder.AppendValues([]float32{2.1, 2.2, 2.3, 2.4}, nil)

	// Row 3: [3.1, 3.2, 3.3, 3.4]
	vb.Append(true)
	valBuilder.AppendValues([]float32{3.1, 3.2, 3.3, 3.4}, nil)

	// String builder
	sb := b.Field(2).(*array.StringBuilder)
	sb.AppendValues([]string{"one", "two", "three"}, nil)

	rec := b.NewRecordBatch()
	defer rec.Release()

	// 2. Marshal
	payload, err := MarshalZeroCopy("test_dataset", rec)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), payload.RowCount)
	assert.Equal(t, 3, len(payload.Meta)) // 3 columns

	// 3. Unmarshal
	rec2, err := UnmarshalZeroCopy(payload, pool)
	assert.NoError(t, err)
	defer rec2.Release()

	// 4. Verify Equality
	assert.True(t, rec.Schema().Equal(rec2.Schema()), "Schema should match")
	assert.Equal(t, rec.NumRows(), rec2.NumRows())
	for i := 0; i < int(rec.NumCols()); i++ {
		assert.True(t, array.Equal(rec.Column(i), rec2.Column(i)), "Column %d should match", i)
	}

	// Verify data access
	idCol := rec2.Column(0).(*array.Int64)
	assert.Equal(t, int64(1), idCol.Value(0))

	vecCol := rec2.Column(1).(*array.FixedSizeList)
	vecVals := vecCol.Data().Children()[0] // Underlying floats
	vecFloats := array.NewFloat32Data(vecVals)
	defer vecFloats.Release()

	assert.Equal(t, float32(1.1), vecFloats.Value(0))
	assert.Equal(t, float32(3.4), vecFloats.Value(11))
}
