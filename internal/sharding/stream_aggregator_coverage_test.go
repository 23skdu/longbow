package sharding

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamAggregator_Internal_Coverage(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	sa := NewStreamAggregator(mem, logger)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
		{Name: "int64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "float64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "string", Type: arrow.BinaryTypes.String},
		{Name: "binary", Type: arrow.BinaryTypes.Binary},
		{Name: "fixed_binary", Type: &arrow.FixedSizeBinaryType{ByteWidth: 4}},
	}, nil)

	// Create a record batch
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	b.Field(1).(*array.Float32Builder).AppendValues([]float32{0.9, 0.8}, nil)
	b.Field(2).(*array.Int64Builder).AppendValues([]int64{100, 200}, nil)
	b.Field(3).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2}, nil)
	b.Field(4).(*array.StringBuilder).AppendValues([]string{"a", "b"}, nil)
	b.Field(5).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("bin1"), []byte("bin2")}, nil)
	b.Field(6).(*array.FixedSizeBinaryBuilder).AppendValues([][]byte{[]byte("1234"), []byte("5678")}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	t.Run("MergeAndSort_Basic", func(t *testing.T) {
		rec.Retain()
		res, err := sa.mergeAndSort([]arrow.RecordBatch{rec}, 10)
		require.NoError(t, err)
		require.Len(t, res, 1)
		assert.Equal(t, int64(2), res[0].NumRows())
		res[0].Release()
	})

	t.Run("MergeAndSort_Multiple", func(t *testing.T) {
		rec.Retain()
		rec.Retain()
		res, err := sa.mergeAndSort([]arrow.RecordBatch{rec, rec}, 1)
		require.NoError(t, err)
		require.Len(t, res, 1)
		assert.Equal(t, int64(1), res[0].NumRows())
		res[0].Release()
	})

	t.Run("SliceTable", func(t *testing.T) {
		tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
		defer tbl.Release()
		res, err := sa.sliceTable(tbl, 1)
		require.NoError(t, err)
		require.Len(t, res, 1)
		assert.Equal(t, int64(1), res[0].NumRows())
		res[0].Release()
	})

	t.Run("Aggregate_ErrorPaths", func(t *testing.T) {
		rm := NewRingManager("local", logger)
		fwdCfg := DefaultForwarderConfig()
		fwd := NewRequestForwarder(&fwdCfg, rm)
		sg := NewScatterGather(rm, fwd, logger)
		
		_, err := sa.Aggregate(context.Background(), sg, 10, func(ctx context.Context, nodeID string) (any, error) {
			return nil, nil
		})
		assert.NoError(t, err) // No members in ring yet, so scatter returns nil, nil
	})
	
	t.Run("AppendValue_Null", func(t *testing.T) {
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()
		arrB := array.NewInt32Builder(mem)
		arrB.AppendNull()
		a := arrB.NewArray()
		defer a.Release()
		
		err := appendValue(bldr, a, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, bldr.Len())
		assert.True(t, bldr.IsNull(0))
	})
}
