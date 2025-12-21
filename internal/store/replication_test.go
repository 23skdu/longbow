package store

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplication_MarshalZeroCopy(t *testing.T) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "values", Type: arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Float32)},
	}, nil)

	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()
	listBuilder := array.NewFixedSizeListBuilder(mem, 3, arrow.PrimitiveTypes.Float32)
	defer listBuilder.Release()
	valueBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)

	idBuilder.Append(1)
	listBuilder.Append(true)
	valueBuilder.AppendValues([]float32{1.1, 2.2, 3.3}, nil)

	idBuilder.Append(2)
	listBuilder.Append(true)
	valueBuilder.AppendValues([]float32{4.4, 5.5, 6.6}, nil)

	rec := array.NewRecordBatch(schema, []arrow.Array{idBuilder.NewArray(), listBuilder.NewArray()}, 2)
	defer rec.Release()

	t.Run("Basic", func(t *testing.T) {
		payload, err := MarshalZeroCopy("test_dataset", rec)
		require.NoError(t, err)
		assert.Equal(t, "test_dataset", payload.DatasetName)
		// Now it's IPC bytes, so we can't just check string easily without a reader
		reader, err := ipc.NewReader(bytes.NewReader(payload.Schema), ipc.WithAllocator(mem))
		require.NoError(t, err)
		assert.Equal(t, rec.Schema().String(), reader.Schema().String())

		// Int64 col has 2 buffers (validity bitmap + data)
		// FixedSizeList col has 1 buffer (validity bitmap) + child data
		// Child Float32 array has 2 buffers (validity bitmap + data)
		// Total expected: 2 + 1 + 2 = 5 buffers
		assert.Equal(t, 5, len(payload.Buffers))

		// Verify data in the first buffer (id data)
		// Buffer 0 for Int64 is validity, Buffer 1 is data
		idData := payload.Buffers[1]
		assert.Equal(t, int64(1), int64(binary.LittleEndian.Uint64(idData[0:8])))
		assert.Equal(t, int64(2), int64(binary.LittleEndian.Uint64(idData[8:16])))
	})
}
