package store

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"github.com/rs/zerolog"
)

func TestCastRecordToSchema_MissingDictionary(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1024, 0, 0)

	// Schema A: int64
	fieldsA := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}
	schemaA := arrow.NewSchema(fieldsA, nil)

	// Schema B: int64 + Dictionary(Index: Int32, Value: String)
	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
		Ordered:   false,
	}
	fieldsB := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "dict_col", Type: dictType, Nullable: true},
	}
	schemaB := arrow.NewSchema(fieldsB, nil)

	// Create Batch A (Schema A)
	bldrA := array.NewRecordBuilder(mem, schemaA)
	bldrA.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	recA := bldrA.NewRecordBatch()
	defer recA.Release()

	// Cast A -> B (Should create missing Dict column)
	t.Log("Casting Schema A to Schema B (Missing Dictionary)...")
	out, err := castRecordToSchemaHelper(vs, recA, schemaB)
	require.NoError(t, err)
	defer out.Release()

	require.Equal(t, int64(3), out.NumRows())
	require.Equal(t, 2, int(out.NumCols()))

	col1 := out.Column(1)
	require.Equal(t, arrow.DICTIONARY, col1.DataType().ID())
	require.True(t, col1.IsNull(0))

	// Check dictionary
	dictArr := col1.(*array.Dictionary)
	t.Logf("Dictionary Length: %d", dictArr.Dictionary().Len())
	// If Dictionary Length is 0, IPC might crash if it expects at least something?

	// Verify IPC write
	t.Log("Attempting IPC Write of casted record with Missing Dictionary...")
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schemaB), ipc.WithLZ4())

	defer func() {
		if r := recover(); r != nil {
			t.Logf("Caught Panic: %v", r)
			t.Fail()
		}
	}()

	err = w.Write(out)
	require.NoError(t, err, "IPC Write failed")
	require.NoError(t, w.Close())
	t.Log("IPC Write successful")
}
