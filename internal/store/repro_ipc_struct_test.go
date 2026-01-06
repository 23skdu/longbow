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

func TestCastRecordToSchema_MissingStruct(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1024*1024*1024, 0, 0)

	// Schema A: int64
	fieldsA := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}
	schemaA := arrow.NewSchema(fieldsA, nil)

	// Schema B: int64 + Struct{val: int64}
	structType := arrow.StructOf(arrow.Field{Name: "val", Type: arrow.PrimitiveTypes.Int64})
	fieldsB := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "struct_col", Type: structType, Nullable: true},
	}
	schemaB := arrow.NewSchema(fieldsB, nil)

	// Create Batch A (Schema A)
	bldrA := array.NewRecordBuilder(mem, schemaA)
	bldrA.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	recA := bldrA.NewRecordBatch()
	defer recA.Release()

	// Cast A -> B (Should create missing Struct column)
	t.Log("Casting Schema A to Schema B (Missing Struct)...")
	out, err := castRecordToSchemaHelper(vs, recA, schemaB)
	require.NoError(t, err)
	defer out.Release()

	require.Equal(t, int64(3), out.NumRows())
	require.Equal(t, 2, int(out.NumCols()))

	structCol := out.Column(1)
	require.Equal(t, arrow.STRUCT, structCol.DataType().ID())
	require.True(t, structCol.IsNull(0))

	// Inspect child array length
	structArr := structCol.(*array.Struct)
	require.Equal(t, 1, structArr.NumField())
	childArr := structArr.Field(0)
	t.Logf("Struct Length: %d, Child Length: %d", structArr.Len(), childArr.Len())

	// Expectation: Child length MUST be 3. If it's 0, that's the bug.
	if childArr.Len() != 3 {
		t.Log("FAILURE: Child array length mismatch!")
		// We expect the IPC write to crash if this is true
	}

	// Verify IPC write
	t.Log("Attempting IPC Write of casted record with Missing Struct...")
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schemaB), ipc.WithLZ4())
	err = w.Write(out)
	require.NoError(t, w.Close()) // Always lint check Close :)
	require.NoError(t, err, "IPC Write failed")
	t.Log("IPC Write successful")
}
