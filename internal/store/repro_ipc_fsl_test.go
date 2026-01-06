package store


import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestCastRecordToSchema_MissingFSL(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1024*1024*1024, 0, 0)

	// Schema A: int64
	fieldsA := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}
	schemaA := arrow.NewSchema(fieldsA, nil)

	// Schema B: int64 + FixedSizeList(2, float32)
	fieldsB := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "fsl_col", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32), Nullable: true},
	}
	schemaB := arrow.NewSchema(fieldsB, nil)

	// Create Batch A (Schema A)
	bldrA := array.NewRecordBuilder(mem, schemaA)
	bldrA.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	recA := bldrA.NewRecordBatch()
	defer recA.Release()

	// Cast A -> B (Should create missing FSL column)
	t.Log("Casting Schema A to Schema B (Missing FSL)...")
	out, err := castRecordToSchemaHelper(vs, recA, schemaB)
	require.NoError(t, err)
	defer out.Release()

	require.Equal(t, int64(3), out.NumRows())
	require.Equal(t, 2, int(out.NumCols()))

	fslCol := out.Column(1)
	require.Equal(t, arrow.FIXED_SIZE_LIST, fslCol.DataType().ID())
	require.True(t, fslCol.IsNull(0))
	require.True(t, fslCol.IsNull(1))
	require.True(t, fslCol.IsNull(2))

	// Verify Data Buffer Logic (CRITICAL)
	// Even if null, FSL array data buffer typically must have space or specific layout for IPC?
	// Let's test IPC write

	t.Log("Attempting IPC Write of casted record with Missing FSL...")
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schemaB), ipc.WithLZ4())
	err = w.Write(out)
	require.NoError(t, err, "IPC Write failed - suspect FSL child array issue")
	require.NoError(t, w.Close())
	t.Log("IPC Write successful")
}
