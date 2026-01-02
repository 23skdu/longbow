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

// Helper to access castRecordToSchema since it's unexported (test is in same package)
func castRecordToSchemaHelper(s *VectorStore, rec arrow.RecordBatch, target *arrow.Schema) (arrow.RecordBatch, error) {
	return castRecordToSchema(s.mem, rec, target)
}

func TestCastRecordToSchema_EdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1024*1024*1024, 0, 0)

	fields := []arrow.Field{
		{Name: "col1", Type: arrow.PrimitiveTypes.Int64},
	}
	schema := arrow.NewSchema(fields, nil)

	// Target schema with an extra column
	fieldsTarget := make([]arrow.Field, len(fields))
	copy(fieldsTarget, fields)
	fieldsTarget = append(fieldsTarget, arrow.Field{Name: "new_col", Type: arrow.PrimitiveTypes.Int64})
	targetSchema := arrow.NewSchema(fieldsTarget, nil)

	// Case 1: Zero-length record batch
	bldr := array.NewRecordBuilder(mem, schema)
	recZero := bldr.NewRecordBatch() // 0 rows
	defer recZero.Release()

	t.Log("Testing Cast on Zero-Row RecordBatch...")
	outZero, err := castRecordToSchemaHelper(vs, recZero, targetSchema)
	require.NoError(t, err)
	defer outZero.Release()

	require.Equal(t, int64(0), outZero.NumRows())
	require.Equal(t, 2, int(outZero.NumCols()))

	// Verify IPC write
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(targetSchema), ipc.WithLZ4())
	err = w.Write(outZero)
	require.NoError(t, err, "IPC Write of zero-length record failed")
	require.NoError(t, w.Close())

	// Case 2: Sliced RecordBatch (offset non-zero)
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	recFull := bldr.NewRecordBatch()
	defer recFull.Release()

	recSlice := recFull.NewSlice(2, 5) // Rows 2, 3, 4 (length 3)
	defer recSlice.Release()

	t.Log("Testing Cast on Sliced RecordBatch...")
	outSlice, err := castRecordToSchemaHelper(vs, recSlice, targetSchema)
	require.NoError(t, err)
	defer outSlice.Release()

	require.Equal(t, int64(3), outSlice.NumRows())
	// Verify data
	col0 := outSlice.Column(0).(*array.Int64)
	require.Equal(t, int64(3), col0.Value(0))

	// Verify IPC write of slice
	var bufSlice bytes.Buffer
	wSlice := ipc.NewWriter(&bufSlice, ipc.WithSchema(targetSchema), ipc.WithLZ4())
	err = wSlice.Write(outSlice)
	require.NoError(t, err, "IPC Write of sliced record failed")
	require.NoError(t, wSlice.Close())
}
