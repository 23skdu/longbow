package store

import (
	"context"
	"math/rand"
	"os"
	"testing"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatasetIO_ExportImportParquet(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
			{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	
	vb := b.Field(1).(*array.FixedSizeListBuilder)
	veb := vb.ValueBuilder().(*array.Float32Builder)
	for i := 0; i < 3; i++ {
		vb.Append(true)
		for j := 0; j < 4; j++ {
			veb.Append(rand.Float32())
		}
	}
	
	b.Field(2).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("m1"), []byte("m2"), []byte("m3")}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	// Setup VectorStore and Dataset
	vs := NewVectorStore(pool, zerolog.Nop(), 1024*1024*1024, 0, 0)
	defer vs.Close()

	datasetName := "test_ds"
	_, _ = vs.getOrCreateDataset(datasetName, func() *Dataset {
		return NewDataset(datasetName, schema)
	})

	ds, ok := vs.getDataset(datasetName)
	require.True(t, ok)
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	ds.dataMu.Unlock()

	// Setup Mock Backend
	tmpDir, err := os.MkdirTemp("", "longbow-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewFileSnapshotBackend(tmpDir)
	require.NoError(t, err)

	// Export
	total, err := vs.ExportDataset(datasetName, backend)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), total)

	// Import into a new dataset
	importName := "imported_ds"
	totalImported, err := vs.ImportDatasetFrom(context.Background(), datasetName, importName, backend, schema)
	require.NoError(t, err)
	require.Equal(t, int64(3), totalImported)

	// Verify data
	importedDS, ok := vs.getDataset(importName)
	require.True(t, ok)
	require.NotNil(t, importedDS)
	assert.Equal(t, 1, len(importedDS.Records))
	assert.Equal(t, int64(3), importedDS.Records[0].NumRows())
}
