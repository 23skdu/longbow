package store

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParquetIngester(t *testing.T) {
	// 1. Setup a dummy dataset
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	ds := NewDataset("test_parquet", schema)

	// 2. Create a dummy parquet file
	tmpFile, err := os.CreateTemp("", "test_ingest_*.parquet")
	require.NoError(t, err)
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	// Use parquet-go directly to write a valid test file
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)

	pw := parquet.NewGenericWriter[DatasetParquetRecord](f)
	
	// Create some dummy records
	// Vector is []byte (4*4 bytes for 4 floats)
	records := []DatasetParquetRecord{
		{ID: 1, Vector: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{ID: 2, Vector: []byte{0, 0, 128, 63, 0, 0, 128, 63, 0, 0, 128, 63, 0, 0, 128, 63}}, // [1.0, 1.0, 1.0, 1.0]
	}
	
	_, err = pw.Write(records)
	require.NoError(t, err)
	err = pw.Close()
	require.NoError(t, err)
	f.Close()

	// 3. Ingest using ParquetIngester
	ingester := NewParquetIngester(ds, 10)
	total, err := ingester.Ingest(context.Background(), tmpPath)
	
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Equal(t, 1, len(ds.Records))
	assert.Equal(t, int64(2), ds.Records[0].NumRows())
}
