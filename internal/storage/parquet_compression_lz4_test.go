package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/require"
)

func TestParquetCompression_LZ4(t *testing.T) {
	mem := memory.NewGoAllocator()
	tmpDir := t.TempDir()

	rec := makeParquetTestRecord(mem, 100, 128)
	defer rec.Release()

	path := filepath.Join(tmpDir, "lz4_compressed.parquet")
	f, err := os.Create(path)
	require.NoError(t, err)

	// Use LZ4
	err = writeParquet(f, "lz4", rec)
	require.NoError(t, err)
	_ = f.Close()

	// Verify
	f2, err := os.Open(path)
	require.NoError(t, err)
	defer f2.Close()

	stat, _ := f2.Stat()
	pf, err := parquet.OpenFile(f2, stat.Size())
	require.NoError(t, err)

	meta := pf.Metadata()
	require.NotEmpty(t, meta.RowGroups)
	rg := meta.RowGroups[0]
	require.NotEmpty(t, rg.Columns)

	codec := rg.Columns[0].MetaData.Codec
	// Note: parquet-go might use Lz4Raw
	require.Equal(t, format.Lz4Raw, codec, "Should use LZ4Raw compression")
}

func TestParquetCompression_Uncompressed(t *testing.T) {
	mem := memory.NewGoAllocator()
	tmpDir := t.TempDir()

	rec := makeParquetTestRecord(mem, 100, 128)
	defer rec.Release()

	path := filepath.Join(tmpDir, "uncompressed.parquet")
	f, err := os.Create(path)
	require.NoError(t, err)

	// Use uncompressed
	err = writeParquet(f, "uncompressed", rec)
	require.NoError(t, err)
	_ = f.Close()

	// Verify
	f2, err := os.Open(path)
	require.NoError(t, err)
	defer f2.Close()

	stat, _ := f2.Stat()
	pf, err := parquet.OpenFile(f2, stat.Size())
	require.NoError(t, err)

	meta := pf.Metadata()
	require.NotEmpty(t, meta.RowGroups)
	rg := meta.RowGroups[0]

	codec := rg.Columns[0].MetaData.Codec
	require.Equal(t, format.Uncompressed, codec, "Should be uncompressed")
}
