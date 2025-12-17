package store

import (
"os"
"path/filepath"
"testing"

"github.com/apache/arrow-go/v18/arrow/memory"
"github.com/parquet-go/parquet-go"
"github.com/parquet-go/parquet-go/format"
)

func TestParquetCompression_IsZSTD(t *testing.T) {
mem := memory.NewGoAllocator()
tmpDir := t.TempDir()

// Create a small record
rec := makeParquetTestRecord(mem, 100, 128)
defer rec.Release()

path := filepath.Join(tmpDir, "compressed.parquet")
f, err := os.Create(path)
if err != nil {
t.Fatalf("Failed to create file: %v", err)
}

if err := writeParquet(f, rec); err != nil {
_ = f.Close()
t.Fatalf("writeParquet failed: %v", err)
}
_ = f.Close()

// Open file to check metadata
f2, err := os.Open(path)
if err != nil {
t.Fatalf("Failed to open file: %v", err)
}
defer func() { _ = f2.Close() }()

stat, _ := f2.Stat()
pf, err := parquet.OpenFile(f2, stat.Size())
if err != nil {
t.Fatalf("Failed to open parquet file: %v", err)
}

// Check compression via FileMetaData
meta := pf.Metadata()
if len(meta.RowGroups) == 0 {
t.Fatal("No row groups found")
}

rg := meta.RowGroups[0]
if len(rg.Columns) == 0 {
t.Fatal("No columns found")
}

col := rg.Columns[0]
// MetaData is a struct, so we access it directly
codec := col.MetaData.Codec

if codec != format.Zstd {
t.Errorf("Expected ZSTD compression (%d), got %v", format.Zstd, codec)
}
}
