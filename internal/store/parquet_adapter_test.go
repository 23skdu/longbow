package store

import (
"bytes"
"os"
"path/filepath"
"testing"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
)

// makeParquetTestRecord creates a record compatible with parquet_adapter.go
// Uses Int32 for ID (matching VectorRecord struct) and FixedSizeList<Float32> for vectors
func makeParquetTestRecord(mem memory.Allocator, numRows, vecDim int) arrow.RecordBatch {
schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int32},
{Name: "vector", Type: arrow.FixedSizeListOf(int32(vecDim), arrow.PrimitiveTypes.Float32)},
}, nil)

idBuilder := array.NewInt32Builder(mem)
listBuilder := array.NewFixedSizeListBuilder(mem, int32(vecDim), arrow.PrimitiveTypes.Float32)
vecBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)

for i := 0; i < numRows; i++ {
idBuilder.Append(int32(i))
listBuilder.Append(true)
for j := 0; j < vecDim; j++ {
vecBuilder.Append(float32(i) + float32(j)*0.01)
}
}

return array.NewRecordBatch(schema, []arrow.Array{idBuilder.NewArray(), listBuilder.NewArray()}, int64(numRows))
}

// TestParquetRoundTrip_BasicIntegrity tests write -> read preserves data
func TestParquetRoundTrip_BasicIntegrity(t *testing.T) {
mem := memory.NewGoAllocator()
tmpDir := t.TempDir()

// Create test record: 100 rows, 128-dim vectors
rec := makeParquetTestRecord(mem, 100, 128)
defer rec.Release()

// Write to parquet file
path := filepath.Join(tmpDir, "test.parquet")
f, err := os.Create(path)
if err != nil {
t.Fatalf("Failed to create file: %v", err)
}

if err := writeParquet(f, rec); err != nil {
_ = f.Close()
t.Fatalf("writeParquet failed: %v", err)
}
_ = f.Close()

// Read back
f2, err := os.Open(path)
if err != nil {
t.Fatalf("Failed to open file: %v", err)
}
defer func() { _ = f2.Close() }()

stat, _ := f2.Stat()
readRec, err := readParquet(f2, stat.Size(), mem)
if err != nil {
t.Fatalf("readParquet failed: %v", err)
}
if readRec == nil {
t.Fatal("readParquet returned nil record")
}
defer readRec.Release()

// Verify row count
if readRec.NumRows() != rec.NumRows() {
t.Errorf("Row count mismatch: got %d, want %d", readRec.NumRows(), rec.NumRows())
}

// Verify column count
if readRec.NumCols() != rec.NumCols() {
t.Errorf("Column count mismatch: got %d, want %d", readRec.NumCols(), rec.NumCols())
}

// Verify ID values
origIDs := rec.Column(0).(*array.Int32)
readIDs := readRec.Column(0).(*array.Int32)
for i := 0; i < int(rec.NumRows()); i++ {
if origIDs.Value(i) != readIDs.Value(i) {
t.Errorf("ID mismatch at row %d: got %d, want %d", i, readIDs.Value(i), origIDs.Value(i))
break
}
}

// Verify vector values (spot check)
origVecs := rec.Column(1).(*array.FixedSizeList)
readVecs := readRec.Column(1).(*array.FixedSizeList)
origVals := origVecs.ListValues().(*array.Float32).Float32Values()
readVals := readVecs.ListValues().(*array.Float32).Float32Values()

for i := 0; i < min(len(origVals), 1000); i++ {
if origVals[i] != readVals[i] {
t.Errorf("Vector value mismatch at index %d: got %f, want %f", i, readVals[i], origVals[i])
break
}
}
}

// TestParquetRoundTrip_SchemaPreservation verifies schema is preserved
func TestParquetRoundTrip_SchemaPreservation(t *testing.T) {
mem := memory.NewGoAllocator()
tmpDir := t.TempDir()

testCases := []struct {
name   string
vecDim int
}{
{"dim_4", 4},
{"dim_128", 128},
{"dim_768", 768},
{"dim_1536", 1536},
}

for _, tc := range testCases {
t.Run(tc.name, func(t *testing.T) {
rec := makeParquetTestRecord(mem, 10, tc.vecDim)
defer rec.Release()

path := filepath.Join(tmpDir, tc.name+".parquet")
f, _ := os.Create(path)
if err := writeParquet(f, rec); err != nil {
_ = f.Close()
t.Fatalf("writeParquet failed: %v", err)
}
_ = f.Close()

f2, _ := os.Open(path)
defer func() { _ = f2.Close() }()
stat, _ := f2.Stat()
readRec, err := readParquet(f2, stat.Size(), mem)
if err != nil {
t.Fatalf("readParquet failed: %v", err)
}
defer readRec.Release()

// Verify schema field types
origSchema := rec.Schema()
readSchema := readRec.Schema()

if origSchema.NumFields() != readSchema.NumFields() {
t.Errorf("Field count mismatch: got %d, want %d", readSchema.NumFields(), origSchema.NumFields())
}

// Verify vector dimension preserved
origVecType := origSchema.Field(1).Type.(*arrow.FixedSizeListType)
readVecType := readSchema.Field(1).Type.(*arrow.FixedSizeListType)
if origVecType.Len() != readVecType.Len() {
t.Errorf("Vector dim mismatch: got %d, want %d", readVecType.Len(), origVecType.Len())
}
})
}
}

// TestParquetRoundTrip_LargeDataset tests serialization of >100K vectors
func TestParquetRoundTrip_LargeDataset(t *testing.T) {
if testing.Short() {
t.Skip("Skipping large dataset test in short mode")
}

mem := memory.NewGoAllocator()
tmpDir := t.TempDir()

// 100K rows, 128-dim vectors (~51MB of vector data)
numRows := 100000
vecDim := 128

rec := makeParquetTestRecord(mem, numRows, vecDim)
defer rec.Release()

path := filepath.Join(tmpDir, "large.parquet")
f, err := os.Create(path)
if err != nil {
t.Fatalf("Failed to create file: %v", err)
}

if err := writeParquet(f, rec); err != nil {
_ = f.Close()
t.Fatalf("writeParquet failed: %v", err)
}
_ = f.Close()

// Verify file was written
stat, _ := os.Stat(path)
t.Logf("Large parquet file size: %d bytes", stat.Size())

// Read back and verify
f2, _ := os.Open(path)
defer func() { _ = f2.Close() }()
stat2, _ := f2.Stat()

readRec, err := readParquet(f2, stat2.Size(), mem)
if err != nil {
t.Fatalf("readParquet failed: %v", err)
}
defer readRec.Release()

if readRec.NumRows() != int64(numRows) {
t.Errorf("Row count mismatch: got %d, want %d", readRec.NumRows(), numRows)
}

// Spot check first and last IDs
ids := readRec.Column(0).(*array.Int32)
if ids.Value(0) != 0 {
t.Errorf("First ID mismatch: got %d, want 0", ids.Value(0))
}
if ids.Value(numRows-1) != int32(numRows-1) {
t.Errorf("Last ID mismatch: got %d, want %d", ids.Value(numRows-1), numRows-1)
}
}

// TestParquetCorruptedFile_Recovery tests handling of corrupted parquet files
func TestParquetCorruptedFile_Recovery(t *testing.T) {
mem := memory.NewGoAllocator()
tmpDir := t.TempDir()

t.Run("empty_file", func(t *testing.T) {
path := filepath.Join(tmpDir, "empty.parquet")
_ = os.WriteFile(path, []byte{}, 0o644)

f, _ := os.Open(path)
defer func() { _ = f.Close() }()

_, err := readParquet(f, 0, mem)
if err == nil {
t.Error("Expected error for empty file, got nil")
}
})

t.Run("truncated_file", func(t *testing.T) {
// Write valid parquet first
rec := makeParquetTestRecord(mem, 50, 64)
defer rec.Release()

path := filepath.Join(tmpDir, "truncated.parquet")
f, _ := os.Create(path)
_ = writeParquet(f, rec)
_ = f.Close()

// Truncate file to corrupt it
data, _ := os.ReadFile(path)
_ = os.WriteFile(path, data[:len(data)/2], 0o644)

f2, _ := os.Open(path)
defer func() { _ = f2.Close() }()
stat, _ := f2.Stat()

_, err := readParquet(f2, stat.Size(), mem)
if err == nil {
t.Error("Expected error for truncated file, got nil")
}
})

t.Run("garbage_data", func(t *testing.T) {
path := filepath.Join(tmpDir, "garbage.parquet")
_ = os.WriteFile(path, []byte("not a parquet file at all"), 0o644)

f, _ := os.Open(path)
defer func() { _ = f.Close() }()
stat, _ := f.Stat()

_, err := readParquet(f, stat.Size(), mem)
if err == nil {
t.Error("Expected error for garbage data, got nil")
}
})

t.Run("corrupted_middle", func(t *testing.T) {
// Write valid parquet first
rec := makeParquetTestRecord(mem, 100, 64)
defer rec.Release()

path := filepath.Join(tmpDir, "corrupted_middle.parquet")
f, _ := os.Create(path)
_ = writeParquet(f, rec)
_ = f.Close()

// Corrupt middle of file
data, _ := os.ReadFile(path)
middle := len(data) / 2
for i := middle; i < middle+100 && i < len(data); i++ {
data[i] = 0xFF
}
_ = os.WriteFile(path, data, 0o644)

f2, _ := os.Open(path)
defer func() { _ = f2.Close() }()
stat, _ := f2.Stat()

// May succeed or fail depending on where corruption is
// The test verifies we don't panic
_, _ = readParquet(f2, stat.Size(), mem)
// No assertion - just ensuring no panic
})
}

// TestParquetRoundTrip_EmptyRecord tests handling empty records
func TestParquetRoundTrip_EmptyRecord(t *testing.T) {
mem := memory.NewGoAllocator()
tmpDir := t.TempDir()

// Create empty record (0 rows)
rec := makeParquetTestRecord(mem, 0, 128)
defer rec.Release()

path := filepath.Join(tmpDir, "empty.parquet")
f, _ := os.Create(path)

// Write empty record - may succeed or fail, both are acceptable
err := writeParquet(f, rec)
_ = f.Close()

if err != nil {
t.Logf("writeParquet returned error for empty record (acceptable): %v", err)
return
}

// If write succeeded, verify read
f2, _ := os.Open(path)
defer func() { _ = f2.Close() }()
stat, _ := f2.Stat()

readRec, err := readParquet(f2, stat.Size(), mem)
if err != nil {
t.Logf("readParquet returned error for empty record (acceptable): %v", err)
return
}

if readRec != nil {
defer readRec.Release()
if readRec.NumRows() != 0 {
t.Errorf("Expected 0 rows, got %d", readRec.NumRows())
}
}
}

// TestParquetWriteToBuffer tests writing to in-memory buffer
func TestParquetWriteToBuffer(t *testing.T) {
mem := memory.NewGoAllocator()

rec := makeParquetTestRecord(mem, 50, 64)
defer rec.Release()

var buf bytes.Buffer
err := writeParquet(&buf, rec)
if err != nil {
t.Fatalf("writeParquet to buffer failed: %v", err)
}

if buf.Len() == 0 {
t.Error("Buffer is empty after write")
}

t.Logf("Parquet buffer size for 50 rows x 64 dims: %d bytes", buf.Len())
}
