package store

import (
"io"

"github.com/apache/arrow/go/v18/arrow"
"github.com/apache/arrow/go/v18/arrow/array"
"github.com/apache/arrow/go/v18/arrow/memory"
"github.com/parquet-go/parquet-go"
)

// VectorRecord represents a single row for Parquet serialization
type VectorRecord struct {
ID     int32     `parquet:"id"`
Vector []float32 `parquet:"vector"`
}

// writeParquet writes an Arrow record to a Parquet writer
func writeParquet(w io.Writer, rec arrow.Record) error {
pw := parquet.NewGenericWriter[VectorRecord](w)

// Iterate over rows and write
// This is not zero-copy but good for snapshots/cold storage
rows := rec.NumRows()
cols := rec.Columns()

// Assuming col 0 is ID (int32) and col 1 is Vector (FixedSizeList<float32>)
// We should probably check schema or find columns by name
// For now, let's assume the standard schema used in this repo

idCol := cols[0].(*array.Int32)
vecCol := cols[1].(*array.FixedSizeList)
vecData := vecCol.Data().Children()[0].(*array.Float32)
vecLen := int(vecCol.DataType().(*arrow.FixedSizeListType).Len())

batch := make([]VectorRecord, rows)
for i := int64(0); i < rows; i++ {
start := int(i) * vecLen
end := start + vecLen
batch[i] = VectorRecord{
ID:     idCol.Value(int(i)),
Vector: vecData.Float32Values()[start:end],
}
}

_, err := pw.Write(batch)
if err != nil {
return err
}
return pw.Close()
}

// readParquet reads a Parquet file and converts it to an Arrow record
func readParquet(r io.ReaderAt, size int64, mem memory.Allocator) (arrow.Record, error) {
pr := parquet.NewGenericReader[VectorRecord](r, size)
rows := make([]VectorRecord, pr.NumRows())
_, err := pr.Read(rows)
if err != nil && err != io.EOF {
return nil, err
}

// Convert back to Arrow Record
b := array.NewRecordBuilder(mem, arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int32},
{Name: "vector", Type: arrow.FixedSizeListOf(len(rows[0].Vector), arrow.PrimitiveTypes.Float32)},
},
nil,
))
defer b.Release()

idBuilder := b.Field(0).(*array.Int32Builder)
vecBuilder := b.Field(1).(*array.FixedSizeListBuilder)
vecValBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

for _, row := range rows {
idBuilder.Append(row.ID)
vecBuilder.Append(true)
vecValBuilder.AppendValues(row.Vector, nil)
}

return b.NewRecord(), nil
}
