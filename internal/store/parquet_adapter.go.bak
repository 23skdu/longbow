package store

import (
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/parquet-go/parquet-go"
)

// VectorRecord represents a single row for Parquet serialization
type VectorRecord struct {
	ID     int32     `parquet:"id"`
	Vector []float32 `parquet:"vector"`
}

// writeParquet writes an Arrow record to a Parquet writer
func writeParquet(w io.Writer, rec arrow.RecordBatch) error {
	pw := parquet.NewGenericWriter[VectorRecord](w, parquet.Compression(&parquet.Zstd))

	// Iterate over rows and write
	// This is not zero-copy but good for snapshots/cold storage
	rows := rec.NumRows()
	cols := rec.Columns()

	// Assuming col 0 is ID (int32) and col 1 is Vector (FixedSizeList<float32>)
	idCol := cols[0].(*array.Int32)
	vecCol := cols[1].(*array.FixedSizeList)

	// Fix: Use ListValues() to get the underlying flat array, then cast to *array.Float32
	vecData := vecCol.ListValues().(*array.Float32)
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
// Changed r from io.ReaderAt to *os.File to use parquet.OpenFile
func readParquet(f *os.File, size int64, mem memory.Allocator) (arrow.RecordBatch, error) {
	// Use parquet.OpenFile to correctly handle the file and size
	pf, err := parquet.OpenFile(f, size)
	if err != nil {
		return nil, err
	}

	pr := parquet.NewGenericReader[VectorRecord](pf)
	rows := make([]VectorRecord, pr.NumRows())
	_, err = pr.Read(rows)
	if err != nil && err != io.EOF {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	// Convert back to Arrow Record
	// Fix: Cast len() to int32 for FixedSizeListOf
	b := array.NewRecordBuilder(mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(len(rows[0].Vector)), arrow.PrimitiveTypes.Float32)},
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

	return b.NewRecordBatch(), nil
}
