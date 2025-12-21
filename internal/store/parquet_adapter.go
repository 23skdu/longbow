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
	// Find ID and Vector columns by name
	idColIdx := -1
	vecColIdx := -1
	for i, f := range rec.Schema().Fields() {
		switch f.Name {
		case "id":
			idColIdx = i
		case "vector":
			vecColIdx = i
		}
	}

	if idColIdx == -1 || vecColIdx == -1 {
		// If columns not found by name, fallback to 0 and 1 if types match
		idColIdx = 0
		vecColIdx = 1
	}

	var ids []int32
	idCol := rec.Column(idColIdx)
	if idArr, ok := idCol.(*array.Int32); ok {
		ids = idArr.Int32Values()
	} else if idArr, ok := idCol.(*array.Uint32); ok {
		// Convert Uint32 to Int32 for Parquet if needed, or change VectorRecord
		for i := 0; i < int(rec.NumRows()); i++ {
			ids = append(ids, int32(idArr.Value(i)))
		}
	} else {
		// fallback for other types?
		for i := 0; i < int(rec.NumRows()); i++ {
			ids = append(ids, int32(i))
		}
	}

	vecCol := rec.Column(vecColIdx).(*array.FixedSizeList)
	vecData := vecCol.ListValues().(*array.Float32)
	vecLen := int(vecCol.DataType().(*arrow.FixedSizeListType).Len())

	batch := make([]VectorRecord, rows)
	for i := int64(0); i < rows; i++ {
		start := int(i) * vecLen
		end := start + vecLen
		batch[i] = VectorRecord{
			ID:     ids[i],
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
