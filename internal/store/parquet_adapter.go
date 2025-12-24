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

// GraphEdgeRecord represents a graph edge for Parquet serialization
type GraphEdgeRecord struct {
	Subject   uint32  `parquet:"subject"`
	Predicate string  `parquet:"predicate"`
	Object    uint32  `parquet:"object"`
	Weight    float32 `parquet:"weight"`
}

// writeGraphParquet writes a Graph Arrow record to a Parquet writer
func writeGraphParquet(w io.Writer, rec arrow.RecordBatch) error {
	pw := parquet.NewGenericWriter[GraphEdgeRecord](w, parquet.Compression(&parquet.Zstd))

	rows := int(rec.NumRows())

	// Expecting columns: subject (uint32), predicate (dictionary<string>), object (uint32), weight (float32)
	// Indices might vary, so finding by name is safer, but assuming standard schema from GraphStore

	// 0: subject
	subjCol := rec.Column(0).(*array.Uint32)

	// 1: predicate (Dictionary)
	predCol := rec.Column(1).(*array.Dictionary)
	predDict := predCol.Dictionary().(*array.String)
	predIndices := predCol.Indices().(*array.Uint16)

	// 2: object
	objCol := rec.Column(2).(*array.Uint32)

	// 3: weight
	weightCol := rec.Column(3).(*array.Float32)

	batch := make([]GraphEdgeRecord, rows)
	for i := 0; i < rows; i++ {
		predIdx := predIndices.Value(i)
		predStr := predDict.Value(int(predIdx))

		batch[i] = GraphEdgeRecord{
			Subject:   subjCol.Value(i),
			Predicate: predStr,
			Object:    objCol.Value(i),
			Weight:    weightCol.Value(i),
		}
	}

	_, err := pw.Write(batch)
	if err != nil {
		return err
	}
	return pw.Close()
}

// readGraphParquet reads a Graph Parquet file and converts it to an Arrow record
func readGraphParquet(f *os.File, size int64, mem memory.Allocator) (arrow.RecordBatch, error) {
	pf, err := parquet.OpenFile(f, size)
	if err != nil {
		return nil, err
	}

	pr := parquet.NewGenericReader[GraphEdgeRecord](pf)
	rows := make([]GraphEdgeRecord, pr.NumRows())
	_, err = pr.Read(rows)
	if err != nil && err != io.EOF {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	// Reconstruct Arrow RecordBatch
	// Need to rebuild Dictionary encoding for predicate to match GraphStore schema

	// 1. Collect unique predicates to build dictionary
	uniquePreds := make(map[string]uint16)
	nextIdx := uint16(0)
	// Preserve order of appearance or alphabetical? GraphStore order is insertion based usually
	// Let's just build it as we go

	var predIndices []uint16
	var dictValues []string

	for _, row := range rows {
		p := row.Predicate
		if idx, ok := uniquePreds[p]; ok {
			predIndices = append(predIndices, idx)
		} else {
			uniquePreds[p] = nextIdx
			predIndices = append(predIndices, nextIdx)
			dictValues = append(dictValues, p)
			nextIdx++
		}
	}

	// Schema
	md := arrow.NewMetadata(
		[]string{"longbow.entry_type"},
		[]string{"graph"},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "subject", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "predicate", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint16,
			ValueType: arrow.BinaryTypes.String,
		}},
		{Name: "object", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "weight", Type: arrow.PrimitiveTypes.Float32},
	}, &md)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	// Subject
	subjBuilder := b.Field(0).(*array.Uint32Builder)
	for _, row := range rows {
		subjBuilder.Append(row.Subject)
	}

	// Predicate - Build Dictionary Array manually
	dictBuilder := array.NewStringBuilder(mem)
	dictBuilder.AppendValues(dictValues, nil)
	dictArray := dictBuilder.NewStringArray()
	defer dictArray.Release()

	indicesBuilder := array.NewUint16Builder(mem)
	indicesBuilder.AppendValues(predIndices, nil)
	indicesArray := indicesBuilder.NewUint16Array()
	defer indicesArray.Release()

	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint16, ValueType: arrow.BinaryTypes.String}
	dictionaryArray := array.NewDictionaryArray(dt, indicesArray, dictArray)
	defer dictionaryArray.Release()

	// Object
	objBuilder := b.Field(2).(*array.Uint32Builder)
	for _, row := range rows {
		objBuilder.Append(row.Object)
	}

	// Weight
	weightBuilder := b.Field(3).(*array.Float32Builder)
	for _, row := range rows {
		weightBuilder.Append(row.Weight)
	}

	subjArray := subjBuilder.NewUint32Array()
	defer subjArray.Release()
	objArray := objBuilder.NewUint32Array()
	defer objArray.Release()
	weightArray := weightBuilder.NewFloat32Array()
	defer weightArray.Release()

	return array.NewRecordBatch(schema, []arrow.Array{
		subjArray,
		dictionaryArray,
		objArray,
		weightArray,
	}, int64(len(rows))), nil
}
