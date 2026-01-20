package storage

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
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

// writeParquet writes one or more Arrow records to a Parquet writer.
// It uses a single parquet.Writer to ensure a valid file with one footer.
func writeParquet(w io.Writer, records ...arrow.RecordBatch) error {
	if len(records) == 0 {
		return nil
	}

	pw := parquet.NewGenericWriter[VectorRecord](w, parquet.Compression(&parquet.Zstd))
	defer func() {
		// Best effort close on early return
		_ = pw.Close()
	}()

	for _, rec := range records {
		rows := rec.NumRows()
		if rows == 0 {
			continue
		}

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

		if idColIdx == -1 {
			idColIdx = 0
		}
		if vecColIdx == -1 {
			vecColIdx = 1
		}

		if idColIdx >= int(rec.NumCols()) {
			return fmt.Errorf("id column index %d out of bounds (cols=%d)", idColIdx, rec.NumCols())
		}
		hasVec := vecColIdx < int(rec.NumCols())

		var ids []int32
		idCol := rec.Column(idColIdx)
		switch idArr := idCol.(type) {
		case *array.Int32:
			ids = idArr.Int32Values()
		case *array.Uint32:
			ids = make([]int32, rows)
			for i := 0; i < int(rows); i++ {
				ids[i] = int32(idArr.Value(i))
			}
		default:
			ids = make([]int32, rows)
			for i := 0; i < int(rows); i++ {
				ids[i] = int32(i)
			}
		}

		var vecData *array.Float32
		var vecDataF16 *array.Float16
		var vecLen int

		if hasVec {
			col := rec.Column(vecColIdx)
			if vecCol, ok := col.(*array.FixedSizeList); ok {
				elemType := vecCol.DataType().(*arrow.FixedSizeListType).Elem()
				vecLen = int(vecCol.DataType().(*arrow.FixedSizeListType).Len())

				switch elemType.ID() {
				case arrow.FLOAT16:
					vecDataF16 = vecCol.ListValues().(*array.Float16)
				case arrow.FLOAT32:
					vecData = vecCol.ListValues().(*array.Float32)
				case arrow.FLOAT64:
					// Float64 conversion is handled in the loop below,
					// as the target schema is Float32.
					// No direct assignment to vecData or vecDataF16 here.
				default:
					return fmt.Errorf("unsupported vector element type: %s", elemType)
				}
			} else {
				hasVec = false
			}
		}

		parquetRecords := make([]VectorRecord, rows)
		for i := int64(0); i < rows; i++ {
			var vec []float32
			if hasVec {
				start := int(i) * vecLen
				end := start + vecLen

				switch {
				case vecDataF16 != nil:
					vals := vecDataF16.Values()[start:end]
					vec = make([]float32, vecLen)
					for k, v := range vals {
						vec[k] = v.Float32()
					}
				case vecData != nil:
					vec = vecData.Float32Values()[start:end]
				default:
					// Must be Float64 or handled above
					col := rec.Column(vecColIdx).(*array.FixedSizeList)
					vals := col.ListValues().(*array.Float64).Float64Values()[start:end]
					vec = make([]float32, vecLen)
					for k, v := range vals {
						vec[k] = float32(v)
					}
				}
			}

			parquetRecords[i] = VectorRecord{
				ID:     ids[i],
				Vector: vec,
			}
		}

		if _, err := pw.Write(parquetRecords); err != nil {
			return err
		}
	}

	start := time.Now()
	err := pw.Close()
	if err == nil {
		metrics.SnapshotWriteDurationSeconds.Observe(time.Since(start).Seconds())
		if fi, ok := w.(interface{ Stat() (os.FileInfo, error) }); ok {
			if stat, err := fi.Stat(); err == nil {
				metrics.SnapshotSizeBytes.Observe(float64(stat.Size()))
			}
		}
	}
	return err
}

// readParquet reads a Parquet file and converts it to an Arrow record
func readParquet(f *os.File, size int64, mem memory.Allocator) (arrow.RecordBatch, error) {
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
	subjCol := rec.Column(0).(*array.Uint32)
	predCol := rec.Column(1).(*array.Dictionary)
	predDict := predCol.Dictionary().(*array.String)
	predIndices := predCol.Indices().(*array.Uint16)
	objCol := rec.Column(2).(*array.Uint32)
	weightCol := rec.Column(3).(*array.Float32)

	records := make([]GraphEdgeRecord, rows)
	for i := 0; i < rows; i++ {
		predIdx := predIndices.Value(i)
		predStr := predDict.Value(int(predIdx))

		records[i] = GraphEdgeRecord{
			Subject:   subjCol.Value(i),
			Predicate: predStr,
			Object:    objCol.Value(i),
			Weight:    weightCol.Value(i),
		}
	}

	start := time.Now()
	_, err := pw.Write(records)
	if err != nil {
		return err
	}
	err = pw.Close()
	if err == nil {
		metrics.SnapshotWriteDurationSeconds.Observe(time.Since(start).Seconds())
		if fi, ok := w.(interface{ Stat() (os.FileInfo, error) }); ok {
			if stat, err := fi.Stat(); err == nil {
				metrics.SnapshotSizeBytes.Observe(float64(stat.Size()))
			}
		}
	}
	return err
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

	uniquePreds := make(map[string]uint16)
	nextIdx := uint16(0)
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

	md := arrow.NewMetadata([]string{"longbow.entry_type"}, []string{"graph"})
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

	subjBuilder := b.Field(0).(*array.Uint32Builder)
	for _, row := range rows {
		subjBuilder.Append(row.Subject)
	}

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

	objBuilder := b.Field(2).(*array.Uint32Builder)
	for _, row := range rows {
		objBuilder.Append(row.Object)
	}

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
