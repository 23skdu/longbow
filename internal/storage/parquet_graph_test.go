package storage

import (
	"bytes"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphParquetRoundTrip(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Define Graph Schema
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

	// Create Data
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	subjBuilder := b.Field(0).(*array.Uint32Builder)
	subjBuilder.AppendValues([]uint32{1, 2, 3}, nil)

	// Dictionary predicate
	dictBuilder := array.NewStringBuilder(mem)
	dictBuilder.AppendValues([]string{"knows", "likes"}, nil)
	dictArr := dictBuilder.NewStringArray()
	defer dictArr.Release()

	indicesBuilder := array.NewUint16Builder(mem)
	indicesBuilder.AppendValues([]uint16{0, 1, 0}, nil)
	indicesArr := indicesBuilder.NewUint16Array()
	defer indicesArr.Release()

	dictionaryArr := array.NewDictionaryArray(schema.Field(1).Type.(*arrow.DictionaryType), indicesArr, dictArr)
	defer dictionaryArr.Release()

	// Manual assembly for dictionary column since RecordBuilder with Dictionary is tricky
	objBuilder := b.Field(2).(*array.Uint32Builder)
	objBuilder.AppendValues([]uint32{2, 3, 1}, nil)

	weightBuilder := b.Field(3).(*array.Float32Builder)
	weightBuilder.AppendValues([]float32{1.0, 0.5, 0.8}, nil)

	subjArr := subjBuilder.NewUint32Array()
	defer subjArr.Release()
	objArr := objBuilder.NewUint32Array()
	defer objArr.Release()
	weightArr := weightBuilder.NewFloat32Array()
	defer weightArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{
		subjArr,
		dictionaryArr,
		objArr,
		weightArr,
	}, 3)
	defer rec.Release()

	// Write to Buffer
	var buf bytes.Buffer
	err := writeGraphParquet(&buf, "lz4", rec)
	require.NoError(t, err)

	// Write to Temp File for readGraphParquet
	tmpFile, err := os.CreateTemp("", "graph_test_*.parquet")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(buf.Bytes())
	require.NoError(t, err)
	err = tmpFile.Sync()
	require.NoError(t, err)

	// Read Back
	readRec, err := readGraphParquet(tmpFile, int64(buf.Len()), mem)
	require.NoError(t, err)
	defer readRec.Release()

	// Verify
	assert.Equal(t, rec.NumRows(), readRec.NumRows())
	assert.Equal(t, rec.Schema().String(), readRec.Schema().String())

	// Check values
	for i := 0; i < int(rec.NumRows()); i++ {
		assert.Equal(t, subjArr.Value(i), readRec.Column(0).(*array.Uint32).Value(i))
		assert.Equal(t, objArr.Value(i), readRec.Column(2).(*array.Uint32).Value(i))
		assert.Equal(t, weightArr.Value(i), readRec.Column(3).(*array.Float32).Value(i))
	}
}

func TestReadGraphParquet_Empty(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create an empty parquet file for GraphEdgeRecord
	tmpFile, err := os.CreateTemp("", "empty_graph_*.parquet")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	pw := parquet.NewGenericWriter[GraphEdgeRecord](tmpFile)
	err = pw.Close()
	require.NoError(t, err)

	stat, _ := tmpFile.Stat()
	readRec, err := readGraphParquet(tmpFile, stat.Size(), mem)
	require.NoError(t, err)
	assert.Nil(t, readRec)
}
