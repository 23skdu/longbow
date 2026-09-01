package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_Extended(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "engine-extended-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	mem := memory.NewGoAllocator()
	cfg := StorageConfig{
		DataPath:            tempDir,
		WALCompression:      true,
		SnapshotCompression: "zstd",
	}

	engine, err := NewStorageEngine(cfg, mem)
	require.NoError(t, err)
	defer engine.Close()

	err = engine.InitWAL()
	require.NoError(t, err)

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Uint32}}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Uint32Builder).AppendValues([]uint32{1, 2, 3}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	t.Run("CreateSnapshot_Full", func(t *testing.T) {
		item := &SnapshotItem{
			Name:    "test-ds",
			Records: []arrow.RecordBatch{rec},
		}
		err := engine.CreateSnapshot(item)
		assert.NoError(t, err)

		// Verify file exists
		snapshotPath := filepath.Join(tempDir, "snapshots_tmp", "test-ds.parquet")
		_, err = os.Stat(snapshotPath)
		assert.NoError(t, err)
	})

	t.Run("WALWrappers", func(t *testing.T) {
		err := engine.WriteWAL("test-ds", rec, 10, 12345)
		assert.NoError(t, err)

		err = engine.FlushWAL()
		assert.NoError(t, err)

		err = engine.TruncateWAL(5)
		assert.NoError(t, err)
	})

	t.Run("SnapshotBackend", func(t *testing.T) {
		// It's nil initially
		backend := &FileSnapshotBackend{baseDir: tempDir}
		engine.SetSnapshotBackend(backend)
		assert.Equal(t, backend, engine.GetSnapshotBackend())
	})

	t.Run("ErrCh", func(t *testing.T) {
		ch := engine.ErrCh()
		assert.NotNil(t, ch)
	})
}

func TestStorageEngine_writeSnapshotItem_Graph(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "engine-graph-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	mem := memory.NewGoAllocator()
	engine, _ := NewStorageEngine(StorageConfig{DataPath: tempDir}, mem)
	defer engine.Close()

	// GraphEdgeRecord schema: subject, predicate, object, weight
	predDictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint16, ValueType: arrow.BinaryTypes.String}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "subject", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "predicate", Type: predDictType},
		{Name: "object", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "weight", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	subBuilder := array.NewUint32Builder(mem)
	objBuilder := array.NewUint32Builder(mem)
	weightBuilder := array.NewFloat32Builder(mem)

	subBuilder.Append(1)
	objBuilder.Append(2)
	weightBuilder.Append(1.0)

	// Manual dictionary construction
	dictBuilder := array.NewStringBuilder(mem)
	dictBuilder.Append("neighbor")
	dict := dictBuilder.NewArray()
	defer dict.Release()

	indicesBuilder := array.NewUint16Builder(mem)
	indicesBuilder.Append(0)
	indices := indicesBuilder.NewArray()
	defer indices.Release()

	predArr := array.NewDictionaryArray(predDictType, indices, dict)
	defer predArr.Release()

	subArr := subBuilder.NewArray()
	defer subArr.Release()
	objArr := objBuilder.NewArray()
	defer objArr.Release()
	weightArr := weightBuilder.NewArray()
	defer weightArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{subArr, predArr, objArr, weightArr}, 1)
	defer rec.Release()

	item := &SnapshotItem{
		Name:         "test-graph",
		GraphRecords: []arrow.RecordBatch{rec},
	}

	err = engine.CreateSnapshot(item)
	assert.NoError(t, err)
}

func TestStorageEngine_Snapshot_Error(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "engine-err-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	mem := memory.NewGoAllocator()
	engine, _ := NewStorageEngine(StorageConfig{DataPath: tempDir}, mem)

	// Create records
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Uint32}}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Uint32Builder).AppendValues([]uint32{1}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	item := &SnapshotItem{
		Name:    "test-err",
		Records: []arrow.RecordBatch{rec},
	}

	// Make snapshots_tmp a file so directory creation fails
	tmpPath := filepath.Join(tempDir, "snapshots_tmp")
	err = os.WriteFile(tmpPath, []byte("not-a-dir"), 0644)
	require.NoError(t, err)

	err = engine.CreateSnapshot(item)
	assert.Error(t, err)

	engine.Close()
}

func TestStorageEngine_LoadSnapshots(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "engine-load-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	mem := memory.NewGoAllocator()
	engine, _ := NewStorageEngine(StorageConfig{DataPath: tempDir}, mem)
	defer engine.Close()

	// 1. Create a snapshot
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}, {Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)}}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int32Builder).Append(1)
	vB := builder.Field(1).(*array.FixedSizeListBuilder)
	vB.Append(true)
	vB.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{0.1, 0.2}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	item := &SnapshotItem{
		Name:    "coll1",
		Records: []arrow.RecordBatch{rec},
	}

	// CreateSnapshot writes to snapshots_tmp
	err = engine.CreateSnapshot(item)
	require.NoError(t, err)

	// Manually move snapshots_tmp to snapshots to simulate real Snapshot() call completion
	err = os.Rename(filepath.Join(tempDir, "snapshots_tmp"), filepath.Join(tempDir, "snapshots"))
	require.NoError(t, err)

	// 2. Load Snapshots
	count := 0
	err = engine.LoadSnapshots(func(item *SnapshotItem) error {
		count++
		assert.Equal(t, "coll1", item.Name)
		assert.Len(t, item.Records, 1)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestStorageEngine_NoBatcher(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "engine-nobatcher-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	mem := memory.NewGoAllocator()
	engine, _ := NewStorageEngine(StorageConfig{DataPath: tempDir}, mem)
	defer engine.Close()

	// Write without InitWAL (wal and walBatcher are nil) — should error
	err = engine.WriteToWAL("test", nil, 1, 1)
	assert.Error(t, err) // WAL not initialized is now an error (previously silent data loss)

	err = engine.SyncWAL()
	assert.NoError(t, err)
}
