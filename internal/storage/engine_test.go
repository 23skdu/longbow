package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_Lifecycle(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-engine-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := StorageConfig{
		DataPath:         tmpDir,
		DoPutBatchSize:   10,
		AsyncFsync:       false,
		SnapshotInterval: 1 * time.Hour,
	}
	mem := memory.NewGoAllocator()

	engine, err := NewStorageEngine(cfg, mem)
	require.NoError(t, err)
	require.NotNil(t, engine)

	err = engine.InitWAL()
	require.NoError(t, err)

	// Test WriteToWAL
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).Append(1)
	vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
	vecBuilder.Append(true)
	builder.Field(1).(*array.FixedSizeListBuilder).ValueBuilder().(*array.Float32Builder).AppendValues([]float32{0.1, 0.2, 0.3, 0.4}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	err = engine.WriteToWAL("test-collection", rec, 1, time.Now().UnixNano())
	require.NoError(t, err)

	err = engine.SyncWAL()
	require.NoError(t, err)

	pending, capacity := engine.GetWALQueueDepth()
	assert.Equal(t, 1000, capacity) // Default batcher queue cap is 100 * 100? No, NewWALBatcher has 10k hardcoded in NewWALBatcher line 97: make(chan WALEntry, config.MaxBatchSize*100)
	assert.LessOrEqual(t, pending, 1)

	// Test Close
	err = engine.Close()
	require.NoError(t, err)
}

type mockSnapshotSource struct {
	items []SnapshotItem
}

func (m *mockSnapshotSource) Iterate(f func(SnapshotItem) error) error {
	for _, item := range m.items {
		if err := f(item); err != nil {
			return err
		}
	}
	return nil
}

func TestStorageEngine_Snapshot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-snapshot-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := StorageConfig{
		DataPath: tmpDir,
	}
	mem := memory.NewGoAllocator()
	engine, _ := NewStorageEngine(cfg, mem)
	_ = engine.InitWAL()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int32Builder).Append(1)
	vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
	vecBuilder.Append(true)
	builder.Field(1).(*array.FixedSizeListBuilder).ValueBuilder().(*array.Float32Builder).AppendValues([]float32{0.1, 0.2, 0.3, 0.4}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()
	defer builder.Release()

	source := &mockSnapshotSource{
		items: []SnapshotItem{
			{
				Name:    "col1",
				Records: []arrow.RecordBatch{rec},
			},
		},
	}

	err = engine.Snapshot(source)
	require.NoError(t, err)

	// Verify snapshot directory
	_, err = os.Stat(filepath.Join(tmpDir, "snapshots", "col1.parquet"))
	assert.NoError(t, err)

	// Test LoadSnapshots
	count := 0
	err = engine.LoadSnapshots(func(item *SnapshotItem) error {
		count++
		assert.Equal(t, "col1", item.Name)
		assert.Len(t, item.Records, 1)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestStorageEngine_BackendSetters(t *testing.T) {
	engine := &StorageEngine{}
	backend := &FileSnapshotBackend{}
	
	engine.SetSnapshotBackend(backend)
	assert.Equal(t, backend, engine.GetSnapshotBackend())
}
