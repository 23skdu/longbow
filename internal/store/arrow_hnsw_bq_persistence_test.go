package store

import (
	"os"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_BQ_Persistence(t *testing.T) {
	// 1. Setup Storage
	tmpDir, err := os.MkdirTemp("", "bq_persistence_test")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	config := storage.StorageConfig{
		DataPath:         tmpDir,
		SnapshotInterval: 10 * time.Minute, // Manual snapshot only
		WALCompression:   false,
	}

	mem := memory.NewGoAllocator()
	logger := zerolog.New(os.Stdout)
	store := NewVectorStore(mem, logger, 1024*1024*1024, 0, 0)
	err = store.InitPersistence(config)
	require.NoError(t, err)

	// 2. Create Dataset with BQ Enabled
	schemaName := "test_bq_dataset"
	dim := 16
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	// Create initial record to trigger dataset creation
	builder := array.NewRecordBuilder(mem, schema)
	listB := builder.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	// Add one vector
	listB.Append(true)
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = 1.0 // All positive -> BQ: all 1s
	}
	valB.AppendValues(vec, nil)

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Apply delta to create dataset
	err = store.ApplyDelta(schemaName, rec, 1, time.Now().UnixNano())
	require.NoError(t, err)

	// Wait for background indexing to complete before potentially modifying the index state or releasing rec
	// Although rec.Release is deferred, ApplyDelta starts an ingestion worker.
	store.WaitForIndexing(schemaName)

	// 3. Enable BQ Manually on the Index
	ds, err := store.GetDataset(schemaName)
	require.NoError(t, err)
	require.NotNil(t, ds)
	require.NotNil(t, ds.Index)

	// Check if we can cast to AutoShardingIndex
	asi, ok := ds.Index.(*AutoShardingIndex)
	require.True(t, ok, "Index should be AutoShardingIndex")

	// Create ArrowHNSW with BQ
	bqConfig := DefaultArrowHNSWConfig()
	bqConfig.BQEnabled = true
	arrowIndex := NewArrowHNSW(ds, bqConfig, nil)

	// Re-add data to this new index
	_, err = arrowIndex.AddByRecord(rec, 0, 0)
	require.NoError(t, err)

	// Replace in AutoShardingIndex via reflection or if accessible?
	// ASI.current is unexported but we are in package store
	asi.mu.Lock()
	asi.current = arrowIndex
	asi.sharded = false
	asi.mu.Unlock()

	// Verify BQ state before snapshot
	gd := arrowIndex.data.Load()
	require.NotNil(t, gd.VectorsBQ, "VectorsBQ should be allocated")
	require.True(t, arrowIndex.config.BQEnabled)

	// 4. Snapshot
	err = store.Snapshot()
	require.NoError(t, err)

	// 5. Close and Reopen
	_ = store.ClosePersistence()

	// Creating new store to verify restore
	store2 := NewVectorStore(mem, logger, 1024*1024*1024, 0, 0)
	err = store2.InitPersistence(config)
	require.NoError(t, err)
	defer func() { _ = store2.ClosePersistence() }()

	// 6. Verify Restore
	ds2, err := store2.GetDataset(schemaName)
	require.NoError(t, err)
	require.NotNil(t, ds2)
	require.NotNil(t, ds2.Index)

	// Check if BQEnabled is preserved
	asi2, ok := ds2.Index.(*AutoShardingIndex)
	require.True(t, ok)

	arrowIndex2, ok := asi2.current.(*ArrowHNSW)
	require.True(t, ok, "Restored index should be ArrowHNSW if persistence handled it correctly")

	assert.True(t, arrowIndex2.config.BQEnabled, "BQEnabled should be true after restore")

	if arrowIndex2.config.BQEnabled {
		t.Log("BQ Configuration successfully persisted!")
	}
}
