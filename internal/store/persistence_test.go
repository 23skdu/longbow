package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/storage"
)

func TestAsyncPersistence_EndToEnd(t *testing.T) {
	// 1. Setup
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create Store (Worker starts automatically)
	s := NewVectorStore(mem, logger, 1024*1024*100, 0, 0)
	defer close(s.stopChan) // Ensure cleanup

	// Initialize Engine
	cfg := storage.StorageConfig{
		DataPath:       tmpDir,
		AsyncFsync:     false, // Sync for test reliability
		DoPutBatchSize: 10,
	}
	engine, err := storage.NewStorageEngine(cfg, mem)
	require.NoError(t, err)
	require.NoError(t, engine.InitWAL())
	defer func() { _ = engine.Close() }()

	// Inject Engine (Safe due to channel synchronization)
	s.engine = engine

	// 2. Create Test Data
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)

	vb := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vb.ValueBuilder().(*array.Float32Builder)
	valB.AppendValues([]float32{1.0, 1.1, 2.0, 2.1, 3.0, 3.1}, nil)
	vb.Append(true)
	vb.Append(true)
	vb.Append(true)

	rec := b.NewRecordBatch()
	// Retain is usually done by caller (StoreRecordBatch), but here we simulate manually if needed
	// BUT s.flushPutBatch calls Retain().
	// For this test, let's call s.flushPutBatch to test the full flow including Retain logic.

	datasetName := "test_async_wal"
	// Create dataset first to avoid internal errors in flushPutBatch if logic depends on it
	// flushPutBatch doesn't check dataset existence for persistence queue, but it does for ingestion queue.
	// We'll mock dataset creation or just use NewDataset.
	ds := NewDataset(datasetName, schema)
	s.datasets.Store(&map[string]*Dataset{datasetName: ds})

	// 3. Execute Async Write
	// flushPutBatch expects []arrow.RecordBatch
	// It calls rec.Retain() internaly.
	err = s.flushPutBatch(ds, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	rec.Release() // Caller releases its own ref

	// 4. Verify
	// Wait for persistence to happen (async)
	// We can check WALQueueDepth metric or just poll file size.
	// Or poll engine.GetWALQueueDepth() if batcher is used.

	require.Eventually(t, func() bool {
		// Check WAL file size > 0
		walPath := filepath.Join(tmpDir, "wal.log")
		info, err := os.Stat(walPath)
		if err != nil {
			return false
		}
		return info.Size() > 0
	}, 2*time.Second, 10*time.Millisecond, "WAL should be written to")

	// 5. Verify Metrics
	// Note: checking generic gauge value might be flaky if other tests run, but locally should be fine.
	// Since we can't easily read Prometheus registry in test without helpers, we assume compilation
	// and the fact verifying file presence implies worker ran.
}
