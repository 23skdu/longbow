package storage

import (
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALBatcher_CompressionTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-compression-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	compressionTypes := []string{"snappy", "zstd", "lz4"}
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)

	for _, ct := range compressionTypes {
		t.Run(ct, func(t *testing.T) {
			cfg := WALBatcherConfig{
				MaxBatchSize:       10,
				FlushInterval:      10 * time.Millisecond,
				WALCompression:     true,
				WALCompressionType: ct,
			}
			w := NewWALBatcher(tmpDir, &cfg)
			err := w.Start()
			require.NoError(t, err)

			builder := array.NewRecordBuilder(mem, schema)
			builder.Field(0).(*array.Int32Builder).Append(1)
			rec := builder.NewRecordBatch()

			err = w.Write(rec, "test", 1, time.Now().UnixNano())
			require.NoError(t, err)
			rec.Release()
			builder.Release()

			err = w.Flush()
			require.NoError(t, err)

			err = w.Stop()
			require.NoError(t, err)
			
			// Clean up for next run
			os.RemoveAll(tmpDir)
			os.MkdirAll(tmpDir, 0750)
		})
	}
}

func TestWALBatcher_Adaptive(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-adaptive-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := WALBatcherConfig{
		MaxBatchSize:  10,
		FlushInterval: 100 * time.Millisecond,
		Adaptive: AdaptiveWALConfig{
			Enabled:     true,
			MinInterval: 1 * time.Millisecond,
			MaxInterval: 200 * time.Millisecond,
		},
	}
	w := NewWALBatcher(tmpDir, &cfg)
	err = w.Start()
	require.NoError(t, err)

	assert.True(t, w.IsAdaptiveEnabled())
	
	// Initial interval
	interval := w.GetCurrentInterval()
	assert.Greater(t, interval, time.Duration(0))

	err = w.Stop()
	require.NoError(t, err)
}

func TestWALBatcher_AsyncFsync(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-async-fsync-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := DefaultWALBatcherConfig()
	cfg.MaxBatchSize = 10
	cfg.AsyncFsync.Enabled = true
	w := NewWALBatcher(tmpDir, &cfg)
	err = w.Start()
	require.NoError(t, err)

	assert.True(t, w.IsAsyncFsyncEnabled())
	
	stats := w.AsyncFsyncStats()
	assert.NotNil(t, stats)

	err = w.Stop()
	require.NoError(t, err)
}

func TestWALBatcher_QueueFull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-full-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := DefaultWALBatcherConfig()
	cfg.MaxBatchSize = 1
	// Manually set a very small entries channel if we could, 
	// but it's hardcoded to MaxBatchSize * 100.
	// We'll just test that we can write.
	
	w := NewWALBatcher(tmpDir, &cfg)
	// We don't start it, so writes might fail once queue is full?
	// Actually Write() checks w.stopCh.
	
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int32Builder).Append(1)
	rec := builder.NewRecordBatch()
	defer rec.Release()
	defer builder.Release()

	// Writing without starting should still queue until full
	for i := 0; i < 100; i++ {
		_ = w.Write(rec, "test", uint64(i), 0)
	}
	
	pending, capacity := w.QueueStatus()
	assert.Equal(t, 100, pending)
	assert.Equal(t, 100, capacity)
}
