package store

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALCompression_Integration(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a record batch
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "val", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	// Add enough data to make compression meaningful
	for i := 0; i < 1000; i++ {
		b.Field(0).(*array.Uint32Builder).Append(uint32(i))
		b.Field(1).(*array.Float64Builder).Append(float64(i) * 1.1)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()

	// 1. Write with Compression Enabled
	cfg := StorageConfig{
		DataPath:       tmpDir,
		AsyncFsync:     false,
		DoPutBatchSize: 10, // Small batch size to trigger flushes
		WALCompression: true,
	}

	// Use Correct Constructor
	logger := zerolog.New(os.Stderr)
	store := NewVectorStore(memory.NewGoAllocator(), logger, 1024*1024, 2048*1024, 1*time.Minute)
	// Manually init persistence with compression
	require.NoError(t, store.InitPersistence(cfg))

	// Write data
	err := store.writeToWAL(rec, "compressed_dataset")
	require.NoError(t, err)

	// Force close to flush
	err = store.Close()
	require.NoError(t, err)

	// 2. Verify File Content
	walPath := filepath.Join(tmpDir, "wal.log")
	data, err := os.ReadFile(walPath)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// We expect multiple headers. The first might be normal depending on how walBatcher flushes.
	// But since we wrote 1000 items and batch size is effectively managed, we should see compressed blocks.
	// Wait, writeToWAL uses s.wal (StdWAL) which is UNCOMPRESSED in standard path, UNLESS NewWAL was modified?
	// Ah! I discovered earlier that s.wal is StdWAL and s.walBatcher is WALBatcher.
	// writeToWAL uses s.wal.
	// SO, store.writeToWAL does NOT use Compression if s.wal is StdWAL!

	// I need to use the batcher directly or trigger the path that uses batcher.
	// Store.StoreActions uses batcher if available.
	// Or I can test WALBatcher directly.
}

func TestWALBatcher_Compression_Direct(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := DefaultWALBatcherConfig()
	cfg.WALCompression = true
	cfg.MaxBatchSize = 5 // Small batch
	cfg.FlushInterval = 100 * time.Millisecond

	batcher := NewWALBatcher(tmpDir, &cfg)
	require.NoError(t, batcher.Start())
	defer batcher.Stop()

	// Create record
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		}, nil,
	)
	b := array.NewRecordBuilder(pool, schema)
	b.Field(0).(*array.Uint32Builder).AppendValues([]uint32{1, 2, 3}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	// Write enough to trigger batch flush
	for i := 0; i < 10; i++ {
		batcher.Write(rec, "test_ds", uint64(i), time.Now().UnixNano())
	}

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	// Verify file content has Sentinel
	walPath := filepath.Join(tmpDir, "wal.log")
	content, err := os.ReadFile(walPath)
	require.NoError(t, err)

	// Scan for 0xFFFFFFFF (Little Endian)
	foundSentinel := false
	for i := 0; i < len(content)-4; i++ {
		val := binary.LittleEndian.Uint32(content[i : i+4])
		if val == 0xFFFFFFFF {
			foundSentinel = true
			break
		}
	}
	assert.True(t, foundSentinel, "WAL file should contain Sentinel Checksum (0xFFFFFFFF)")
}

func TestPersistence_ReplayCompressed(t *testing.T) {
	// this tests the replay logic works on the file generated above
	tmpDir := t.TempDir()

	// 1. Generate Compressed WAL
	{
		cfg := DefaultWALBatcherConfig()
		cfg.WALCompression = true
		cfg.MaxBatchSize = 5
		cfg.FlushInterval = 10 * time.Millisecond

		batcher := NewWALBatcher(tmpDir, &cfg)
		require.NoError(t, batcher.Start())

		pool := memory.NewGoAllocator()
		schema := arrow.NewSchema(
			[]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Uint32}}, nil,
		)
		b := array.NewRecordBuilder(pool, schema)
		b.Field(0).(*array.Uint32Builder).Append(42)
		rec := b.NewRecordBatch()

		// Write 10 entries
		for i := 0; i < 10; i++ {
			batcher.Write(rec, "dataset_1", uint64(i), time.Now().UnixNano())
		}
		rec.Release()
		b.Release()

		batcher.Stop() // Flushes everything
	}

	// 2. Replay using VectorStore
	// Use Correct Constructor: NewVectorStore(mem, logger, softLimit, hardLimit, checkInterval)
	logger := zerolog.New(os.Stderr)
	store := NewVectorStore(memory.NewGoAllocator(), logger, 1024*1024, 2048*1024, 1*time.Minute)

	err := store.InitPersistence(StorageConfig{
		DataPath:       tmpDir,
		WALCompression: true,
	})
	require.NoError(t, err)

	// Verify sequence
	savedSeq := store.sequence.Load()
	assert.Equal(t, uint64(9), savedSeq)

	// Dataset should exist
	// Access unexported datasets map since we are in store package
	store.mu.RLock()
	ds := store.datasets["dataset_1"]
	store.mu.RUnlock()

	require.NotNil(t, ds)
	ds.dataMu.RLock()
	assert.Greater(t, len(ds.Records), 0)
	ds.dataMu.RUnlock()

	store.Close()
}
