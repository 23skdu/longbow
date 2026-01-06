package storage

import (
	"encoding/binary"
	"io"
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

func TestWALBatcher_Compression_Direct(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &WALBatcherConfig{
		FlushInterval:  100 * time.Millisecond,
		MaxBatchSize:   5,
		WALCompression: true,
	}

	batcher := NewWALBatcher(tmpDir, cfg)
	require.NoError(t, batcher.Start())
	defer func() { _ = batcher.Stop() }()

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
		_ = batcher.Write(rec, "test_ds", uint64(i), time.Now().UnixNano())
	}

	// Wait for flush or Stop flushes
	_ = batcher.Stop()

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

	// Now try to replay it
	it, err := NewWALIterator(tmpDir, pool)
	require.NoError(t, err)
	defer it.Close()

	count := 0
	for {
		_, _, _, r, err := it.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		assert.Equal(t, int64(3), int64(r.NumRows()))
		r.Release()
		count++
	}
	assert.Equal(t, 10, count)
}
