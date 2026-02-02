package storage

import (
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALBatcher_MultiCompression(t *testing.T) {
	testCases := []struct {
		name string
		comp string
	}{
		{"Snappy", "snappy"},
		{"Zstd", "zstd"},
		{"LZ4", "lz4"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			cfg := &WALBatcherConfig{
				FlushInterval:      10 * time.Millisecond,
				MaxBatchSize:       2,
				WALCompression:     true,
				WALCompressionType: tc.comp,
			}

			batcher := NewWALBatcher(tmpDir, cfg)
			require.NoError(t, batcher.Start())
			defer func() { _ = batcher.Stop() }()

			pool := memory.NewGoAllocator()
			schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Uint32}}, nil)
			b := array.NewRecordBuilder(pool, schema)
			b.Field(0).(*array.Uint32Builder).AppendValues([]uint32{1, 2, 3}, nil)
			rec := b.NewRecordBatch()
			defer rec.Release()

			// Write enough to trigger batch flush
			for i := 0; i < 4; i++ {
				err := batcher.Write(rec, "test_ds", uint64(i), time.Now().UnixNano())
				require.NoError(t, err)
			}

			// Stop flushes remaining
			err := batcher.Stop()
			require.NoError(t, err)

			// Replay with WALIterator
			it, err := NewWALIterator(tmpDir, pool)
			require.NoError(t, err)
			defer it.Close()

			count := 0
			for {
				seq, _, _, r, err := it.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				assert.Equal(t, uint64(count), seq)
				r.Release()
				count++
			}
			assert.Equal(t, 4, count)

			// Replay with StorageEngine.ReplayWAL
			engine, err := NewStorageEngine(StorageConfig{DataPath: tmpDir}, pool)
			require.NoError(t, err)

			replayCount := 0
			_, err = engine.ReplayWAL(func(name string, r arrow.RecordBatch, seq uint64, ts int64) error {
				replayCount++
				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, 4, replayCount)
		})
	}
}
