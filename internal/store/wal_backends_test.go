package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestWALBackends_StdWAL(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	store := NewVectorStore(mem, logger, 1<<30, 0, 0)
	store.dataPath = tmpDir

	stdWAL := NewStdWAL(tmpDir, store)

	schema := arrow.NewSchema([]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewInt32Builder(mem)
	b.Append(123)
	rec := array.NewRecordBatch(schema, []arrow.Array{b.NewArray()}, 1)
	defer rec.Release()

	t.Run("WriteAndReplay", func(t *testing.T) {
		// Write using StdWAL
		err := stdWAL.Write("ds1", 1, 0, rec)
		require.NoError(t, err)

		// Verify file exists
		walPath := filepath.Join(tmpDir, walFileName)
		_, err = os.Stat(walPath)
		assert.NoError(t, err)

		// Replay using VectorStore
		err = store.replayWAL()
		require.NoError(t, err)

		// Check if record was loaded
		ds, ok := store.datasets["ds1"]
		assert.True(t, ok)
		assert.Equal(t, 1, len(ds.Records))
	})

	t.Run("AppendMultiple", func(t *testing.T) {
		// Write second record
		err := stdWAL.Write("ds1", 2, 0, rec)
		require.NoError(t, err)

		store.mu.Lock()
		store.datasets = make(map[string]*Dataset) // Clear memory
		store.mu.Unlock()
		err = store.replayWAL()
		require.NoError(t, err)

		ds := store.datasets["ds1"]
		assert.Equal(t, 2, len(ds.Records))
	})
}
