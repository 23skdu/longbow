package storage

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALBackends_StdWAL(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()

	stdWAL := NewStdWAL(tmpDir)
	defer func() { _ = stdWAL.Close() }()

	schema := arrow.NewSchema([]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewInt32Builder(mem)
	b.Append(123)
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	t.Run("WriteAndReplay", func(t *testing.T) {
		// Write using StdWAL
		err := stdWAL.Write("ds1", 1, 1000, rec)
		require.NoError(t, err)

		// Verify file exists
		walPath := filepath.Join(tmpDir, "wal.log")
		_, err = os.Stat(walPath)
		assert.NoError(t, err)

		// Replay using WALIterator
		it, err := NewWALIterator(tmpDir, mem)
		require.NoError(t, err)
		defer func() { _ = it.Close() }()

		seq, ts, name, r, err := it.Next()
		require.NoError(t, err)
		assert.Equal(t, "ds1", name)
		assert.Equal(t, uint64(1), seq)
		assert.Equal(t, int64(1000), ts)
		assert.NotNil(t, r)
		r.Release()

		_, _, _, _, err = it.Next()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("AppendMultiple", func(t *testing.T) {
		// Write second record
		err := stdWAL.Write("ds1", 2, 2000, rec)
		require.NoError(t, err)

		it, err := NewWALIterator(tmpDir, mem)
		require.NoError(t, err)
		defer func() { _ = it.Close() }()

		// Skip first
		seq1, _, _, r1, err := it.Next()
		require.NoError(t, err)
		assert.Equal(t, uint64(1), seq1)
		r1.Release()

		// Check second
		seq2, ts2, _, r2, err := it.Next()
		require.NoError(t, err)
		assert.Equal(t, uint64(2), seq2)
		assert.Equal(t, int64(2000), ts2)
		r2.Release()

		_, _, _, _, err = it.Next()
		assert.Equal(t, io.EOF, err)
	})
}
