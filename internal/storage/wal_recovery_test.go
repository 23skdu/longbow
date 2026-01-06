package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestWALRecovery_CorruptedTail(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	mem := memory.NewGoAllocator()

	// 1. Write some valid data
	wal := NewStdWAL(tmpDir)
	schema := arrow.NewSchema([]arrow.Field{{Name: "f", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewInt32Builder(mem)
	b.Append(1)
	rec := array.NewRecordBatch(schema, []arrow.Array{b.NewArray()}, 1)
	defer rec.Release()

	require.NoError(t, wal.Write("ds1", 1, 0, rec))
	require.NoError(t, wal.Write("ds1", 2, 0, rec))
	wal.Close()

	// 2. Corrupt tail
	info, err := os.Stat(walPath)
	require.NoError(t, err)
	err = os.Truncate(walPath, info.Size()-5)
	require.NoError(t, err)

	// 3. Replay
	it, err := NewWALIterator(tmpDir, mem)
	require.NoError(t, err)
	defer it.Close()

	// First should be ok
	seq, _, _, r, err := it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq)
	r.Release()

	// Second should fail with EOF or corruption
	_, _, _, _, err = it.Next()
	require.Error(t, err)
}

func TestWALRecovery_ChecksumMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")
	mem := memory.NewGoAllocator()

	f, err := os.Create(walPath)
	require.NoError(t, err)

	// Header with BAD Checksum
	header := make([]byte, 32)
	binary.LittleEndian.PutUint32(header[0:4], 0xDEADBEEF)
	binary.LittleEndian.PutUint64(header[4:12], 1)
	binary.LittleEndian.PutUint32(header[20:24], 4)
	binary.LittleEndian.PutUint64(header[24:32], 4)

	_, _ = f.Write(header)
	_, _ = f.WriteString("test")
	_, _ = f.WriteString("test")
	f.Close()

	it, err := NewWALIterator(tmpDir, mem)
	require.NoError(t, err)
	defer it.Close()

	_, _, _, _, err = it.Next()
	require.Error(t, err, "Should fail on CRC mismatch")
}
