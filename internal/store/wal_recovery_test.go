package store

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestWALRecovery_CorruptedTail(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "wal_recovery_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create a valid WAL with 2 entries
	walPath := filepath.Join(tmpDir, "wal.log")

	// Use a helper to write valid data manually or use the Store's write mechanism?
	// Let's use the Store to write valid data first.
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 1024*1024, 1024*1024, 0)
	config := StorageConfig{
		DataPath: tmpDir,
	}
	err = store.InitPersistence(config)
	require.NoError(t, err)

	// Create a record
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).Append(1)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Write 1
	err = store.writeToWAL(rec, "dataset1")
	require.NoError(t, err)

	// Write 2
	err = store.writeToWAL(rec, "dataset1")
	require.NoError(t, err)

	// Close store to flush everything
	store.wal.Close()

	// Now, corrupt the file!
	// 1. Truncate the file to cut off half of the second record
	info, err := os.Stat(walPath)
	require.NoError(t, err)
	size := info.Size()

	// Assume 2 records. Cut off the last 10 bytes.
	err = os.Truncate(walPath, size-10)
	require.NoError(t, err)

	// Re-open store and attempt recovery
	newStore := NewVectorStore(mem, zerolog.Nop(), 1024*1024, 1024*1024, 0)

	// This should NOT fail with the new fix.
	// Currently it likely fails with "unexpected EOF" or checksum mismatch.
	err = newStore.InitPersistence(config)

	// With the fix, we expect NO error, and 1 valid loaded record (or 0 if both corrupted, but here 1 should be safe)
	// For reproduction, we might ASSERT Error to fail if it passes?
	// But usually we write the test assuming the fix is present, and see it fail first.
	// Current behavior: Returns error.
	// Desired behavior: Returns nil.

	// Desired behavior: Returns nil (successful partial load).
	require.NoError(t, err, "Replay should tolerate corrupted tail")
}

func TestWALRecovery_ChecksumMismatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_chk_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "wal.log")

	// Manually write a corrupted entry
	f, err := os.Create(walPath)
	require.NoError(t, err)

	// Header
	header := make([]byte, 32)
	binary.LittleEndian.PutUint32(header[0:4], 0xDEADBEEF) // BAD Checksum
	binary.LittleEndian.PutUint64(header[4:12], 1)         // Seq
	binary.LittleEndian.PutUint32(header[20:24], 4)        // NameLen
	binary.LittleEndian.PutUint64(header[24:32], 4)        // RecLen

	f.Write(header)
	f.Write([]byte("test")) // Name
	f.Write([]byte("test")) // Rec (garbage)
	f.Close()

	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 1024*1024, 1024*1024, 0)
	config := StorageConfig{DataPath: tmpDir}

	err = store.InitPersistence(config)
	require.NoError(t, err, "Replay should tolerate checksum mismatch (truncate/ignore)")
}
