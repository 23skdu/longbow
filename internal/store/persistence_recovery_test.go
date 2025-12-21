package store

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPersistence_ReplayWAL_Corruption(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	// Helper to create a valid WAL entry buffer
	createWALEntry := func(name string, rec arrow.RecordBatch) []byte {
		var buf bytes.Buffer

		// Serialize record
		var recordBuf bytes.Buffer
		writer := ipc.NewWriter(&recordBuf, ipc.WithSchema(rec.Schema()))
		err := writer.Write(rec)
		if err != nil {
			panic(err)
		}
		err = writer.Close()
		if err != nil {
			panic(err)
		}
		recBytes := recordBuf.Bytes()

		nameBytes := []byte(name)

		// Calculate checksum
		crc := crc32.NewIEEE()
		_, _ = crc.Write(nameBytes)
		_, _ = crc.Write(recBytes)
		checksum := crc.Sum32()

		// Write header (CRC(4) | Seq(8) | Timestamp(8) | NameLen(4) | RecLen(8)) = 32 bytes
		header := make([]byte, 32)
		binary.LittleEndian.PutUint32(header[0:4], checksum)
		binary.LittleEndian.PutUint64(header[4:12], 0)  // dummy seq
		binary.LittleEndian.PutUint64(header[12:20], 0) // dummy ts
		binary.LittleEndian.PutUint32(header[20:24], uint32(len(nameBytes)))
		binary.LittleEndian.PutUint64(header[24:32], uint64(len(recBytes)))

		buf.Write(header)
		buf.Write(nameBytes)
		buf.Write(recBytes)

		return buf.Bytes()
	}

	schema := arrow.NewSchema([]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewInt32Builder(mem)
	b.Append(1)
	arr := b.NewArray()
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
	defer rec.Release()
	defer arr.Release()

	t.Run("ChecksumMismatch", func(t *testing.T) {
		walPath := filepath.Join(tmpDir, walFileName)
		entry := createWALEntry("test_ds", rec)

		// Corrupt the checksum (first 4 bytes)
		entry[0] ^= 0xFF

		err := os.WriteFile(walPath, entry, 0644)
		require.NoError(t, err)

		store := NewVectorStore(mem, logger, 1<<30, 0, time.Hour)
		store.dataPath = tmpDir

		err = store.replayWAL()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "crc mismatch")

		var walErr *WALError
		assert.ErrorAs(t, err, &walErr)
		assert.Equal(t, "read", walErr.Op)
	})

	t.Run("TruncatedHeader", func(t *testing.T) {
		walPath := filepath.Join(tmpDir, walFileName)
		// Only write 10 bytes instead of 32
		err := os.WriteFile(walPath, make([]byte, 10), 0644)
		require.NoError(t, err)

		store := NewVectorStore(mem, logger, 1<<30, 0, time.Hour)
		store.dataPath = tmpDir

		err = store.replayWAL()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "header")
	})

	t.Run("TruncatedRecord", func(t *testing.T) {
		walPath := filepath.Join(tmpDir, walFileName)
		entry := createWALEntry("test_ds", rec)

		// Truncate the record data (last few bytes)
		truncated := entry[:len(entry)-5]

		err := os.WriteFile(walPath, truncated, 0644)
		require.NoError(t, err)

		store := NewVectorStore(mem, logger, 1<<30, 0, time.Hour)
		store.dataPath = tmpDir

		err = store.replayWAL()
		assert.Error(t, err)
		// Since it checks CRC first, it will fail on CRC mismatch or io.ReadFull
		assert.Contains(t, err.Error(), "read failed")
	})
}
