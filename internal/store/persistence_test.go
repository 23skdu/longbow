package store

import (
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func mockLogger() zerolog.Logger {
	return zerolog.Nop()
}

func TestPersistence_ReadSeekClose(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()
	logger := mockLogger()
	store := NewVectorStore(mem, logger, 1024, 0, time.Hour)

	err := store.InitPersistence(storage.StorageConfig{
		DataPath:         tmpDir,
		SnapshotInterval: 1 * time.Hour,
	})
	assert.NoError(t, err)

	// Test Close
	err = store.Close()
	assert.NoError(t, err)
}

func TestPersistence_WriteToWAL_NoFile(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := mockLogger()
	store := NewVectorStore(mem, logger, 1024, 0, time.Hour)
	defer func() { _ = store.Close() }()

	// Should not error if persistence not initialized (returns nil)
	// But writeToWAL signature takes record. If record is nil, it might panic/error inside writeToWAL before checking engine?
	// Checking `writeToWAL` implementation:
	// func (s *VectorStore) writeToWAL(rec arrow.RecordBatch, name string) error {
	// 	if s.engine == nil { return nil } ...
	// }
	// So it returns nil.
	// But if record is nil, it's ignored if engine is nil.
	err := store.writeToWAL(nil, "test", time.Now().UnixNano())
	assert.NoError(t, err)

	schema := arrow.NewSchema([]arrow.Field{{Name: "f", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecordBatch()
	defer rec.Release()

	err = store.writeToWAL(rec, "test", time.Now().UnixNano())
	assert.NoError(t, err) // Should return nil if engine is nil
}
