package store

import (
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
"github.com/stretchr/testify/assert"
)

func TestPersistence_ReadSeekClose(t *testing.T) {
tmpDir := t.TempDir()
mem := memory.NewGoAllocator()
logger := mockLogger()
store := NewVectorStore(mem, logger, 1024, 0, time.Hour)

err := store.InitPersistence(tmpDir, time.Hour)
assert.NoError(t, err)

// Test Close
err = store.Close()
assert.NoError(t, err)
assert.Nil(t, store.walFile)
}

func TestPersistence_WriteToWAL_NoFile(t *testing.T) {
mem := memory.NewGoAllocator()
logger := mockLogger()
store := NewVectorStore(mem, logger, 1024, 0, time.Hour)

// Should not error if persistence not initialized (walFile is nil)
err := store.writeToWAL(nil, "test")
assert.Error(t, err) // record is nil

schema := arrow.NewSchema([]arrow.Field{{Name: "f", Type: arrow.PrimitiveTypes.Int64}}, nil)
b := array.NewRecordBuilder(mem, schema)
b.Field(0).(*array.Int64Builder).Append(1)
rec := b.NewRecordBatch()
defer rec.Release()

err = store.writeToWAL(rec, "test")
assert.NoError(t, err) // Should return nil if walFile is nil
}
