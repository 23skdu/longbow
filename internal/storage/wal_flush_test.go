package storage

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

// MockFailingBackend simulates persistent failures
type MockFailingBackend struct {
	failWrite bool
	failSync  bool
}

func (m *MockFailingBackend) Write(p []byte) (int, error) {
	if m.failWrite {
		return 0, errors.New("simulated disk full")
	}
	return len(p), nil
}

func (m *MockFailingBackend) Sync() error {
	if m.failSync {
		return errors.New("simulated sync error")
	}
	return nil
}

func (m *MockFailingBackend) Close() error   { return nil }
func (m *MockFailingBackend) Name() string   { return "mock_fail" }
func (m *MockFailingBackend) File() *os.File { return nil }

func TestBufferedWAL_SilentFlushFailure_Reproduction(t *testing.T) {
	// After fix: verify we catch the error on ErrCh

	mock := &MockFailingBackend{failWrite: true}
	wal := NewBufferedWAL(mock, 1024, 10*time.Millisecond)
	defer func() { _ = wal.Close() }()

	// Create dummy record
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecordBatch()
	defer rec.Release()

	// 1. Write to WAL
	err := wal.Write("test_dataset", 1, time.Now().UnixNano(), rec)
	assert.NoError(t, err, "Write to buffer should always succeed")

	// 2. Wait for flush error
	select {
	case flushErr := <-wal.ErrCh:
		assert.Error(t, flushErr)
		assert.Contains(t, flushErr.Error(), "simulated disk full")
	case <-time.After(1 * time.Second):
		t.Fatal("Expected flush error on ErrCh, but timed out")
	}

	// 3. Verify Sync fails fast
	err = wal.Sync()
	assert.Error(t, err, "Sync should return fatal error")
	assert.Contains(t, err.Error(), "simulated disk full")

	// 4. Verify subsequent Writes fail fast
	err = wal.Write("test_dataset", 2, time.Now().UnixNano(), rec)
	assert.Error(t, err, "Write should return fatal error")
	assert.Contains(t, err.Error(), "simulated disk full")
}
