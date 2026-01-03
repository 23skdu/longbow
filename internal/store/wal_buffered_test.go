package store

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockBackend implements WALBackend for testing (in-memory)
type MockBackend struct {
	mu     sync.Mutex // Added mutex
	writes [][]byte
	synced bool
	closed bool
}

func (m *MockBackend) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Copy p because buffer will be reset
	c := make([]byte, len(p))
	copy(c, p)
	m.writes = append(m.writes, c)
	return len(p), nil
}

func (m *MockBackend) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.synced = true
	return nil
}

func (m *MockBackend) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *MockBackend) getWrites() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writes
}

func (m *MockBackend) isSynced() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.synced
}

func (m *MockBackend) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *MockBackend) Name() string   { return "mock" }
func (m *MockBackend) File() *os.File { return nil }

func TestBufferedWAL_FlushInterval(t *testing.T) {
	backend := &MockBackend{}
	// Short flush interval
	wal := NewBufferedWAL(backend, 1024*1024, 50*time.Millisecond)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	// 1. Write Data
	err := wal.Write("test_ds", 1, time.Now().UnixNano(), rec)
	require.NoError(t, err)

	// Since we haven't flushed, backend should be empty (unless maxBatchSize is very small, which is not here)
	// HOWEVER, there is a race: Write acquires lock, releases. Loop acquires lock.
	// We just check that eventually it flushes.

	// Wait for longer than flush interval
	time.Sleep(100 * time.Millisecond)

	// Backend should have data
	assert.NotEmpty(t, backend.getWrites(), "Backend should have received writes after flush interval")
	assert.True(t, backend.isSynced(), "Backend should be synced")

	wal.Close()
	assert.True(t, backend.isClosed())
}

func TestBufferedWAL_SyncForce(t *testing.T) {
	backend := &MockBackend{}
	// Long flush interval
	wal := NewBufferedWAL(backend, 1024*1024, 1*time.Minute)
	defer wal.Close()

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{10}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	// Write
	wal.Write("sync_ds", 2, 0, rec)

	// Backend possibly empty immediately

	// Force Sync
	err := wal.Sync()
	require.NoError(t, err)

	assert.NotEmpty(t, backend.getWrites())
	assert.True(t, backend.isSynced())
}

func TestBufferedWAL_BatchSizeTrigger(t *testing.T) {
	backend := &MockBackend{}
	// Small batch buffer
	wal := NewBufferedWAL(backend, 100, 1*time.Minute)
	defer wal.Close()

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.BinaryTypes.Binary},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	b.Field(0).(*array.BinaryBuilder).Append([]byte("large_payload_exceeding_100_bytes_hopefully_with_overhead"))
	rec := b.NewRecordBatch()
	defer rec.Release()

	// Only 1 write needed if it exceeds 100 bytes (header 32 + name + payload)
	wal.Write("trigger", 3, 0, rec)

	// Give loop a tiny bit of time to grab the channel signal
	time.Sleep(50 * time.Millisecond)

	assert.NotEmpty(t, backend.getWrites(), "Should trigger flush due to size limit")
}
