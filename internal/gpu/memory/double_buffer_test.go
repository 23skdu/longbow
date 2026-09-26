package memory

import (
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDoubleBuffer(t *testing.T) {
	db := NewDoubleBuffer(1024)
	assert.NotNil(t, db)
	assert.Equal(t, 1024, db.Capacity())
	assert.Equal(t, 0, db.ActiveSize())
}

func TestDoubleBufferActiveInactive(t *testing.T) {
	db := NewDoubleBuffer(64)
	active := db.GetActive()
	assert.Len(t, active, 64)

	inactive := db.GetInactive()
	assert.Len(t, inactive, 64)

	assert.False(t, &active[0] == &inactive[0])
}

func TestDoubleBufferSwap(t *testing.T) {
	db := NewDoubleBuffer(64)
	before := db.GetActive()
	db.Swap()
	after := db.GetActive()
	assert.False(t, &before[0] == &after[0])
	assert.Equal(t, 0, db.ActiveSize())
}

func TestDoubleBufferWrite(t *testing.T) {
	db := NewDoubleBuffer(64)
	n, err := db.Write([]byte("hello"))
	assert.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, 5, db.ActiveSize())
}

func TestDoubleBufferWriteCapacityExceeded(t *testing.T) {
	db := NewDoubleBuffer(10)
	_, err := db.Write(make([]byte, 11))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "capacity exceeded")
}

func TestDoubleBufferWriteAcrossSwap(t *testing.T) {
	db := NewDoubleBuffer(64)
	db.Write([]byte("first"))
	db.Swap()
	n, err := db.Write([]byte("second"))
	assert.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, 6, db.ActiveSize())

	db.Swap()
	ok := db.GetActive()
	assert.NotNil(t, ok)
}

func TestDoubleBufferReset(t *testing.T) {
	db := NewDoubleBuffer(64)
	db.Write([]byte("data"))
	db.Reset()
	assert.Equal(t, 0, db.ActiveSize())
}

func TestDoubleBufferCapacity(t *testing.T) {
	db := NewDoubleBuffer(256)
	assert.Equal(t, 256, db.Capacity())
}

func TestDoubleBufferRoundTrip(t *testing.T) {
	db := NewDoubleBuffer(32)
	data := []byte("test-data-here")
	n, err := db.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	buf := db.GetActive()
	assert.Equal(t, data, buf[:len(data)])
}

func TestDoubleBufferZeroCapacity(t *testing.T) {
	db := NewDoubleBuffer(0)
	_, err := db.Write([]byte{1})
	assert.Error(t, err)
}

func TestDoubleBufferWithHeadroom(t *testing.T) {
	pool := &GPUMemPool{
		totalBytes:  1000,
		usedBytes:   200,
		backend:     BackendCPU,
		allocations: make(map[unsafe.Pointer]int64),
	}

	// 2 * 200 + 100 = 500 <= 800 available -> should succeed
	db, err := NewDoubleBufferWithHeadroom(200, pool, 100)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.Equal(t, 200, db.Capacity())

	// Check headroom for operation needing 400 bytes (+ 100 headroom = 500 <= 800) -> success
	err = db.CheckHeadroom(400)
	assert.NoError(t, err)

	// Check headroom for operation needing 750 bytes (+ 100 headroom = 850 > 800) -> error
	err = db.CheckHeadroom(750)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "headroom check failed")

	// Allocation exceeding available headroom (2 * 400 + 100 = 900 > 800) -> should fail
	_, err = NewDoubleBufferWithHeadroom(400, pool, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient GPU memory headroom")
}

func TestDoubleBuffer_HighVRAM_Stress(t *testing.T) {
	// Simulate 16GB total VRAM pool with 1GB headroom and 1M vector operations (128 dims * 4 bytes = 512MB per batch)
	const totalVRAM = 16 * 1024 * 1024 * 1024
	const headroom = 1024 * 1024 * 1024
	const batchSize = 100000 // 100k vectors per buffer = 51.2MB

	pool := &GPUMemPool{
		totalBytes:  totalVRAM,
		usedBytes:   0,
		backend:     BackendCPU,
		allocations: make(map[unsafe.Pointer]int64),
	}

	db, err := NewDoubleBufferWithHeadroom(batchSize*128*4, pool, headroom)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Stress concurrent multi-stream access: 16 streams performing concurrent Writes, Swaps, and Headroom checks
	const numGoroutines = 16
	const iterations = 50
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()
			scratch := make([]byte, 1024)
			for i := 0; i < iterations; i++ {
				// 1. Verify headroom for upcoming operation
				err := db.CheckHeadroom(int64(len(scratch)))
				assert.NoError(t, err)

				// 2. Write data to active buffer
				_, _ = db.Write(scratch)

				// 3. Swap buffers simulating stream completion
				db.Swap()

				// 4. Retrieve active buffer
				_ = db.GetActive()
			}
		}()
	}

	wg.Wait()
}
