package memory

import (
	"fmt"
	"sync"
)

type DoubleBuffer struct {
	mu           sync.Mutex
	bufA         []byte
	bufB         []byte
	activeBuffer int // 0 for A, 1 for B
	activeSize   int
	capacity     int
	memPool      *GPUMemPool
	minHeadroom  int64
}

// DoubleBufferOption defines functional configuration for DoubleBuffer.
type DoubleBufferOption func(*DoubleBuffer)

// WithMemPool attaches a GPUMemPool to validate device memory headroom.
func WithMemPool(pool *GPUMemPool, minHeadroomBytes int64) DoubleBufferOption {
	return func(db *DoubleBuffer) {
		db.memPool = pool
		db.minHeadroom = minHeadroomBytes
	}
}

// NewDoubleBuffer allocates two slabs of the given capacity with optional configuration.
func NewDoubleBuffer(capacity int, opts ...DoubleBufferOption) *DoubleBuffer {
	db := &DoubleBuffer{
		bufA:     make([]byte, capacity),
		bufB:     make([]byte, capacity),
		capacity: capacity,
	}
	for _, opt := range opts {
		opt(db)
	}
	return db
}

// NewDoubleBufferWithHeadroom allocates two slabs after validating that the GPU memory pool has sufficient headroom.
func NewDoubleBufferWithHeadroom(capacity int, memPool *GPUMemPool, minHeadroomBytes int64) (*DoubleBuffer, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity must be positive")
	}
	if memPool != nil && memPool.GetTotalMemory() > 0 {
		avail := memPool.GetAvailableMemory()
		needed := int64(capacity*2) + minHeadroomBytes
		if needed > avail {
			return nil, fmt.Errorf("insufficient GPU memory headroom: requested %d bytes (capacity 2x%d + headroom %d), available %d bytes",
				needed, capacity, minHeadroomBytes, avail)
		}
	}
	return NewDoubleBuffer(capacity, WithMemPool(memPool, minHeadroomBytes)), nil
}

// CheckHeadroom validates that the attached GPU memory pool has sufficient headroom for an upcoming operation.
func (db *DoubleBuffer) CheckHeadroom(requiredBytes int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.memPool == nil || db.memPool.GetTotalMemory() == 0 {
		return nil
	}
	avail := db.memPool.GetAvailableMemory()
	needed := requiredBytes + db.minHeadroom
	if needed > avail {
		return fmt.Errorf("GPU memory headroom check failed: required %d bytes (+ %d headroom), only %d bytes available",
			requiredBytes, db.minHeadroom, avail)
	}
	return nil
}

// GetActive returns the current active buffer that can be written to by Go CPU code.
func (db *DoubleBuffer) GetActive() []byte {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.activeBuffer == 0 {
		return db.bufA
	}
	return db.bufB
}

// GetInactive returns the current inactive buffer that is currently being copied or processed on the GPU.
func (db *DoubleBuffer) GetInactive() []byte {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.activeBuffer == 0 {
		return db.bufB
	}
	return db.bufA
}

// Swap toggles the active and inactive buffers, resetting the active buffer size.
func (db *DoubleBuffer) Swap() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.activeBuffer = 1 - db.activeBuffer
	db.activeSize = 0
}

// Write appends data to the active buffer.
func (db *DoubleBuffer) Write(data []byte) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeSize+len(data) > db.capacity {
		return 0, fmt.Errorf("double buffer capacity exceeded (cap=%d, requested=%d)", db.capacity, db.activeSize+len(data))
	}

	var activeBuf []byte
	if db.activeBuffer == 0 {
		activeBuf = db.bufA
	} else {
		activeBuf = db.bufB
	}

	copy(activeBuf[db.activeSize:], data)
	db.activeSize += len(data)
	return len(data), nil
}

// ActiveSize returns the current active size.
func (db *DoubleBuffer) ActiveSize() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeSize
}

// Reset clears the active size.
func (db *DoubleBuffer) Reset() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.activeSize = 0
}

// Capacity returns the total capacity of each buffer.
func (db *DoubleBuffer) Capacity() int {
	return db.capacity
}
