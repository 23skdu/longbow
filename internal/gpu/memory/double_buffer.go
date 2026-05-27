package memory

import (
	"fmt"
	"sync"
)

// DoubleBuffer holds two pre-allocated memory blocks to pipeline host-to-device transfers.
type DoubleBuffer struct {
	mu           sync.Mutex
	bufA         []byte
	bufB         []byte
	activeBuffer int // 0 for A, 1 for B
	activeSize   int
	capacity     int
}

// NewDoubleBuffer allocates two slabs of the given capacity.
func NewDoubleBuffer(capacity int) *DoubleBuffer {
	return &DoubleBuffer{
		bufA:     make([]byte, capacity),
		bufB:     make([]byte, capacity),
		capacity: capacity,
	}
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
