//go:build linux

package iouring

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Default alignment for O_DIRECT I/O (512 bytes, standard sector size)
const DefaultAlignment = 512

// Default page size (will be detected at runtime)
var pageSize = unix.Getpagesize()

// BufferPool manages O_DIRECT-aligned buffers for zero-copy I/O
type BufferPool struct {
	alignment  int // Alignment requirement (typically 512 for O_DIRECT)
	bufferSize int // Size of each buffer
	maxBuffers int // Maximum number of buffers in pool

	// Pool state
	available chan []byte          // Channel of available buffers
	allocated map[uintptr]struct{} // Track allocated buffers for safety
	mu        sync.RWMutex         // Protects allocated map

	// Stats
	hits   uint64
	misses uint64
}

// NewBufferPool creates a new buffer pool with O_DIRECT alignment
func NewBufferPool(bufferSize, maxBuffers int) (*BufferPool, error) {
	if bufferSize <= 0 {
		return nil, errors.New("buffer size must be positive")
	}
	if maxBuffers <= 0 {
		return nil, errors.New("max buffers must be positive")
	}

	// Round buffer size up to page size for mmap efficiency
	bufferSize = ((bufferSize + pageSize - 1) / pageSize) * pageSize

	pool := &BufferPool{
		alignment:  DefaultAlignment,
		bufferSize: bufferSize,
		maxBuffers: maxBuffers,
		available:  make(chan []byte, maxBuffers),
		allocated:  make(map[uintptr]struct{}),
	}

	// Pre-allocate buffers
	for i := 0; i < maxBuffers; i++ {
		buf, err := pool.allocAligned()
		if err != nil {
			// Cleanup already allocated buffers
			if closeErr := pool.Close(); closeErr != nil {
				return nil, fmt.Errorf("failed to allocate buffer %d: %w (close error: %v)", i, err, closeErr)
			}
			return nil, fmt.Errorf("failed to allocate buffer %d: %w", i, err)
		}
		pool.available <- buf
	}

	return pool, nil
}

// allocAligned allocates a new aligned buffer using mmap
func (p *BufferPool) allocAligned() ([]byte, error) {
	// Allocate extra space for alignment
	allocSize := p.bufferSize + p.alignment

	// Use mmap for aligned allocation
	// MAP_ANONYMOUS | MAP_PRIVATE gives us zeroed memory
	data, err := unix.Mmap(-1, 0, allocSize,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANONYMOUS|unix.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	// Calculate aligned offset
	ptr := uintptr(unsafe.Pointer(&data[0]))
	alignedPtr := (ptr + uintptr(p.alignment-1)) & ^uintptr(p.alignment-1)
	offset := int(alignedPtr - ptr)

	// Return the aligned slice
	alignedBuf := data[offset : offset+p.bufferSize]

	return alignedBuf, nil
}

// Get retrieves a buffer from the pool
// Returns nil if pool is exhausted
func (p *BufferPool) Get() []byte {
	select {
	case buf := <-p.available:
		if buf != nil {
			p.mu.Lock()
			ptr := uintptr(unsafe.Pointer(&buf[0]))
			p.allocated[ptr] = struct{}{}
			p.mu.Unlock()
		}
		return buf
	default:
		return nil
	}
}

// GetWait retrieves a buffer from the pool, waiting if necessary
func (p *BufferPool) GetWait() []byte {
	buf := <-p.available
	if buf != nil {
		p.mu.Lock()
		ptr := uintptr(unsafe.Pointer(&buf[0]))
		p.allocated[ptr] = struct{}{}
		p.mu.Unlock()
	}
	return buf
}

// Put returns a buffer to the pool
// Panics if buffer wasn't allocated from this pool
func (p *BufferPool) Put(buf []byte) {
	if len(buf) == 0 {
		return
	}

	ptr := uintptr(unsafe.Pointer(&buf[0]))

	p.mu.Lock()
	if _, ok := p.allocated[ptr]; !ok {
		p.mu.Unlock()
		panic("buffer pool: tried to return buffer not allocated from this pool")
	}
	delete(p.allocated, ptr)
	p.mu.Unlock()

	// Non-blocking send
	select {
	case p.available <- buf:
	default:
		// Pool is full, drop the buffer
		// This shouldn't happen with proper usage
	}
}

// IsAligned checks if a buffer meets alignment requirements
func (p *BufferPool) IsAligned(buf []byte) bool {
	if len(buf) == 0 {
		return false
	}
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	return ptr%uintptr(p.alignment) == 0
}

// Stats returns pool statistics
func (p *BufferPool) Stats() (available, allocated int) {
	p.mu.RLock()
	allocated = len(p.allocated)
	p.mu.RUnlock()
	available = len(p.available)
	return
}

// Close releases all buffers in the pool
func (p *BufferPool) Close() error {
	close(p.available)

	// Free all buffers
	for buf := range p.available {
		if buf != nil && len(buf) > 0 {
			// Find the original mmap start
			// We allocated bufferSize + alignment, so the mmap starts before our slice
			// Use unsafe.Add for pointer arithmetic (Go 1.20+)
			start := (*byte)(unsafe.Add(unsafe.Pointer(&buf[0]), -p.alignment))

			// Create slice to unmap
			slice := unsafe.Slice(start, p.bufferSize+p.alignment)
			unix.Munmap(slice)
		}
	}

	return nil
}

// BufferPoolStats provides statistics for the buffer pool
type BufferPoolStats struct {
	TotalBuffers     int
	AvailableBuffers int
	AllocatedBuffers int
	Hits             uint64
	Misses           uint64
}
