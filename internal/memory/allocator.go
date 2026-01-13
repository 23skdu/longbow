package memory

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// DefaultArenaChunkSize is 64MB
	DefaultArenaChunkSize = 64 * 1024 * 1024
)

// ArenaAllocator implements memory.Allocator using a pool of large buffers.
// It is designed to reduce GC pressure by reusing large chunks of memory.
// It is NOT thread-safe for allocation within a single Arena instance (except via the pool),
// but typical Arrow usage is single-threaded per record batch construction.
// However, to be safe and strictly compliant, we use a mutex for the current chunk.
type ArenaAllocator struct {
	mu           sync.Mutex
	currentChunk []byte
	offset       int
	allocated    int64
	chunks       []*[]byte
	pool         *sync.Pool
}

// NewArenaAllocator creates a new allocator backed by a shared pool.
func NewArenaAllocator() *ArenaAllocator {
	return &ArenaAllocator{
		pool: globalChunkPool,
	}
}

var globalChunkPool = &sync.Pool{
	New: func() interface{} {
		// Allocate a big chunk
		b := make([]byte, DefaultArenaChunkSize)
		return &b
	},
}

// Allocate allocates a slice of size b.
func (a *ArenaAllocator) Allocate(size int) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()

	atomic.AddInt64(&a.allocated, int64(size))

	// If request is huge, just make a dedicated slice (don't pollute arena)
	if size > DefaultArenaChunkSize {
		return make([]byte, size)
	}

	// Check if we have room in current chunk
	if a.currentChunk != nil && a.offset+size <= len(a.currentChunk) {
		start := a.offset
		a.offset += size
		return a.currentChunk[start:a.offset]
	}

	// Need new chunk
	newChunkPtr := a.pool.Get().(*[]byte)
	newChunk := *newChunkPtr
	a.chunks = append(a.chunks, newChunkPtr)
	a.currentChunk = newChunk
	a.offset = size
	return newChunk[:size]
}

// Reallocate resizes a slice.
func (a *ArenaAllocator) Reallocate(size int, b []byte) []byte {
	if size == len(b) {
		return b
	}
	newBuf := a.Allocate(size)
	copy(newBuf, b)
	return newBuf
}

// Free is a no-op for the arena, as we free everything at Release().
// However, strictly compliant allocators might track bytes liberated.
func (a *ArenaAllocator) Free(b []byte) {
	// No-op
	atomic.AddInt64(&a.allocated, -int64(len(b)))
}

// Allocated returns total bytes currently allocated.
func (a *ArenaAllocator) Allocated() int64 {
	return atomic.LoadInt64(&a.allocated)
}

// Release returns all chunks to the pool.
// MUST be called when the batch/request flows are done.
func (a *ArenaAllocator) Release() {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, chunkPtr := range a.chunks {
		// Reset chunk for next user?
		// Actually, standard sync.Pool behavior relies on the user to re-initialize or overwrite.
		// Since we append to it, we don't zero it out (expensive). DO NOT Assume zeroed memory.
		a.pool.Put(chunkPtr)
	}
	a.chunks = nil
	a.currentChunk = nil
	a.offset = 0
	atomic.StoreInt64(&a.allocated, 0)
}

// AssertSize is a test helper (no-op here)
func (a *ArenaAllocator) AssertSize(t interface{}, sz int) {
	if int(a.Allocated()) != sz {
		panic(fmt.Sprintf("allocator size mismatch: expected %d, got %d", sz, a.Allocated()))
	}
}

var _ memory.Allocator = (*ArenaAllocator)(nil)
