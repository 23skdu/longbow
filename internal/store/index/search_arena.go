package index

import (
	"sync"

	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// SearchArena is a per-request arena allocator that provides O(1) allocations
// with zero GC pressure. It uses a simple bump allocator strategy where
// allocations advance an offset through a pre-allocated buffer.
// The arena should be Reset() after each search request to reuse memory.
type SearchArena struct {
	buf    []byte
	offset int
	node   int
}

// NewSearchArena creates a new arena with the specified capacity in bytes.
// The entire buffer is pre-allocated to avoid runtime allocations.
func NewSearchArena(capacity int) *SearchArena {
	return NewSearchArenaForNode(capacity, 0)
}

// NewSearchArenaForNode creates a new arena mapped to a specific NUMA node.
func NewSearchArenaForNode(capacity int, node int) *SearchArena {
	return &SearchArena{
		buf:    allocateNUMAArena(capacity, node),
		offset: 0,
		node:   node,
	}
}

// Free releases the NUMA-allocated buffer.
func (a *SearchArena) Free() {
	freeNUMAArena(a.buf, len(a.buf))
}

// NUMANode returns the NUMA node this arena is pinned to.
func (a *SearchArena) NUMANode() int {
	return a.node
}

// Alloc allocates size bytes from the arena and returns a slice.
// Returns nil if the allocation would exceed capacity.
// This is O(1) - just a pointer bump with no GC involvement.
func (a *SearchArena) Alloc(size int) []byte {
	if size == 0 {
		return []byte{}
	}
	if a.offset+size > len(a.buf) {
		return nil
	}
	result := a.buf[a.offset : a.offset+size]
	a.offset += size
	return result
}

// Reset resets the arena for reuse without deallocating the underlying buffer.
// Call this after each search request to recycle memory.
func (a *SearchArena) Reset() {
	a.offset = 0
}

// Cap returns the total capacity of the arena in bytes.
func (a *SearchArena) Cap() int {
	return len(a.buf)
}

// Offset returns the current allocation offset (bytes used).
func (a *SearchArena) Offset() int {
	return a.offset
}

// Remaining returns the number of bytes still available for allocation.
func (a *SearchArena) Remaining() int {
	return len(a.buf) - a.offset
}

// AllocFloat32Slice allocates a slice of float32 values from the arena.
// Returns nil if the allocation would exceed capacity.
// This is useful for allocating distance/score arrays in search operations.
func (a *SearchArena) AllocFloat32Slice(count int) []float32 {
	if count == 0 {
		return []float32{}
	}

	// Calculate bytes needed (4 bytes per float32)
	const float32Size = 4
	bytesNeeded := count * float32Size

	// Ensure proper alignment for float32 (4-byte alignment)
	alignment := float32Size
	alignedOffset := (a.offset + alignment - 1) &^ (alignment - 1)

	if alignedOffset+bytesNeeded > len(a.buf) {
		return nil
	}

	// Update offset to aligned position plus allocation
	a.offset = alignedOffset + bytesNeeded

	// Convert byte slice to float32 slice using unsafe
	ptr := unsafe.Pointer(&a.buf[alignedOffset]) // #nosec G103
	return unsafe.Slice((*float32)(ptr), count)  // #nosec G103
}

// AllocVectorIDSlice allocates a slice of types.VectorID values from the arena.
// Returns nil if the allocation would exceed capacity.
// This is useful for allocating search result arrays without GC pressure.
func (a *SearchArena) AllocVectorIDSlice(count int) []types.VectorID {
	if count == 0 {
		return []types.VectorID{}
	}

	// Calculate bytes needed (4 bytes per types.VectorID which is uint32)
	const vectorIDSize = 4
	bytesNeeded := count * vectorIDSize

	// Ensure proper alignment for uint32 (4-byte alignment)
	alignment := vectorIDSize
	alignedOffset := (a.offset + alignment - 1) &^ (alignment - 1)

	if alignedOffset+bytesNeeded > len(a.buf) {
		return nil
	}

	// Update offset to aligned position plus allocation
	a.offset = alignedOffset + bytesNeeded

	// Convert byte slice to types.VectorID slice using unsafe
	ptr := unsafe.Pointer(&a.buf[alignedOffset])       // #nosec G103
	return unsafe.Slice((*types.VectorID)(ptr), count) // #nosec G103
}

// DefaultArenaSize is the default capacity for pooled arenas (64KB)
const DefaultArenaSize = 64 * 1024

var arenaPools [8]*sync.Pool

func init() {
	for i := 0; i < 8; i++ {
		node := i // Capture loop variable
		arenaPools[i] = &sync.Pool{
			New: func() any {
				return NewSearchArenaForNode(DefaultArenaSize, node)
			},
		}
	}
}

// GetArena retrieves a SearchArena from the global pool for node 0.
// The arena is reset and ready for use.
// Caller must call PutArena when done to return it to the pool.
func GetArena() *SearchArena {
	return GetArenaForNode(0)
}

// GetArenaForNode retrieves a SearchArena allocated on the specified NUMA node.
func GetArenaForNode(node int) *SearchArena {
	metrics.ArenaPoolGets.Inc()
	if node < 0 || node >= len(arenaPools) {
		node = 0
	}
	return arenaPools[node].Get().(*SearchArena)
}

// PutArena returns a SearchArena to the global pool for reuse.
// The arena is automatically reset before being pooled.
func PutArena(arena *SearchArena) {
	metrics.ArenaPoolPuts.Inc()
	if arena == nil {
		return
	}
	arena.Reset()
	node := arena.NUMANode()
	if node >= 0 && node < len(arenaPools) {
		arenaPools[node].Put(arena)
	}
}
