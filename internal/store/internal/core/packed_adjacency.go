package core

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Adjacency and reference packing constants for HNSW graph storage.
const (
	// adjacencyChunkSize defines the number of nodes per adjacency page.
	adjacencyChunkSize = 1024
	// packedRefLenMask is the bitmask for extracting the neighbor list length.
	packedRefLenMask = 0xFFFF
	// packedRefOffShift is the bit shift for the offset in a packed adjacency reference.
	packedRefOffShift = 16
)

// PackRef combines an offset and length into a single 64-bit reference.
func PackRef(offset uint64, length uint32) uint64 {
	return (offset << packedRefOffShift) | (uint64(length) & packedRefLenMask)
}

// UnpackRef extracts the offset and length from a 64-bit reference.
func UnpackRef(packed uint64) (offset uint64, length uint32) {
	return packed >> packedRefOffShift, uint32(packed & packedRefLenMask)
}

// PackedAdjacency manages neighbor lists using 2-level indirection.
type PackedAdjacency struct {
	baseArena     *memory.SlabArena
	neighborArena *memory.TypedArena[uint32]
	distanceArena *memory.TypedArena[float16.Num]
	pageArena     *memory.TypedArena[uint64]

	// chunks stores pointers to "Pages".
	// Index = NodeID / types.ChunkSize.
	// Value = Offset to Page (in pageArena).
	chunks atomic.Pointer[[]uint64]
	mu     sync.RWMutex // Protects chunks growth
}

// NewPackedAdjacency creates a new PackedAdjacency structure with the given arena.
func NewPackedAdjacency(arena *memory.SlabArena, initialCapacity int) *PackedAdjacency {
	return NewPackedAdjacencyWithArenas(arena,
		memory.NewTypedArena[uint32](arena),
		memory.NewTypedArena[float16.Num](arena),
		memory.NewTypedArena[uint64](arena),
		initialCapacity)
}

// NewPackedAdjacencyWithArenas allows reusing arenas (e.g. from types.GraphData)
func NewPackedAdjacencyWithArenas(arena *memory.SlabArena,
	neighborArena *memory.TypedArena[uint32],
	distanceArena *memory.TypedArena[float16.Num],
	pageArena *memory.TypedArena[uint64],
	initialCapacity int) *PackedAdjacency {

	numChunks := (initialCapacity + adjacencyChunkSize - 1) / adjacencyChunkSize
	if numChunks < 1 {
		numChunks = 1
	}

	chunks := make([]uint64, numChunks)
	pa := &PackedAdjacency{
		baseArena:     arena,
		neighborArena: neighborArena,
		distanceArena: distanceArena,
		pageArena:     pageArena,
	}
	pa.chunks.Store(&chunks)
	return pa
}

// EnsureCapacity resizes the directory if needed.
// thread-safe across multiple concurrent writers.
func (pa *PackedAdjacency) EnsureCapacity(nodeID uint32) {
	chunkIdx := int(nodeID) / adjacencyChunkSize

	// Quick check without lock
	curPtr := pa.chunks.Load()
	if curPtr != nil && chunkIdx < len(*curPtr) {
		return
	}

	pa.mu.Lock()
	defer pa.mu.Unlock()

	// Re-check after acquiring lock
	curPtr = pa.chunks.Load()
	if curPtr != nil && chunkIdx < len(*curPtr) {
		return
	}

	// Default new capacity logic
	curLen := 0
	if curPtr != nil {
		curLen = len(*curPtr)
	}
	newLen := chunkIdx + 1
	if curLen > 0 && newLen < curLen*2 {
		newLen = curLen * 2
	}

	newChunks := make([]uint64, newLen)
	if curPtr != nil {
		// Atomic copy to prevent race with concurrent CAS on slice elements
		oldChunks := *curPtr
		for i := 0; i < len(oldChunks); i++ {
			newChunks[i] = atomic.LoadUint64(&oldChunks[i])
		}
	}
	
	// Atomic replace
	pa.chunks.Store(&newChunks)
}

// SetNeighbors updates the neighbor list for a node.
func (pa *PackedAdjacency) SetNeighbors(id uint32, neighbors []uint32) error {

	if len(neighbors) == 0 {
		// Store empty reference (offset 0, length 0)
		return pa.updatePage(id, PackRef(0, 0))
	}

	// 1. Alloc neighbor list (Aligned to 64 bytes)
	ref, err := pa.neighborArena.AllocSliceAligned(len(neighbors), 64)
	if err != nil {
		return err
	}

	// Copy neighbors
	dest := pa.neighborArena.Get(ref)
	copy(dest, neighbors)

	// 2. Pack Ref
	packed := PackRef(ref.Offset, uint32(len(neighbors))) // #nosec G115

	// 3. Update Page
	return pa.updatePage(id, packed)
}

// SetNeighborsF16 updates the neighbor list and associated distances for a node.
func (pa *PackedAdjacency) SetNeighborsF16(id uint32, neighbors []uint32, distances []float16.Num) error {

	if len(neighbors) != len(distances) {
		return errors.New("packed adjacency: neighbors and distances length mismatch")
	}

	if len(neighbors) == 0 {
		return pa.updatePage(id, PackRef(0, 0))
	}

	// Alloc a block of size len*4 + len*2
	totalBytes := len(neighbors)*4 + len(distances)*2
	// Align to 64 bytes for SIMD operations
	offset, err := pa.baseArena.AllocAligned(totalBytes, 64)
	if err != nil {
		return err
	}

	dest := pa.baseArena.Get(offset, uint32(totalBytes)) // #nosec G115
	if len(dest) == 0 {
		return errors.New("packed adjacency: allocation failed")
	}

	// Layout: [neighbors...][distances...]
	// Use unsafe to get headers. Pointer to start of dest.
	nDest := unsafe.Slice((*uint32)(unsafe.Pointer(&dest[0])), len(neighbors)) // #nosec G103
	copy(nDest, neighbors)

	dDest := unsafe.Slice((*float16.Num)(unsafe.Pointer(&dest[len(neighbors)*4])), len(distances)) // #nosec G103
	copy(dDest, distances)

	// 2. Pack Ref
	packed := PackRef(offset, uint32(len(neighbors))) // #nosec G115

	// 3. Update Page
	return pa.updatePage(id, packed)
}

func (pa *PackedAdjacency) updatePage(id uint32, packed uint64) error {
	chunkIdx := int(id) / adjacencyChunkSize
	offsetInPage := int(id) % adjacencyChunkSize

	// Auto-grow if needed
	chunksPtr := pa.chunks.Load()
	if chunksPtr == nil || chunkIdx >= len(*chunksPtr) {
		pa.EnsureCapacity(id)
		chunksPtr = pa.chunks.Load()
	}

	chunks := *chunksPtr

	// Get or Alloc Page
	pageOffset := atomic.LoadUint64(&chunks[chunkIdx])
	if pageOffset == 0 {
		// We still need to coordinate page allocation to avoid leaks/double-alloc,
		// but we can use CAS on the chunk slot.
		pRef, err := pa.pageArena.AllocSlice(adjacencyChunkSize)
		if err != nil {
			return err
		}
		pDest := pa.pageArena.Get(pRef)
		for i := range pDest {
			pDest[i] = 0
		}
		if !atomic.CompareAndSwapUint64(&chunks[chunkIdx], 0, pRef.Offset) {
			// Someone else won the race, return our slice to arena (if possible)
			// or just accept the tiny leak (it's internal arena so it stays till Close)
			pageOffset = atomic.LoadUint64(&chunks[chunkIdx])
		} else {
			pageOffset = pRef.Offset
		}
	}

	pageRef := memory.SliceRef{Offset: pageOffset, Len: adjacencyChunkSize, Cap: adjacencyChunkSize}
	page := pa.pageArena.Get(pageRef)
	if page == nil {
		return errors.New("packed adjacency: failed to get page")
	}

	atomic.StoreUint64(&page[offsetInPage], packed)
	return nil
}

// CASNeighbors performs an atomic Compare-And-Swap operation on a node's neighbor list.
func (pa *PackedAdjacency) CASNeighbors(id uint32, oldPacked uint64, new []uint32) bool {
	var newPacked uint64
	if len(new) > 0 {
		ref, err := pa.neighborArena.AllocSliceAligned(len(new), 64)
		if err != nil {
			return false
		}
		dest := pa.neighborArena.Get(ref)
		copy(dest, new)
		newPacked = PackRef(ref.Offset, uint32(len(new))) // #nosec G115
	}

	// 2. CAS in page
	chunkIdx := int(id) / adjacencyChunkSize
	offsetInPage := int(id) % adjacencyChunkSize
	chunksPtr := pa.chunks.Load()
	if chunksPtr == nil || chunkIdx >= len(*chunksPtr) {
		return false
	}
	chunks := *chunksPtr
	pageOffset := atomic.LoadUint64(&chunks[chunkIdx])
	if pageOffset == 0 {
		return false
	}
	page := pa.pageArena.Get(memory.SliceRef{Offset: pageOffset, Len: adjacencyChunkSize, Cap: adjacencyChunkSize})
	
	return atomic.CompareAndSwapUint64(&page[offsetInPage], oldPacked, newPacked)
}

// GetPackedNeighbors retrieves the packed reference for a node's neighbors.
func (pa *PackedAdjacency) GetPackedNeighbors(id uint32) (uint64, bool) {
	return pa.getPackedRef(id)
}

// Lock is a no-op for the lock-free implementation.
func (pa *PackedAdjacency) Lock(id uint32) {
	// No-op in lock-free version
}

// Unlock is a no-op for the lock-free implementation.
func (pa *PackedAdjacency) Unlock(id uint32) {
	// No-op in lock-free version
}

// UpdateNeighbors modifies a node's neighbor list using a transformation function.
func (pa *PackedAdjacency) UpdateNeighbors(id uint32, fn func(old []uint32) []uint32) error {
	for {
		packed, ok := pa.getPackedRef(id)
		if !ok {
			// Page doesn't exist yet, we must ensure it exists
			if err := pa.updatePage(id, 0); err != nil {
				return err
			}
			continue
		}

		off, ln := UnpackRef(packed)
		oldRef := memory.SliceRef{Offset: off, Len: uint32(ln), Cap: uint32(ln)}
		old := pa.neighborArena.Get(oldRef)

		new := fn(old)
		if new == nil {
			return nil // No change requested
		}

		if pa.CASNeighbors(id, packed, new) {
			return nil
		}
		// CAS failed, retry
	}
}

// GetNeighbors retrieves the neighbor list for a node.
func (pa *PackedAdjacency) GetNeighbors(id uint32) ([]uint32, bool) {
	return pa.GetNeighborsWithGen(id, math.MaxUint64)
}

// GetNeighborsWithGen retrieves the neighbor list for a node with generation isolation.
func (pa *PackedAdjacency) GetNeighborsWithGen(id uint32, maxGen uint64) ([]uint32, bool) {
	packed, ok := pa.getPackedRef(id)
	if !ok {
		return nil, false
	}

	return pa.GetNeighborsFromPackedWithGen(packed, maxGen), true
}

// GetNeighborsFromPacked retrieves the neighbor list from a packed reference.
func (pa *PackedAdjacency) GetNeighborsFromPacked(packed uint64) []uint32 {
	return pa.GetNeighborsFromPackedWithGen(packed, math.MaxUint64)
}

// GetNeighborsFromPackedWithGen retrieves the neighbor list from a packed reference with generation isolation.
func (pa *PackedAdjacency) GetNeighborsFromPackedWithGen(packed uint64, maxGen uint64) []uint32 {
	if packed == 0 {
		return nil
	}
	off, ln := UnpackRef(packed)
	nRef := memory.SliceRef{Offset: off, Len: uint32(ln), Cap: uint32(ln)}
	return pa.neighborArena.GetWithGeneration(nRef, maxGen)
}

// GetNeighborsF16 retrieves the neighbor list and distances for a node.
func (pa *PackedAdjacency) GetNeighborsF16(id uint32) ([]uint32, []float16.Num, bool) {
	return pa.GetNeighborsF16WithGen(id, math.MaxUint64)
}

// GetNeighborsF16WithGen retrieves the neighbor list and distances with generation isolation.
func (pa *PackedAdjacency) GetNeighborsF16WithGen(id uint32, maxGen uint64) ([]uint32, []float16.Num, bool) {
	packed, ok := pa.getPackedRef(id)
	if !ok {
		return nil, nil, false
	}

	off, ln := UnpackRef(packed)
	length := int(ln)

	// Get combined block
	totalBytes := uint32(length*4 + length*2) // #nosec G115
	dest := pa.baseArena.GetWithGeneration(off, totalBytes, maxGen)
	if len(dest) == 0 {
		return nil, nil, false
	}

	neighbors := unsafe.Slice((*uint32)(unsafe.Pointer(&dest[0])), length) // #nosec G103
	distances := unsafe.Slice((*float16.Num)(unsafe.Pointer(&dest[length*4])), length) // #nosec G103

	return neighbors, distances, true
}

func (pa *PackedAdjacency) getPackedRef(id uint32) (uint64, bool) {
	chunkIdx := int(id) / adjacencyChunkSize
	offsetInPage := int(id) % adjacencyChunkSize

	chunksPtr := pa.chunks.Load()
	if chunksPtr == nil {
		return 0, false
	}
	chunks := *chunksPtr
	if chunkIdx >= len(chunks) {
		return 0, false
	}

	pageOffset := atomic.LoadUint64(&chunks[chunkIdx])
	if pageOffset == 0 {
		return 0, false
	}

	pageRef := memory.SliceRef{Offset: pageOffset, Len: adjacencyChunkSize, Cap: adjacencyChunkSize}
	page := pa.pageArena.Get(pageRef)
	if page == nil {
		return 0, false
	}

	return atomic.LoadUint64(&page[offsetInPage]), true
}
