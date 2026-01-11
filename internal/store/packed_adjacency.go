package store

import (
	"errors"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/memory"
)

const (
	AdjacencyChunkSize = 1024

	// PackedRef: [48 bits Offset | 16 bits Length]
	PackedRefLenMask  = 0xFFFF
	PackedRefOffShift = 16
)

func PackRef(offset uint64, length uint32) uint64 {
	return (offset << PackedRefOffShift) | (uint64(length) & PackedRefLenMask)
}

func UnpackRef(packed uint64) (offset uint64, length uint32) {
	return packed >> PackedRefOffShift, uint32(packed & PackedRefLenMask)
}

// PackedAdjacency manages neighbor lists using 2-level indirection.
type PackedAdjacency struct {
	baseArena     *memory.SlabArena
	neighborArena *memory.TypedArena[uint32]
	pageArena     *memory.TypedArena[uint64]

	// chunks stores pointers to "Pages".
	// Index = NodeID / ChunkSize.
	// Value = Offset to Page (in pageArena).
	chunks atomic.Pointer[[]uint64]
}

func NewPackedAdjacency(arena *memory.SlabArena, initialCapacity int) *PackedAdjacency {
	return NewPackedAdjacencyWithArenas(arena,
		memory.NewTypedArena[uint32](arena),
		memory.NewTypedArena[uint64](arena),
		initialCapacity)
}

// NewPackedAdjacencyWithArenas allows reusing arenas (e.g. from GraphData)
func NewPackedAdjacencyWithArenas(arena *memory.SlabArena,
	neighborArena *memory.TypedArena[uint32],
	pageArena *memory.TypedArena[uint64],
	initialCapacity int) *PackedAdjacency {

	numChunks := (initialCapacity + AdjacencyChunkSize - 1) / AdjacencyChunkSize
	if numChunks < 1 {
		numChunks = 1
	}

	chunks := make([]uint64, numChunks)

	pa := &PackedAdjacency{
		baseArena:     arena,
		neighborArena: neighborArena,
		pageArena:     pageArena,
	}
	pa.chunks.Store(&chunks)
	return pa
}

// EnsureCapacity resizes the directory if needed.
// This is NOT thread-safe against concurrent EnsureCapacity, but safe against specific readers.
func (pa *PackedAdjacency) EnsureCapacity(nodeID uint32) {
	chunkIdx := int(nodeID) / AdjacencyChunkSize

	curPtr := pa.chunks.Load()
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
		copy(newChunks, *curPtr)
	}
	// Atomic replace
	pa.chunks.Store(&newChunks)
}

func (pa *PackedAdjacency) SetNeighbors(id uint32, neighbors []uint32) error {
	// 1. Alloc neighbor list
	ref, err := pa.neighborArena.AllocSlice(len(neighbors))
	if err != nil {
		return err
	}

	// Copy neighbors
	dest := pa.neighborArena.Get(ref)
	copy(dest, neighbors)

	// 2. Pack Ref
	packed := PackRef(ref.Offset, uint32(len(neighbors)))

	// 3. Update Page
	chunkIdx := int(id) / AdjacencyChunkSize
	offsetInPage := int(id) % AdjacencyChunkSize

	// Auto-grow if needed
	chunksPtr := pa.chunks.Load()
	var chunks []uint64
	if chunksPtr == nil || chunkIdx >= len(*chunksPtr) {
		// Need to grow
		curLen := 0
		if chunksPtr != nil {
			curLen = len(*chunksPtr)
		}
		newLen := chunkIdx + 1
		if curLen > 0 && newLen < curLen*2 {
			newLen = curLen * 2
		}

		newChunks := make([]uint64, newLen)
		if chunksPtr != nil {
			copy(newChunks, *chunksPtr)
		}
		pa.chunks.Store(&newChunks)
		chunks = newChunks
	} else {
		chunks = *chunksPtr
	}

	// Get or Alloc Page
	// We use atomic load on the []uint64 entry
	pageOffset := atomic.LoadUint64(&chunks[chunkIdx])

	if pageOffset == 0 {
		// Alloc new page [1024]uint64
		pRef, err := pa.pageArena.AllocSlice(AdjacencyChunkSize)
		if err != nil {
			return err
		}

		// Zero the page explicitly
		pDest := pa.pageArena.Get(pRef)
		// Zero memory - SlabArena allocations are zeroed by Go's make()
		for i := range pDest {
			pDest[i] = 0
		}

		// CompareAndSwap
		if !atomic.CompareAndSwapUint64(&chunks[chunkIdx], 0, pRef.Offset) {
			// Lost race, use existing
			pageOffset = atomic.LoadUint64(&chunks[chunkIdx])
		} else {
			pageOffset = pRef.Offset
		}
	}

	// Atomic store into page
	// We need to resolve pointer to page again
	pageRef := memory.SliceRef{Offset: pageOffset, Len: AdjacencyChunkSize, Cap: AdjacencyChunkSize}
	page := pa.pageArena.Get(pageRef)

	if page == nil {
		// Corruption or logic error
		return errors.New("packed adjacency: failed to get page")
	}

	atomic.StoreUint64(&page[offsetInPage], packed)

	return nil
}

func (pa *PackedAdjacency) GetNeighbors(id uint32) ([]uint32, bool) {
	chunkIdx := int(id) / AdjacencyChunkSize
	offsetInPage := int(id) % AdjacencyChunkSize

	chunksPtr := pa.chunks.Load()
	if chunksPtr == nil {
		return nil, false
	}
	chunks := *chunksPtr
	if chunkIdx >= len(chunks) {
		return nil, false
	}

	pageOffset := atomic.LoadUint64(&chunks[chunkIdx])
	if pageOffset == 0 {
		return nil, false
	}

	pageRef := memory.SliceRef{Offset: pageOffset, Len: AdjacencyChunkSize, Cap: AdjacencyChunkSize}
	page := pa.pageArena.Get(pageRef)

	if page == nil {
		return nil, false
	}

	packed := atomic.LoadUint64(&page[offsetInPage])
	if packed == 0 {
		return nil, false
	}

	off, ln := UnpackRef(packed)
	nRef := memory.SliceRef{Offset: off, Len: uint32(ln), Cap: uint32(ln)}
	return pa.neighborArena.Get(nRef), true
}
