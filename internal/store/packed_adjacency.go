package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/apache/arrow-go/v18/arrow/float16"
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
	distanceArena *memory.TypedArena[float16.Num]
	pageArena     *memory.TypedArena[uint64]

	// chunks stores pointers to "Pages".
	// Index = NodeID / ChunkSize.
	// Value = Offset to Page (in pageArena).
	chunks atomic.Pointer[[]uint64]
	mu     sync.RWMutex // Protects chunks growth
}

func NewPackedAdjacency(arena *memory.SlabArena, initialCapacity int) *PackedAdjacency {
	return NewPackedAdjacencyWithArenas(arena,
		memory.NewTypedArena[uint32](arena),
		memory.NewTypedArena[float16.Num](arena),
		memory.NewTypedArena[uint64](arena),
		initialCapacity)
}

// NewPackedAdjacencyWithArenas allows reusing arenas (e.g. from GraphData)
func NewPackedAdjacencyWithArenas(arena *memory.SlabArena,
	neighborArena *memory.TypedArena[uint32],
	distanceArena *memory.TypedArena[float16.Num],
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
		distanceArena: distanceArena,
		pageArena:     pageArena,
	}
	pa.chunks.Store(&chunks)
	return pa
}

// EnsureCapacity resizes the directory if needed.
// thread-safe across multiple concurrent writers.
func (pa *PackedAdjacency) EnsureCapacity(nodeID uint32) {
	chunkIdx := int(nodeID) / AdjacencyChunkSize

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
		copy(newChunks, *curPtr)
	}
	// Atomic replace
	pa.chunks.Store(&newChunks)
}

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
	packed := PackRef(ref.Offset, uint32(len(neighbors)))

	// 3. Update Page
	return pa.updatePage(id, packed)
}

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

	dest := pa.baseArena.Get(offset, uint32(totalBytes))
	if len(dest) == 0 {
		return errors.New("packed adjacency: allocation failed")
	}

	// Layout: [neighbors...][distances...]
	// Use unsafe to get headers. Pointer to start of dest.
	nDest := unsafe.Slice((*uint32)(unsafe.Pointer(&dest[0])), len(neighbors))
	copy(nDest, neighbors)

	dDest := unsafe.Slice((*float16.Num)(unsafe.Pointer(&dest[len(neighbors)*4])), len(distances))
	copy(dDest, distances)

	// 2. Pack Ref
	packed := PackRef(offset, uint32(len(neighbors)))

	// 3. Update Page
	return pa.updatePage(id, packed)
}

func (pa *PackedAdjacency) updatePage(id uint32, packed uint64) error {
	chunkIdx := int(id) / AdjacencyChunkSize
	offsetInPage := int(id) % AdjacencyChunkSize

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
		pRef, err := pa.pageArena.AllocSlice(AdjacencyChunkSize)
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

	pageRef := memory.SliceRef{Offset: pageOffset, Len: AdjacencyChunkSize, Cap: AdjacencyChunkSize}
	page := pa.pageArena.Get(pageRef)
	if page == nil {
		return errors.New("packed adjacency: failed to get page")
	}

	atomic.StoreUint64(&page[offsetInPage], packed)
	return nil
}

func (pa *PackedAdjacency) GetNeighbors(id uint32) ([]uint32, bool) {
	packed, ok := pa.getPackedRef(id)
	if !ok {
		return nil, false
	}

	off, ln := UnpackRef(packed)
	nRef := memory.SliceRef{Offset: off, Len: uint32(ln), Cap: uint32(ln)}
	return pa.neighborArena.Get(nRef), true
}

func (pa *PackedAdjacency) GetNeighborsF16(id uint32) ([]uint32, []float16.Num, bool) {
	packed, ok := pa.getPackedRef(id)
	if !ok {
		return nil, nil, false
	}

	off, ln := UnpackRef(packed)
	length := int(ln)

	// Get combined block
	totalBytes := length*4 + length*2
	dest := pa.baseArena.Get(off, uint32(totalBytes))
	if len(dest) == 0 {
		return nil, nil, false
	}

	neighbors := unsafe.Slice((*uint32)(unsafe.Pointer(&dest[0])), length)
	distances := unsafe.Slice((*float16.Num)(unsafe.Pointer(&dest[length*4])), length)

	return neighbors, distances, true
}

func (pa *PackedAdjacency) getPackedRef(id uint32) (uint64, bool) {
	chunkIdx := int(id) / AdjacencyChunkSize
	offsetInPage := int(id) % AdjacencyChunkSize

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

	pageRef := memory.SliceRef{Offset: pageOffset, Len: AdjacencyChunkSize, Cap: AdjacencyChunkSize}
	page := pa.pageArena.Get(pageRef)
	if page == nil {
		return 0, false
	}

	return atomic.LoadUint64(&page[offsetInPage]), true
}
