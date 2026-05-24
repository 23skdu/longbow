package index

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Adjacency and reference packing constants for HNSW graph storage.
const (
	// adjacencyChunkSize defines the number of nodes per adjacency page.
	adjacencyChunkSize = 1024
	// packedRefLenMask is the bitmask for extracting the neighbor list length.
	packedRefLenMask = 0xFFFF
	// packedRefCapMask is the bitmask for extracting the neighbor list capacity.
	packedRefCapMask = 0xFF
	// packedRefCapShift is the bit shift for the capacity
	packedRefCapShift = 16
	// packedRefOffShift is the bit shift for the offset in a packed adjacency reference.
	packedRefOffShift = 24
)

// PackRef combines an offset, length, and capacity into a single 64-bit reference.
func PackRef(offset uint64, length uint32, capacity uint32) uint64 {
	return (offset << packedRefOffShift) | ((uint64(capacity) & packedRefCapMask) << packedRefCapShift) | (uint64(length) & packedRefLenMask)
}

// UnpackRef extracts the offset, length, and capacity from a 64-bit reference.
func UnpackRef(packed uint64) (offset uint64, length uint32, capacity uint32) {
	return packed >> packedRefOffShift, uint32(packed & packedRefLenMask), uint32((packed >> packedRefCapShift) & packedRefCapMask)
}

// PackedAdjacency manages neighbor lists using 2-level indirection.
type PackedAdjacency struct {
	baseArena     *memory.SlabArena
	neighborArena *memory.TypedArena[uint32]
	distanceArena *memory.TypedArena[float16.Num]
	pageArena     *memory.TypedArena[uint64]
	offHeapAlloc  *memory.OffHeapAllocator

	// chunks stores pointers to "Pages".
	// Index = NodeID / types.ChunkSize.
	// Value = Offset to Page (in pageArena).
	chunks   atomic.Pointer[[]uint64]
	mu       sync.RWMutex // Protects chunks growth
	refCount atomic.Int64
	locks    []sync.Mutex // Striped locks to prevent retry storms
}

// NewPackedAdjacency creates a PackedAdjacency with internal arenas
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
		offHeapAlloc:  nil, // Chunks initially on-heap to prevent concurrent off-heap resize races
		locks:         make([]sync.Mutex, 65536),
	}
	pa.chunks.Store(&chunks)
	pa.refCount.Store(1)
	metrics.SlabRefCountDistribution.WithLabelValues("adjacency").Observe(1)
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
		if pa.offHeapAlloc != nil {
			oldBytes := unsafe.Slice((*byte)(unsafe.Pointer(&oldChunks[0])), len(oldChunks)*8) // #nosec G103
			pa.offHeapAlloc.Free(oldBytes)
		}
	}

	// Atomic replace
	pa.chunks.Store(&newChunks)
}

// SetNeighbors updates the neighbor list for a node.
func (pa *PackedAdjacency) SetNeighbors(id uint32, neighbors []uint32) error {

	if len(neighbors) == 0 {
		// Store empty reference (offset 0, length 0)
		return pa.updatePage(id, PackRef(0, 0, 0))
	}

	packed, ok := pa.getPackedRef(id)
	var oldCap uint32
	var oldOffset uint64
	if ok && packed != 0 {
		oldOffset, _, oldCap = UnpackRef(packed)
	}

	newLen := uint32(len(neighbors)) // #nosec G115

	if oldOffset != 0 && newLen <= oldCap {
		// Reuse existing allocation
		dest := pa.neighborArena.Get(memory.SliceRef{Offset: oldOffset, Len: oldCap, Cap: oldCap})
		copy(dest, neighbors)
		return pa.updatePage(id, PackRef(oldOffset, newLen, oldCap))
	}

	// Calculate new capacity (power of 2)
	newCap := oldCap
	if newCap == 0 {
		newCap = 8
	}
	for newCap < newLen {
		newCap *= 2
	}
	// Max capacity to prevent overflow of 8-bit capacity field
	if newCap > 255 {
		newCap = 255
	}
	if newLen > 255 {
		return errors.New("packed adjacency: max capacity 255 exceeded")
	}

	// 1. Alloc neighbor list (Aligned to 64 bytes)
	ref, err := pa.neighborArena.AllocSliceAligned(int(newCap), 64)
	if err != nil {
		return err
	}

	// Copy neighbors
	dest := pa.neighborArena.Get(ref)
	copy(dest, neighbors)

	// 2. Pack Ref
	newPacked := PackRef(ref.Offset, newLen, newCap)

	// 3. Update Page
	return pa.updatePage(id, newPacked)
}

// SetNeighborsF16 updates the neighbor list and associated distances for a node.
func (pa *PackedAdjacency) SetNeighborsF16(id uint32, neighbors []uint32, distances []float16.Num) error {

	if len(neighbors) != len(distances) {
		return errors.New("packed adjacency: neighbors and distances length mismatch")
	}

	if len(neighbors) == 0 {
		return pa.updatePage(id, PackRef(0, 0, 0))
	}

	packed, ok := pa.getPackedRef(id)
	var oldCap uint32
	var oldOffset uint64
	if ok && packed != 0 {
		oldOffset, _, oldCap = UnpackRef(packed)
	}

	newLen := uint32(len(neighbors)) // #nosec G115

	if oldOffset != 0 && newLen <= oldCap {
		// Reuse existing allocation
		totalBytes := int(oldCap)*4 + int(oldCap)*2
		dest := pa.baseArena.Get(oldOffset, uint32(totalBytes))
		nDest := unsafe.Slice((*uint32)(unsafe.Pointer(&dest[0])), oldCap) // #nosec G103
		copy(nDest, neighbors)
		dDest := unsafe.Slice((*float16.Num)(unsafe.Pointer(&dest[int(oldCap)*4])), oldCap) // #nosec G103
		copy(dDest, distances)
		return pa.updatePage(id, PackRef(oldOffset, newLen, oldCap))
	}

	newCap := oldCap
	if newCap == 0 {
		newCap = 8
	}
	for newCap < newLen {
		newCap *= 2
	}
	if newCap > 255 {
		newCap = 255
	}
	if newLen > 255 {
		return errors.New("packed adjacency: max capacity 255 exceeded")
	}

	// Alloc a block of size newCap*4 + newCap*2
	totalBytes := int(newCap)*4 + int(newCap)*2
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
	nDest := unsafe.Slice((*uint32)(unsafe.Pointer(&dest[0])), newCap) // #nosec G103
	copy(nDest, neighbors)

	dDest := unsafe.Slice((*float16.Num)(unsafe.Pointer(&dest[int(newCap)*4])), newCap) // #nosec G103
	copy(dDest, distances)

	// 2. Pack Ref
	newPacked := PackRef(offset, newLen, newCap)

	// 3. Update Page
	return pa.updatePage(id, newPacked)
}

func (pa *PackedAdjacency) updatePage(id uint32, packed uint64) error {
	chunkIdx := int(id) / adjacencyChunkSize
	offsetInPage := int(id) % adjacencyChunkSize

	pa.EnsureCapacity(id)
	chunksPtr := pa.chunks.Load()
	if chunksPtr == nil {
		return errors.New("chunks array is nil")
	}
	chunks := *chunksPtr

	for {
		pageRef := atomic.LoadUint64(&chunks[chunkIdx])
		if pageRef == 0 {
			// Allocate new page
			newRef, err := pa.pageArena.AllocSlice(adjacencyChunkSize)
			if err != nil {
				return err
			}
			newDest := pa.pageArena.Get(newRef)
			for i := 0; i < adjacencyChunkSize; i++ {
				newDest[i] = 0
			}
			// CAS update chunk pointer to the new page
			if !atomic.CompareAndSwapUint64(&chunks[chunkIdx], 0, newRef.Offset) {
				// CAS failed, someone else allocated it. We can just loop and use theirs.
				continue
			}
			pageRef = newRef.Offset
		}

		page := pa.pageArena.Get(memory.SliceRef{Offset: pageRef, Len: adjacencyChunkSize, Cap: adjacencyChunkSize})
		// In-place modification prevents lost updates that happen with CoW pages
		atomic.StoreUint64(&page[offsetInPage], packed)
		return nil
	}
}

// CASNeighbors performs an atomic Compare-And-Swap operation on a node's neighbor list.
func (pa *PackedAdjacency) CASNeighbors(id uint32, oldPacked uint64, new []uint32) bool {
	var newPacked uint64
	if len(new) > 0 {
		newLen := uint32(len(new)) // #nosec G115

		var oldCap uint32
		var oldOffset uint64
		if oldPacked != 0 {
			oldOffset, _, oldCap = UnpackRef(oldPacked)
		}

		if oldOffset != 0 && newLen <= oldCap {
			dest := pa.neighborArena.Get(memory.SliceRef{Offset: oldOffset, Len: oldCap, Cap: oldCap})
			copy(dest, new)
			newPacked = PackRef(oldOffset, newLen, oldCap)
		} else {
			newCap := oldCap
			if newCap == 0 {
				newCap = 8
			}
			for newCap < newLen {
				newCap *= 2
			}
			if newCap > 255 {
				newCap = 255
			}

			ref, err := pa.neighborArena.AllocSliceAligned(int(newCap), 64)
			if err != nil {
				return false
			}
			dest := pa.neighborArena.Get(ref)
			copy(dest, new)
			newPacked = PackRef(ref.Offset, newLen, newCap)
		}
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

// Lock acquires a striped lock for the given node ID.
func (pa *PackedAdjacency) Lock(id uint32) {
	pa.locks[id%65536].Lock()
}

// Unlock unlocks the striped lock for the given node ID.
func (pa *PackedAdjacency) Unlock(id uint32) {
	pa.locks[id%65536].Unlock()
}

// UpdateNeighbors modifies a node's neighbor list using a transformation function.
func (pa *PackedAdjacency) UpdateNeighbors(id uint32, fn func(old []uint32) []uint32) error {
	// Use striped lock to prevent O(N^2) lock-free retry storms when fn() is expensive
	pa.Lock(id)
	defer pa.Unlock(id)

	for {
		packed, ok := pa.getPackedRef(id)
		if !ok {
			// Page doesn't exist yet, we must ensure it exists
			if err := pa.updatePage(id, 0); err != nil {
				return err
			}
			continue
		}

		off, ln, _ := UnpackRef(packed)
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

	res := pa.GetNeighborsFromPackedWithGen(packed, maxGen)
	return res, true
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
	off, ln, _ := UnpackRef(packed)
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

	off, ln, oldCap := UnpackRef(packed)
	length := int(ln)
	capacity := int(oldCap)

	// Get combined block
	totalBytes := uint32(capacity*4 + capacity*2) // #nosec G115
	dest := pa.baseArena.GetWithGeneration(off, totalBytes, maxGen)
	if len(dest) == 0 {
		return nil, nil, false
	}

	neighbors := unsafe.Slice((*uint32)(unsafe.Pointer(&dest[0])), length)               // #nosec G103
	distances := unsafe.Slice((*float16.Num)(unsafe.Pointer(&dest[capacity*4])), length) // #nosec G103

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
func (pa *PackedAdjacency) RelocateToOffHeap(alloc *memory.OffHeapAllocator) {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	curPtr := pa.chunks.Load()
	if curPtr == nil {
		return
	}
	oldChunks := *curPtr

	// Allocate off-heap slice
	size := len(oldChunks) * 8
	newData := alloc.Allocate(size)
	if newData == nil {
		return
	}

	// Zero-copy view as []uint64
	newChunksTyped := unsafe.Slice((*uint64)(unsafe.Pointer(&newData[0])), len(oldChunks)) // #nosec G103

	copy(newChunksTyped, oldChunks)
	pa.chunks.Store(&newChunksTyped)
	pa.offHeapAlloc = alloc

	// Also relocate the underlying arena if it exists
	if pa.baseArena != nil {
		_ = pa.baseArena.ConvertToOffHeap(alloc)
	}
}

func (pa *PackedAdjacency) GetArena() *memory.SlabArena {
	return pa.baseArena
}

// IsOffHeap returns true if the backing arena is off-heap.
func (pa *PackedAdjacency) IsOffHeap() bool {
	if pa.baseArena == nil {
		return false
	}
	return pa.baseArena.IsOffHeap()
}

func (pa *PackedAdjacency) Release() {
	newRef := pa.refCount.Add(-1)
	metrics.SlabRefCountDistribution.WithLabelValues("adjacency").Observe(float64(newRef))
	if newRef == 0 {
		if pa.neighborArena != nil {
			pa.neighborArena.Release()
		}
		if pa.distanceArena != nil {
			pa.distanceArena.Release()
		}
		if pa.pageArena != nil {
			pa.pageArena.Release()
		}
		if pa.offHeapAlloc != nil {
			curPtr := pa.chunks.Load()
			if curPtr != nil {
				chunks := *curPtr
				if len(chunks) > 0 {
					bytes := unsafe.Slice((*byte)(unsafe.Pointer(&chunks[0])), len(chunks)*8) // #nosec G103
					pa.offHeapAlloc.Free(bytes)
				}
			}
		}
		pa.chunks.Store(nil)
	}
}

func (pa *PackedAdjacency) Retain() {
	newRef := pa.refCount.Add(1)
	metrics.SlabRefCountDistribution.WithLabelValues("adjacency").Observe(float64(newRef))
}
