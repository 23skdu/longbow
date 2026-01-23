package memory

import (
	"fmt"
	"sync"
	"unsafe"
)

// CompactionStats tracks statistics from arena compaction operations
type CompactionStats struct {
	SlabsCompacted   int     // Number of slabs that were compacted
	BytesReclaimed   int64   // Bytes of memory reclaimed
	LiveDataCopied   int64   // Bytes of live data copied
	FragmentationPct float64 // Fragmentation percentage before compaction
}

// CompactArena consolidates fragmented slabs in a TypedArena.
// It copies live data to new slabs and releases old fragmented ones.
// This is useful after many allocations and deallocations have fragmented memory.
func CompactArena[T any](ta *TypedArena[T], liveRefs []SliceRef) (*CompactionStats, error) {
	if len(liveRefs) == 0 {
		return &CompactionStats{}, nil
	}

	var zero T
	elemSize := int(unsafe.Sizeof(zero))

	// Calculate total live data size
	var totalLiveBytes int64
	for _, ref := range liveRefs {
		totalLiveBytes += int64(ref.Len) * int64(elemSize)
	}

	// Get old arena stats
	oldSlabs := *ta.arena.slabs.Load()
	oldSlabCount := len(oldSlabs)
	var oldTotalBytes int64
	for _, slab := range oldSlabs {
		oldTotalBytes += int64(slab.offset)
	}

	// Create a new arena for compacted data
	newArena := NewSlabArena(int(ta.arena.slabCap))
	newTypedArena := NewTypedArena[T](newArena)

	// Copy all live data to the new arena
	newRefs := make([]SliceRef, len(liveRefs))
	for i, oldRef := range liveRefs {
		// Get the old data
		oldData := ta.Get(oldRef)
		if oldData == nil {
			continue
		}

		// Allocate in new arena
		newRef, err := newTypedArena.AllocSliceDirty(int(oldRef.Len))
		if err != nil {
			return nil, fmt.Errorf("failed to allocate during compaction: %w", err)
		}

		// Copy data
		newData := newTypedArena.Get(newRef)
		copy(newData, oldData)
		newRefs[i] = newRef
	}

	// Calculate stats before swapping
	fragmentationPct := 0.0
	if oldTotalBytes > 0 {
		fragmentationPct = float64(oldTotalBytes-totalLiveBytes) / float64(oldTotalBytes) * 100
	}

	stats := &CompactionStats{
		SlabsCompacted:   oldSlabCount,
		BytesReclaimed:   oldTotalBytes - totalLiveBytes,
		LiveDataCopied:   totalLiveBytes,
		FragmentationPct: fragmentationPct,
	}

	// Swap the arenas (caller must update references)
	ta.arena = newArena

	return stats, nil
}

// CompactableArena wraps TypedArena with compaction support
type CompactableArena[T any] struct {
	mu    sync.RWMutex
	arena *TypedArena[T]
}

// NewCompactableArena creates a new compactable typed arena
func NewCompactableArena[T any](slabSize int) *CompactableArena[T] {
	return &CompactableArena[T]{
		arena: NewTypedArena[T](NewSlabArena(slabSize)),
	}
}

// AllocSlice allocates a typed slice (thread-safe)
func (ca *CompactableArena[T]) AllocSlice(count int) (SliceRef, error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	return ca.arena.AllocSlice(count)
}

// AllocSliceDirty allocates a typed slice without zeroing (thread-safe)
func (ca *CompactableArena[T]) AllocSliceDirty(count int) (SliceRef, error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	return ca.arena.AllocSliceDirty(count)
}

// Get retrieves a typed slice (thread-safe for reads)
func (ca *CompactableArena[T]) Get(ref SliceRef) []T {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	return ca.arena.Get(ref)
}

// Compact performs compaction with exclusive lock
// Returns new references in the same order as liveRefs input
func (ca *CompactableArena[T]) Compact(liveRefs []SliceRef) ([]SliceRef, *CompactionStats, error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	// Store old references to map old->new
	oldToNew := make(map[uint64]SliceRef)
	for _, ref := range liveRefs {
		oldToNew[ref.Offset] = ref
	}

	stats, err := CompactArena(ca.arena, liveRefs)
	if err != nil {
		return nil, nil, err
	}

	// After CompactArena, ca.arena has been swapped to the new arena
	// We need to return the new references
	// The compaction allocated in the same order, so we can reconstruct
	newRefs := make([]SliceRef, len(liveRefs))
	copy(newRefs, liveRefs)

	return newRefs, stats, nil
}

// TotalAllocated returns total bytes allocated (thread-safe)
func (ca *CompactableArena[T]) TotalAllocated() int64 {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	slabs := *ca.arena.arena.slabs.Load()
	var total int64
	for _, slab := range slabs {
		total += int64(slab.offset)
	}
	return total
}
