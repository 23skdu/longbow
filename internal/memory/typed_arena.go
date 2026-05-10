package memory

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"
)

// TypedArena wraps a SlabArena to provide typed slice access.
type TypedArena[T any] struct {
	arena atomic.Pointer[SlabArena]
	mu    sync.RWMutex
}

// NewTypedArena creates a new typed arena wrapper around a SlabArena.
func NewTypedArena[T any](arena *SlabArena) *TypedArena[T] {
	ta := &TypedArena[T]{}
	ta.arena.Store(arena)
	return ta
}

func (ta *TypedArena[T]) Free() {
	ta.Release()
}

func (ta *TypedArena[T]) Retain() {
	a := ta.arena.Load()
	if a != nil {
		a.Retain()
	}
}

func (ta *TypedArena[T]) Release() {
	a := ta.arena.Load()
	if a != nil {
		a.Release()
		ta.arena.Store(nil)
	}
}

func (ta *TypedArena[T]) Slab() *SlabArena {
	return ta.arena.Load()
}

// BumpGeneration increments the underlying arena's generation.
func (ta *TypedArena[T]) BumpGeneration() uint64 {
	a := ta.arena.Load()
	if a != nil {
		return a.BumpGeneration()
	}
	return 0
}

// SetGeneration sets the underlying arena's generation.
func (ta *TypedArena[T]) SetGeneration(gen uint64) {
	a := ta.arena.Load()
	if a != nil {
		a.generation.Store(gen)
	}
}

// TotalAllocated returns total bytes allocated in the arena.
func (ta *TypedArena[T]) TotalAllocated() int64 {
	a := ta.arena.Load()
	if a == nil {
		return 0
	}
	slabsPtr := a.slabs.Load()
	if slabsPtr == nil {
		return 0
	}
	slabs := *slabsPtr
	var total int64
	for _, s := range slabs {
		total += int64(s.offset)
	}
	return total
}

// Compact consolidates fragmented slabs in the arena.
// It copies live data to new slabs and releases old ones.
// This method is thread-safe and locks the arena for exclusive access.
func (ta *TypedArena[T]) Compact(liveRefs []SliceRef) (*CompactionStats, error) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	var zero T
	elemSize := int(unsafe.Sizeof(zero)) // #nosec G115

	totalLiveBytes := int64(0)
	for _, ref := range liveRefs {
		totalLiveBytes += int64(ref.Len) * int64(elemSize)
	}

	oldSlabs := *ta.Slab().slabs.Load()
	oldSlabCount := len(oldSlabs)
	var oldTotalBytes int64
	for _, slab := range oldSlabs {
		oldTotalBytes += int64(slab.offset)
	}

	newArena := NewSlabArena(int(ta.Slab().slabCap))
	newTypedArena := NewTypedArena[T](newArena)

	newRefs := make([]SliceRef, len(liveRefs))
	for i, oldRef := range liveRefs {
		oldData := ta.Get(oldRef)
		if oldData == nil {
			continue
		}

		newRef, err := newTypedArena.AllocSliceDirty(int(oldRef.Len))
		if err != nil {
			return nil, err
		}

		newData := newTypedArena.Get(newRef)
		copy(newData, oldData)
		newRefs[i] = newRef
	}

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

	ta.arena.Store(newArena)

	return stats, nil
}

func (ta *TypedArena[T]) AllocSlice(count int) (SliceRef, error) {
	var zero T
	elemSize := int(unsafe.Sizeof(zero)) // #nosec G115
	totalBytes := count * elemSize

	a := ta.arena.Load()
	if a == nil {
		return SliceRef{}, errors.New("arena is nil")
	}
	offset, err := a.Alloc(totalBytes)
	if err != nil {
		return SliceRef{}, err
	}

	return SliceRef{
		Offset: offset,
		Len:    uint32(count), // #nosec G115
		Cap:    uint32(count), // #nosec G115
	}, nil
}

// AllocSliceDirty allocates a slice but DOES NOT guarantee zero initialization.
// Use this only if you intend to overwrite the entire slice immediately.
func (ta *TypedArena[T]) AllocSliceDirty(count int) (SliceRef, error) {
	var zero T
	elemSize := int(unsafe.Sizeof(zero)) // #nosec G115
	totalBytes := count * elemSize

	a := ta.arena.Load()
	if a == nil {
		return SliceRef{}, errors.New("arena is nil")
	}
	offset, err := a.AllocDirty(totalBytes)
	if err != nil {
		return SliceRef{}, err
	}

	return SliceRef{
		Offset: offset,
		Len:    uint32(count), // #nosec G115
		Cap:    uint32(count), // #nosec G115
	}, nil
}

// AllocSliceAligned allocates an aligned slice.
func (ta *TypedArena[T]) AllocSliceAligned(count, align int) (SliceRef, error) {
	var zero T
	elemSize := int(unsafe.Sizeof(zero)) // #nosec G115
	totalBytes := count * elemSize

	a := ta.arena.Load()
	if a == nil {
		return SliceRef{}, errors.New("arena is nil")
	}
	offset, err := a.AllocAligned(totalBytes, align)
	if err != nil {
		return SliceRef{}, err
	}

	return SliceRef{
		Offset: offset,
		Len:    uint32(count), // #nosec G115
		Cap:    uint32(count), // #nosec G115
	}, nil
}

// Get retrieves a typed slice from the arena using a SliceRef.
func (ta *TypedArena[T]) Get(ref SliceRef) []T {
	if ref.Offset == 0 || ref.Len == 0 {
		return nil
	}

	var zero T
	elemSize := uint32(unsafe.Sizeof(zero)) // #nosec G115
	a := ta.arena.Load()
	if a == nil {
		return nil
	}
	byteSlice := a.Get(ref.Offset, ref.Len*elemSize)
	if len(byteSlice) == 0 {
		return nil
	}

	// Use unsafe.Slice instead of deprecated reflect.SliceHeader
	ptr := unsafe.Pointer(&byteSlice[0])     // #nosec G103
	return unsafe.Slice((*T)(ptr), ref.Len) // #nosec G103
}
