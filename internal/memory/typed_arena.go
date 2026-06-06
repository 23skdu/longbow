package memory

import (
	"errors"
	"math"
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
	// Do NOT nil out the arena pointer here. The Slab has its own
	// ref-count (SlabArena.refs); it stays alive as long as any
	// TypedArena holds a Retain() (e.g. a Clone of a GraphData that
	// was made before this Release). Nilling the pointer would cause
	// concurrent AllocSlice callers in other goroutines to fail
	// with "arena is nil" even though the underlying Slab is still
	// live and valid. The GraphData.AcquireReader/ReleaseReader pin
	// guarantees the Slab is not freed while readers are in-flight.
	a := ta.arena.Load()
	if a != nil {
		a.Release()
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
	return a.TotalAllocated()
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
	return ta.GetWithGeneration(ref, math.MaxUint64)
}

// GetWithGeneration retrieves a typed slice from the arena, enforcing generation isolation.
// When maxGeneration is math.MaxUint64, generation isolation is bypassed for performance.
func (ta *TypedArena[T]) GetWithGeneration(ref SliceRef, maxGeneration uint64) []T {
	if ref.Len == 0 {
		return nil
	}

	var zero T
	elemSize := uint32(unsafe.Sizeof(zero)) // #nosec G115
	a := ta.arena.Load()
	if a == nil {
		return nil
	}

	// Fast path: bypass generation check when not needed (committed data)
	if maxGeneration == math.MaxUint64 {
		byteSlice := a.Get(ref.Offset, ref.Len*elemSize)
		if len(byteSlice) == 0 {
			return nil
		}
		ptr := unsafe.Pointer(&byteSlice[0])    // #nosec G103
		return unsafe.Slice((*T)(ptr), ref.Len) // #nosec G103
	}

	byteSlice := a.GetWithGeneration(ref.Offset, ref.Len*elemSize, maxGeneration)
	if len(byteSlice) == 0 {
		return nil
	}

	ptr := unsafe.Pointer(&byteSlice[0])    // #nosec G103
	return unsafe.Slice((*T)(ptr), ref.Len) // #nosec G103
}
