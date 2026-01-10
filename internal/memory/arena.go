package memory

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
)

// Common errors
var (
	ErrOOM = errors.New("arena out of memory")
)

// SliceRef is a lightweight handle to a slice allocated in the arena.
// It replaces standard []T headers to save memory (16 bytes vs 24 bytes).
type SliceRef struct {
	Offset uint64
	Len    uint32
	Cap    uint32
}

// SliceRef64 is a compact handle (just 8 bytes) if we assume smaller lengths or encoded offsets.
// For now, we stick to struct for clarity.

// SlabArena manages a list of large byte slices (slabs) to reduce allocation count.
// It uses a fixed-size array of atomic pointers to allow lock-free reads.
const MaxSlabs = 65536

type SlabArena struct {
	slabSize int
	// slabs stores pointers to []byte. We use atomic.Pointer for lock-free Get().
	slabs [MaxSlabs]atomic.Pointer[[]byte]

	mu sync.Mutex // Protects allocation (writing new slabs)

	// Current slab allocation pointer
	currentSlabIdx int
	currentOffset  uint64 // Offset within the current slab
}

// NewSlabArena creates a new arena with the given slab size (e.g., 16*1024*1024).
func NewSlabArena(slabSize int) *SlabArena {
	if slabSize <= 0 {
		slabSize = 16 * 1024 * 1024 // 16MB default
	}
	metrics.ArenaSlabsTotal.Inc()
	metrics.ArenaAllocatedBytes.WithLabelValues("slab_init").Add(float64(slabSize))

	a := &SlabArena{
		slabSize:       slabSize,
		currentOffset:  16, // Reserve 0-15
		currentSlabIdx: 0,
	}

	// Initialize first slab
	firstSlab := make([]byte, slabSize)
	a.slabs[0].Store(&firstSlab)

	return a
}

// Alloc reserves 'size' bytes and returns a global offset handle.
// GlobalOffset = (SlabIndex << 32) | SlabOffset.
func (a *SlabArena) Alloc(size int) (uint64, error) {
	if size > a.slabSize {
		return 0, errors.New("allocation larger than slab size")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// 1. Get current slab
	// We can access directly via Load() or assume consistency since we hold Lock()
	// and are the only writer.
	currentSlabPtr := a.slabs[a.currentSlabIdx].Load()
	if currentSlabPtr == nil {
		// Should not happen if initialized correctly, but handle safety
		newSlab := make([]byte, a.slabSize)
		a.slabs[a.currentSlabIdx].Store(&newSlab)
		currentSlabPtr = &newSlab
	}

	// 2. Check fit
	needed := uint64(size)
	if a.currentOffset+needed > uint64(len(*currentSlabPtr)) {
		// 3. Alloc new slab
		if a.currentSlabIdx >= MaxSlabs-1 {
			return 0, ErrOOM
		}

		newSlab := make([]byte, a.slabSize)
		a.currentSlabIdx++
		a.slabs[a.currentSlabIdx].Store(&newSlab)
		a.currentOffset = 0 // Reset offset for new slab

		metrics.ArenaSlabsTotal.Inc()
		metrics.ArenaAllocatedBytes.WithLabelValues("slab_grow").Add(float64(a.slabSize))
	}

	// 4. Alloc
	offset := a.currentOffset
	a.currentOffset += needed

	// 5. Pack Global Offset
	idx := uint64(a.currentSlabIdx)
	globalOffset := (idx << 32) | offset

	return globalOffset, nil
}

// Get returns the byte slice for a given global offset and length.
// Lock-Free (Wait-Free) implementation.
func (a *SlabArena) Get(globalOffset uint64, size int) []byte {
	slabIdx := int(globalOffset >> 32)
	slabOffset := int(globalOffset & 0xFFFFFFFF)

	if slabIdx >= MaxSlabs {
		return nil
	}

	// Atomic Load - No Lock!
	slabPtr := a.slabs[slabIdx].Load()
	if slabPtr == nil {
		return nil
	}

	slab := *slabPtr
	if slabOffset+size > len(slab) {
		return nil
	}

	return slab[slabOffset : slabOffset+size]
}

// TypedArena wraps SlabArena for a specific type T.
type TypedArena[T any] struct {
	arena    *SlabArena
	elemSize int
}

func NewTypedArena[T any](arena *SlabArena) *TypedArena[T] {
	var zero T
	return &TypedArena[T]{
		arena:    arena,
		elemSize: int(unsafe.Sizeof(zero)),
	}
}

// AllocSlice allocates space for 'len' elements of type T.
func (t *TypedArena[T]) AllocSlice(length int) (SliceRef, error) {
	sizeBytes := length * t.elemSize
	offset, err := t.arena.Alloc(sizeBytes)
	if err != nil {
		return SliceRef{}, err
	}
	return SliceRef{
		Offset: offset,
		Len:    uint32(length),
		Cap:    uint32(length),
	}, nil
}

// Get returns a Go slice []T mapped to the arena memory.
// WARNING: The returned slice is valid only as long as the arena is valid.
func (t *TypedArena[T]) Get(ref SliceRef) []T {
	if ref.Len == 0 {
		return nil
	}
	sizeBytes := int(ref.Len) * t.elemSize
	bytes := t.arena.Get(ref.Offset, sizeBytes)
	if bytes == nil {
		return nil
	}

	// Unsafe cast []byte -> []T
	// Only safe if alignment matches.
	// For float32/uint32 (4 bytes), alignment is usually fine if slab is aligned?
	// make([]byte) usually returns pointer aligned to 8 or 16 bytes.
	// Offsets might be unaligned if we mixed types.
	// But our Alloc is sequential. If we always alloc 4-byte multiples, we stay aligned.

	return unsafe.Slice((*T)(unsafe.Pointer(&bytes[0])), ref.Len)
}
