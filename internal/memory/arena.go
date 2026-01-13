package memory

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
)

// SliceRef is a handle to a slice in the arena.
// It is used by TypedArena and external consumers.
type SliceRef struct {
	Offset uint64
	Len    uint32
	Cap    uint32
}

func (r SliceRef) IsNil() bool {
	return r.Offset == 0 && r.Len == 0
}

type slab struct {
	id     uint32
	data   []byte
	offset uint32 // current allocation pointer (relative to slab)
}

// SlabArena manages large blocks of memory.
type SlabArena struct {
	mu      sync.Mutex              // Only guards Alloc (writes)
	slabs   atomic.Pointer[[]*slab] // Lock-free access to slabs slice
	slabCap uint32                  // capacity in BYTES
}

// NewSlabArena creates a new arena with specified slab byte size.
func NewSlabArena(slabSizeBytes int) *SlabArena {
	if slabSizeBytes < 1024 {
		slabSizeBytes = 1024
	}
	// Ensure alignment? For now, we assume simple byte allocation.
	// 4KB or 2MB alignment is good.
	s := &SlabArena{
		slabCap: uint32(slabSizeBytes),
	}
	// Initialize with empty slice
	empty := make([]*slab, 0)
	s.slabs.Store(&empty)
	return s
}

// Alloc reserves space for 'size' bytes.
// Returns a GLOBAL offset.
func (a *SlabArena) Alloc(size int) (uint64, error) {
	if size <= 0 {
		return 0, errors.New("alloc size must be positive")
	}
	needed := uint32(size)
	if needed > a.slabCap {
		// For huge allocations, we strictly might fail or support header-based huge slabs.
		// For now fail.
		return 0, fmt.Errorf("alloc request %d exceeds slab capacity %d", size, a.slabCap)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Load current state
	currentSlabsPtr := a.slabs.Load()
	currentSlabs := *currentSlabsPtr

	var active *slab
	if len(currentSlabs) > 0 {
		active = currentSlabs[len(currentSlabs)-1]
	}

	// Simple bump allocator within slab
	// TODO: Alignment padding? (e.g. 8-byte align)
	// Let's force 8-byte alignment for safety of typed access
	const align = 8

	if active != nil {
		pad := (align - (active.offset % align)) % align
		if active.offset+pad+needed <= uint32(len(active.data)) {
			active.offset += pad // consume padding
		}
		// If won't fit, skip to next slab
	}

	// Check fit
	if active == nil || !canFit(active, needed, align) {
		newSlab := &slab{
			id:     uint32(len(currentSlabs) + 1), // ID starts at 1
			data:   make([]byte, a.slabCap),
			offset: 0,
		}
		// Copy-On-Write for lock-free readers
		newSlabs := make([]*slab, len(currentSlabs)+1)
		copy(newSlabs, currentSlabs)
		newSlabs[len(currentSlabs)] = newSlab

		// Publish new state
		a.slabs.Store(&newSlabs)

		active = newSlab

		metrics.ArenaSlabsTotal.Inc()
		metrics.ArenaAllocatedBytes.WithLabelValues("slab").Add(float64(a.slabCap))
	}

	// Calc aligned offset
	pad := (align - (active.offset % align)) % align
	active.offset += pad

	start := active.offset
	active.offset += needed

	// Result = (SlabIndex * SlabCap) + LocalOffset
	// Slab index is (ID-1).
	slabIdx := uint64(active.id - 1)
	globalOffset := (slabIdx * uint64(a.slabCap)) + uint64(start)

	// Offset 0 is reserved for nil, so strict usage requires start > 0?
	// Or we make sure Slab 1 starts at Offset 1?
	// If GlobalOffset == 0, is it nil?
	// Slab 1, Offset 0 -> Global 0.
	// We should offset by specific base or treat 0 as nil.
	// Let's add +1 to global offset or something?
	// Existing code checks `offset == 0` as nil.
	// So valid offsets must be > 0.
	// Since Slab 1, Offset 0 is 0. We should introduce `offsetBase = 1`.
	// Or just verify we never return 0.
	if globalOffset == 0 {
		// This happens if Slab 1, Offset 0.
		// We can burn the first byte of Slab 1.
		if active.id == 1 && start == 0 {
			active.offset++ // burn byte 0
			globalOffset++
		}
	}

	return globalOffset, nil
}

func canFit(s *slab, needed, align uint32) bool {
	pad := (align - (s.offset % align)) % align
	return s.offset+pad+needed <= uint32(len(s.data))
}

// Get returns the byte slice.
func (a *SlabArena) Get(offset uint64, length uint32) []byte {
	if offset == 0 || length == 0 {
		return nil
	}

	slabIdx := offset / uint64(a.slabCap)
	localOffset := uint32(offset % uint64(a.slabCap))

	// Lock-free read
	slabsPtr := a.slabs.Load()
	slabs := *slabsPtr

	if int(slabIdx) >= len(slabs) {
		return nil
	}

	s := slabs[slabIdx]
	if uint64(localOffset)+uint64(length) > uint64(len(s.data)) {
		return nil
	}

	return s.data[localOffset : localOffset+length]
}

// GetPointer returns unsafe.Pointer to the data.
// Use with caution.
// GetPointer returns unsafe.Pointer to the data.
// Use with caution.
func (a *SlabArena) GetPointer(offset uint64) unsafe.Pointer {
	if offset == 0 {
		return nil
	}
	slabIdx := offset / uint64(a.slabCap)
	localOffset := uint32(offset % uint64(a.slabCap))

	// Lock-free read
	slabsPtr := a.slabs.Load()
	slabs := *slabsPtr

	if int(slabIdx) >= len(slabs) {
		return nil
	}
	s := slabs[slabIdx]
	return unsafe.Pointer(&s.data[localOffset])
}
