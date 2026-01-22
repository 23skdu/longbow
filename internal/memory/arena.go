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

// ArenaStats holds memory usage information about the arena.
type ArenaStats struct {
	TotalCapacity int64
	UsedBytes     int64
}

// SlabArena manages large blocks of memory.
type SlabArena struct {
	mu      sync.Mutex              // Only guards Alloc (writes)
	slabs   atomic.Pointer[[]*slab] // Lock-free access to slabs slice
	slabCap uint32                  // capacity in BYTES
}

// Stats returns the total capacity and used bytes in the arena.
func (a *SlabArena) Stats() ArenaStats {
	slabsPtr := a.slabs.Load()
	if slabsPtr == nil {
		return ArenaStats{}
	}
	slabs := *slabsPtr
	stats := ArenaStats{
		TotalCapacity: int64(len(slabs)) * int64(a.slabCap),
	}
	// Note: We need to sum up used portions. This is a bit slow but okay for tuning.
	// For production, we might want to track this atomically.
	for _, s := range slabs {
		stats.UsedBytes += int64(s.offset)
	}
	return stats
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

	// Register with global registry for GC tuning
	RegisterArena(s)

	return s
}

// Alloc reserves space for 'size' bytes.
// Returns a GLOBAL offset.
// Guarantees zero-initialized memory.
func (a *SlabArena) Alloc(size int) (uint64, error) {
	// Fast path for small allocations (≤ 64 bytes)
	if size <= 64 {
		offset, ok := a.allocFast(size)
		if ok {
			metrics.ArenaFastPathTotal.Inc()
			return offset, nil
		}
		metrics.ArenaFastPathFailedTotal.Inc()
	}
	// Slow path for larger allocations or when fast path fails
	metrics.ArenaSlowPathTotal.Inc()
	return a.allocCommon(size, true)
}

// AllocDirty reserves space for 'size' bytes.
// Returns a GLOBAL offset.
// MEMORY IS NOT GUARANTEED TO BE ZEROED.
// Use this only when you will immediately overwrite the entire range.
func (a *SlabArena) AllocDirty(size int) (uint64, error) {
	// Reverting optimization for stability: Force zeroing
	return a.allocCommon(size, true)
}

// allocFast is a lock-free fast path for small allocations (≤ 64 bytes).
// Returns (globalOffset, true) on success, (0, false) on failure.
func (a *SlabArena) allocFast(size int) (uint64, bool) {
	const align = 8
	needed := uint32(size)
	pad := (align - (needed % align)) % align
	totalNeeded := needed + pad

	for {
		slabsPtr := a.slabs.Load()
		slabs := *slabsPtr

		if len(slabs) == 0 {
			return 0, false
		}

		active := slabs[len(slabs)-1]

		oldOffset := atomic.LoadUint32(&active.offset)
		newOffset := oldOffset + totalNeeded

		if newOffset > uint32(len(active.data)) {
			return 0, false
		}

		if atomic.CompareAndSwapUint32(&active.offset, oldOffset, newOffset) {
			start := oldOffset

			if start == 0 && active.id == 1 {
				start += align
				atomic.AddUint32(&active.offset, align)
			}

			globalOffset := (uint64(active.id-1) * uint64(a.slabCap)) + uint64(start)
			return globalOffset, true
		}
	}
}

func (a *SlabArena) allocCommon(size int, zero bool) (uint64, error) {
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
		} else {
			// Won't fit, need new slab
			active = nil
		}
	}

	// Check fit (active might have been nil'd above if full)
	if active == nil {
		// Allocate new slab using local helper or pool
		buf := GetSlab(int(a.slabCap))

		newSlab := &slab{
			id:     uint32(len(currentSlabs) + 1), // ID starts at 1
			data:   buf,
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
	// Offset 0 is reserved for nil. For the very first slab and very first allocation,
	// we burn 'align' bytes to ensure we move away from 0 while maintaining alignment.
	if start == 0 && active.id == 1 {
		active.offset += uint32(align)
		start = active.offset
	}

	active.offset += needed

	// ZEROING LOGIC
	if zero {
		// Zero the allocated range
		// If the slab came from 'make', it's already zeroed IF we haven't used it.
		// But if we reuse slabs, or if we mix clean/dirty allocs in same slab,
		// we MUST explicitly zero here to be safe.
		// Go's built-in 'clear' or simple loop?
		// For small n, loop is fine. For large n, copy/clear is fine.
		// 'make' provides zeroed memory, but we can't track easily if this specific range is fresh.
		// Pessimistic zeroing is safest when pooling.
		// Given we want to optimize 'AllocDirty', we accept cost in 'Alloc'.
		clear(active.data[start : start+needed])
	}

	// Result = (SlabIndex * SlabCap) + LocalOffset
	// Slab index is (ID-1).
	slabIdx := uint64(active.id - 1)
	globalOffset := (slabIdx * uint64(a.slabCap)) + uint64(start)

	return globalOffset, nil
}

// Free releases all slabs back to the pool.
// The arena must not be used after this.
func (a *SlabArena) Free() {
	a.mu.Lock()
	defer a.mu.Unlock()

	currentSlabsPtr := a.slabs.Load()
	if currentSlabsPtr == nil {
		return
	}
	currentSlabs := *currentSlabsPtr

	for _, s := range currentSlabs {
		PutSlab(s.data)
		s.data = nil // Help GC
	}

	// Reset (optional, if we want to reuse arena struct)
	empty := make([]*slab, 0)
	a.slabs.Store(&empty)

	// metrics.ArenaSlabsTotal is a Counter, we cannot decrement.
	// If we want to track active slabs, we need a separate Gauge.
	// For now, just ignore decrement.
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
