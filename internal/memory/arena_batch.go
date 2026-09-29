package memory

import (
	"math"
	"unsafe"
)

// SlabBatch is a short-lived, batch-scoped view of a SlabArena.
//
// It resolves the slab table and the generation policy exactly once and then
// hands out per-vector slices from a hot-slab cache, replacing the per-call
// atomic table load, slab-index division, slab pointer chase and generation
// comparison performed by GetWithGeneration.
//
// Visibility rules are identical to GetWithGeneration:
//   - a zero length yields nil;
//   - offsets past the last slab yield nil;
//   - placeholder slots (oversized allocations) resolve through the same
//     backwards scan to the owning real slab, with the same uint32 offset
//     arithmetic;
//   - a slab is visible only when maxGeneration is math.MaxUint64 or the
//     slab's generation is <= maxGeneration;
//   - ranges that run past the owning slab's data are rejected.
//
// A SlabBatch is a value, never a global, so it cannot be silently shared
// across a generation bump or an arena mutation. Slab generations are
// immutable, so bumping the arena generation never retroactively changes what
// an already-resolved batch returns; newly allocated data lands in newer slabs
// that a batch holding an older maxGeneration still cannot see. Structural
// arena mutations (Free, Compact, Load, LoadMmap, ConvertToOffHeap, or a
// concurrent Alloc that appends a slab) replace the slab table, and Get
// detects that by identity on every call and re-resolves, so a batch can
// never hand out data that the reference accessor would refuse.
//
// A batch, and every slice obtained from it, is only valid inside the reader
// pin that guards the arena (GraphData.AcquireReader/ReleaseReader), exactly
// as the slices returned by GetWithGeneration are.
type SlabBatch struct {
	arena     *SlabArena
	slabs     []*slab
	table     *[]*slab
	slabCap   uint64
	slabCap32 uint32
	maxGen    uint64
	checkGen  bool

	hotBase uint64
	hotSpan uint64
	hotData []byte
	hotVis  bool
}

// BeginBatch opens a batch-scoped view of the arena for reads that share a
// single maxGeneration. The result is meant to live in a local variable for
// the duration of one batch.
func (a *SlabArena) BeginBatch(maxGeneration uint64) SlabBatch {
	if a == nil {
		return SlabBatch{maxGen: maxGeneration, checkGen: maxGeneration != math.MaxUint64}
	}
	b := SlabBatch{
		arena:     a,
		slabCap:   uint64(a.slabCap),
		slabCap32: a.slabCap,
		maxGen:    maxGeneration,
		checkGen:  maxGeneration != math.MaxUint64,
	}
	if ptr := a.slabs.Load(); ptr != nil {
		b.slabs = *ptr
		b.table = ptr
	}
	return b
}

// Stale reports whether the arena's slab table was replaced after the batch
// resolved it. Get re-resolves transparently, so Stale is only needed by
// callers that want to observe an arena mutation mid-batch.
func (b *SlabBatch) Stale() bool {
	return b.arena == nil || b.arena.slabs.Load() != b.table
}

// MaxGeneration returns the generation the batch was opened with.
func (b *SlabBatch) MaxGeneration() uint64 {
	return b.maxGen
}

// Get returns the bytes for one vector under the batch's generation policy.
func (b *SlabBatch) Get(offset uint64, length uint32) []byte {
	if b.arena == nil || length == 0 {
		return nil
	}
	if local := offset - b.hotBase; local < b.hotSpan && b.arena.slabs.Load() == b.table {
		if !b.hotVis {
			return nil
		}
		l := uint32(local)                     // #nosec G115
		if length > uint32(len(b.hotData))-l { // #nosec G115
			return nil
		}
		return b.hotData[l : l+length]
	}
	return b.resolve(offset, length)
}

func (b *SlabBatch) resolve(offset uint64, length uint32) []byte {
	if b.arena == nil || b.slabCap == 0 {
		return nil
	}
	if b.arena.slabs.Load() != b.table {
		b.refresh()
	}

	slabIdx := offset / b.slabCap
	if int(slabIdx) >= len(b.slabs) { // #nosec G115
		return nil
	}
	s := b.slabs[slabIdx]

	if s.data == nil {
		var realSlab *slab
		var realIdx int
		for j := int(slabIdx); j >= 0; j-- { // #nosec G115
			if b.slabs[j].data != nil {
				realSlab = b.slabs[j]
				realIdx = j
				break
			}
		}
		if realSlab == nil {
			return nil
		}
		visible := !b.checkGen || realSlab.generation <= b.maxGen
		b.prime(uint64(realIdx)*b.slabCap, uint64(len(realSlab.data)), realSlab.data, visible) // #nosec G115 -- realIdx >= 0
		if !visible {
			return nil
		}
		localOffset := uint32(offset & (b.slabCap - 1))                          // #nosec G115
		adjustedOffset := localOffset + uint32(int(slabIdx)-realIdx)*b.slabCap32 // #nosec G115 -- mirrors GetWithGeneration
		if uint64(adjustedOffset)+uint64(length) > uint64(len(realSlab.data)) {
			return nil
		}
		return realSlab.data[adjustedOffset : adjustedOffset+length]
	}

	visible := !b.checkGen || s.generation <= b.maxGen
	b.prime(uint64(slabIdx)*b.slabCap, b.slabCap, s.data, visible) // #nosec G115 -- slabIdx < len(slabs) checked above
	if !visible {
		return nil
	}
	localOffset := uint32(offset & (b.slabCap - 1)) // #nosec G115
	if uint64(localOffset)+uint64(length) > uint64(len(s.data)) {
		return nil
	}
	return s.data[localOffset : localOffset+length]
}

// prime caches the slab that covers global offsets [base, base+span) so that
// every vector in it resolves with a range check instead of a table walk. span
// is clamped to the slab's data length, which keeps the hot-range test a
// sufficient bound for the slice expression below.
func (b *SlabBatch) prime(base, span uint64, data []byte, visible bool) {
	if span > uint64(len(data)) {
		span = uint64(len(data))
	}
	b.hotBase = base
	b.hotSpan = span
	b.hotData = data
	b.hotVis = visible
}

func (b *SlabBatch) refresh() {
	b.hotBase, b.hotSpan, b.hotData, b.hotVis = 0, 0, nil, false
	b.slabCap = uint64(b.arena.slabCap)
	b.slabCap32 = b.arena.slabCap
	if ptr := b.arena.slabs.Load(); ptr != nil {
		b.slabs = *ptr
		b.table = ptr
		return
	}
	b.slabs = nil
	b.table = nil
}

// TypedBatch is the typed-arena counterpart of SlabBatch.
type TypedBatch[T any] struct {
	owner *TypedArena[T]
	raw   SlabBatch
	elem  uint32
}

// BeginBatch opens a batch-scoped typed view, resolving the underlying arena,
// its slab table and the generation policy once.
func (ta *TypedArena[T]) BeginBatch(maxGeneration uint64) TypedBatch[T] {
	var zero T
	b := TypedBatch[T]{elem: uint32(unsafe.Sizeof(zero))} // #nosec G115
	if ta == nil {
		return b
	}
	a := ta.arena.Load()
	if a == nil {
		return b
	}
	b.owner = ta
	b.raw = a.BeginBatch(maxGeneration)
	return b
}

// Stale reports whether the batch no longer tracks the arena it was opened on.
func (b *TypedBatch[T]) Stale() bool {
	return b.owner == nil || b.owner.arena.Load() != b.raw.arena || b.raw.Stale()
}

// Get returns the typed slice for ref, applying the same rules as
// TypedArena.GetWithGeneration at the batch's generation.
func (b *TypedBatch[T]) Get(ref SliceRef) []T {
	if ref.Len == 0 {
		return nil
	}
	if b.owner != nil {
		if a := b.owner.arena.Load(); a != b.raw.arena {
			if a == nil {
				b.raw = SlabBatch{maxGen: b.raw.maxGen, checkGen: b.raw.checkGen}
			} else {
				b.raw = a.BeginBatch(b.raw.maxGen)
			}
		}
	}
	byteSlice := b.raw.Get(ref.Offset, ref.Len*b.elem)
	if len(byteSlice) == 0 {
		return nil
	}
	ptr := unsafe.Pointer(&byteSlice[0])    // #nosec G103
	return unsafe.Slice((*T)(ptr), ref.Len) // #nosec G103
}
