package index

import (
	"sync/atomic"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
)

const (
	// filterMaskMaxBytes caps how much memory a single search may spend on a
	// dense filter bitmask. 256 KiB covers ~2.1M ids.
	//
	// The cap is really a cap on the conversion's fixed cost: a bitmask of
	// universe/8 bytes has to be zeroed on every search (the dense export ORs
	// array and run containers into it), which is ~26 us at 256 KiB. A search
	// probes on the order of ef*M candidates, so even the least favourable filter
	// to densify — one that roaring already stores as a single bitmap container,
	// where the probe saving is only ~9 ns — repays that within a traversal.
	// Above the cap the memclr stops being repaid, and a bitmask far larger than
	// L2 would turn every probe into an LLC miss anyway.
	filterMaskMaxBytes = 256 << 10

	// filterMaskMaxArrayValues bounds the only part of the conversion that costs
	// per admitted id. Roaring stores a block of 65536 ids either as an array
	// container (up to 4096 values, one bit set at a time during conversion) or
	// as a bitmap container, whose 1024 words are a straight memmove into the
	// dense layout; run containers are a handful of word stores. So only
	// array-container values are charged, and the amortisation point for them
	// is what this constant encodes.
	//
	// Measured on the filter probe micro-benchmark, an array-container filter
	// costs 30-85 ns per roaring probe (the per-container binary search plus the
	// binary search inside the array) against ~1-2 ns for a dense probe, while
	// conversion costs ~1.3 ns per array value. A search probes on the order of
	// ef*M, i.e. a few thousand candidates, so 65536 array values are repaid
	// after ~2k probes: comfortably inside one traversal, with an order of
	// magnitude to spare. Beyond that the conversion starts to dominate and the
	// roaring probe is the cheaper representation.
	filterMaskMaxArrayValues = 1 << 16
)

// filterMaskScratchMaxWords bounds the dense buffer a pooled search context may
// keep alive between searches. The pool holds one context per concurrent
// search, so retaining an unbounded bitmask in each of them would pin
// universe/8 bytes times the peak concurrency; conversions above this size
// allocate a fresh buffer and let the GC reclaim it.
const filterMaskScratchMaxWords = 1 << 13 // 8192 words = 64 KiB

// forceFilterMaskRoaring pins the roaring fallback so that parity tests can run
// the same filter through both probe representations. Production code never sets
// it. It is atomic because searches read it on their own goroutines.
var forceFilterMaskRoaring atomic.Bool

// filterMask is a search-scoped membership test over a filter. It holds the
// filter as a dense bitmask so that graph traversal can admit or reject a
// candidate with a single word load instead of walking the roaring container
// tree for every candidate probed.
type filterMask struct {
	// dense is the filter as a bitmask over the filter's own id universe
	// ([0, filter.Maximum()]). A non-nil but empty value means "this filter
	// admits nothing" — an empty roaring filter — and must reject every id
	// rather than be mistaken for "no filter".
	dense types.BitVector
	// roaring is the unconverted filter, used when the id universe is too wide
	// or too expensive to convert. Exactly one of dense and roaring is non-nil.
	roaring *roaring.Bitmap
}

// allows reports whether id is admitted by the filter. A nil *filterMask means
// the search is unfiltered.
func (fm *filterMask) allows(id uint32) bool {
	if fm.dense != nil {
		return fm.dense.Test(id)
	}
	return fm.roaring.Contains(id)
}

// densified reports whether the mask probes a dense bitmask.
func (fm *filterMask) densified() bool {
	return fm.dense != nil
}

// buildFilterMask converts filter for repeated probing, reusing scratch as the
// backing store for the dense bitmask where possible. It returns nil when there
// is no filter to apply, and otherwise a mask that admits exactly the ids
// filter.Contains admits. The dense buffer aliases scratch, so the mask is only
// valid until the owning search context is reused.
func buildFilterMask(filter *roaring.Bitmap, scratch types.BitVector) (*filterMask, types.BitVector) {
	if filter == nil {
		return nil, scratch
	}

	// An empty filter admits nothing. dense is deliberately non-nil so that
	// allows rejects every id instead of deferring to the roaring probe.
	if filter.IsEmpty() {
		return &filterMask{dense: types.BitVector{}}, scratch
	}

	// DenseSize is 0 only for an empty bitmap, which the check above excludes.
	words := int(filter.DenseSize()) // #nosec G115
	stats := filter.Stats()

	if forceFilterMaskRoaring.Load() || !densifyWorthwhile(words, stats) {
		return &filterMask{roaring: filter}, scratch
	}

	bv := resizeBitVector(scratch, words, words <= filterMaskScratchMaxWords)
	// WriteDenseTo is roaring's own dense export: bitmap containers are a
	// memmove of 1024 words into the identical layout, run containers a few word
	// stores, array containers the only per-id work.
	filter.WriteDenseTo(bv)
	return &filterMask{dense: bv}, bv
}

// densifyWorthwhile reports whether a bitmask of the given size, for a filter
// with the given container statistics, is a better probe representation than the
// roaring bitmap itself.
func densifyWorthwhile(words int, stats roaring.Statistics) bool {
	if words*8 > filterMaskMaxBytes {
		return false
	}
	// Only array-container values are charged to the conversion budget; the
	// bitmap and run containers convert by bulk copy.
	charged := stats.ArrayContainerValues
	if stats.RunContainers > 0 {
		charged += stats.RunContainerValues
	}
	return charged <= filterMaskMaxArrayValues
}

// resizeBitVector returns a zeroed bit vector of words words, reusing buf when
// it is large enough and keep is set. Reuse trades the allocator and the GC for
// a memclr, which for the sizes a filter spans is the cheaper half of the
// conversion cost. WriteDenseTo ORs array and run containers into the buffer, so
// the reuse path must zero it.
func resizeBitVector(buf types.BitVector, words int, keep bool) types.BitVector {
	if !keep || cap(buf) < words {
		return make(types.BitVector, words)
	}
	bv := buf[:words]
	for i := range bv {
		bv[i] = 0
	}
	return bv
}
