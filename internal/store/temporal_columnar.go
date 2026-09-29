package store

import (
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
)

// temporalBoundsVariant selects the sorted-search kernel used by a columnar
// temporal snapshot to turn a timestamp range into a group range. Variants
// other than temporalBoundsAuto exist so tests and benchmarks can force each
// kernel independently of the auto-dispatch thresholds.
type temporalBoundsVariant int

const (
	// temporalBoundsAuto picks the widest kernel the platform and slice size
	// justify.
	temporalBoundsAuto temporalBoundsVariant = iota
	// temporalBoundsScalar is the portable one-comparison-per-level search.
	temporalBoundsScalar
	// temporalBoundsUnrolled is the portable search probing four timestamps
	// per iteration.
	temporalBoundsUnrolled
	// temporalBoundsSIMD is the AVX2 VPCMPGTQ kernel; it degrades to the
	// scalar kernel when the CPU lacks AVX2.
	temporalBoundsSIMD
)

// lowerBoundScalar returns the index of the first element of the ascending
// slice ts that is >= x, or len(ts) when every element is < x.
//
// The loop keeps the candidate answer in the half-open window [base, base+cnt]
// and narrows it with exactly one comparison per level, so the descent is
// log2(n) dependent loads deep. It is the same schedule sort.Search uses, with
// the per-level closure call and the predicate indirection removed.
func lowerBoundScalar(ts []int64, x int64) int {
	cnt := len(ts)
	if cnt == 0 {
		return 0
	}
	if ts[0] >= x {
		return 0
	}
	base := 0
	for cnt > 1 {
		half := cnt >> 1
		if ts[base+half-1] < x {
			base += half
			cnt -= half
		} else {
			cnt = half
		}
	}
	if ts[base] < x {
		return base + 1
	}
	return base
}

// lowerBoundUnrolled narrows the search window four timestamps at a time using
// the same window-halving schedule as lowerBoundScalar. The extra probes
// shorten the dependency chain at the cost of a wider effective step, which
// helps for large arrays that are already cache-resident.
func lowerBoundUnrolled(ts []int64, x int64) int {
	cnt := len(ts)
	if cnt < 4 {
		return lowerBoundScalar(ts, x)
	}
	if ts[0] >= x {
		return 0
	}
	base := 0
	for cnt >= 4 {
		half := (cnt >> 1) &^ 3
		if half == 0 {
			break
		}
		p := base + half
		if ts[p-4] < x && ts[p-3] < x && ts[p-2] < x && ts[p-1] < x {
			base = p
			cnt -= half
		} else {
			cnt = half
		}
	}
	for cnt > 0 {
		if ts[base] >= x {
			return base
		}
		base++
		cnt--
	}
	return base
}

// lowerBoundSelect runs the kernel chosen by the variant.
//
// The auto dispatcher deliberately picks the scalar kernel. The AVX2 kernel in
// temporal_colsort_amd64.s is a correct, portable implementation of the
// roadmap's _mm256_cmpgt_epi64 lower bound and it agrees with the scalar result
// exactly, but it does not pay off here, for two measured reasons:
//
//  1. A lower bound is bound by the latency of its chain of dependent loads
//     (one per level, log2(n) levels), not by comparison throughput. The
//     four-wide probe removes comparisons that a scalar lower bound never
//     performs, so the only thing left to win is instruction count, which is
//     not the critical path.
//  2. The comparison cannot be made branchless in portable Go: cmd/compile
//     deliberately declines to emit CMOV for loop conditionals, so a
//     "branchless" Go lower bound is still a predicted branch, and the asm
//     kernel pays an ABI0 call, a CPUID feature check and a VZEROUPPER on
//     every exit.
//
// On the development machine the two kernels are within noise of each other up
// to n = 4096 and the AVX2 kernel is 10-40% slower from n = 1024 up, so the auto
// path takes the scalar kernel. temporalBoundsSIMD remains available, and
// BenchmarkTemporalColumnarLowerBound re-measures all three on any hardware.
func lowerBoundSelect(v temporalBoundsVariant, ts []int64, x int64) int {
	switch v {
	case temporalBoundsScalar:
		return lowerBoundScalar(ts, x)
	case temporalBoundsUnrolled:
		return lowerBoundUnrolled(ts, x)
	case temporalBoundsSIMD:
		if temporalHasAVX2 {
			return temporalLowerBoundAVX2(ts, x)
		}
		return lowerBoundScalar(ts, x)
	default:
		return lowerBoundScalar(ts, x)
	}
}

// temporalColumnarIndex is an immutable, column-oriented snapshot of a
// TemporalTree.
//
// The chunked TemporalTree stores timestamps as an array of nodes interleaved
// with arena offsets, so answering a range query means walking leaves and
// copying multi-kilobyte leaf structures. The snapshot flattens that into
// struct-of-arrays columns:
//
//	ts        distinct timestamps, ascending            (int64 column)
//	groupOff  groupOff[i]..groupOff[i+1] indexes ids/norms (uint32 column)
//	ids       entry ids in (timestamp, insertion) order   (uint64 column)
//	norms     entry norms in the same order              (float32 column)
//
// groupOff is a prefix sum, so the number of entries covered by a group range
// is a subtraction and the result slice can be allocated exactly once. A
// snapshot is published through an atomic pointer and never mutated afterwards,
// so concurrent readers need no lock at all.
type temporalColumnarIndex struct {
	ts       []int64
	groupOff []uint32
	ids      []uint64
	norms    []float32
	entries  int
	variant  temporalBoundsVariant
}

// buildTemporalColumnarIndexLocked flattens the chunked tree into columns. The
// caller must hold the tree's write lock so that no insert can race the walk.
//
// The first pass reads only the node metadata to size every column exactly, so
// the second pass never reallocates and never copies a column it has already
// filled. The first pass also warms the leaves, which the second pass then reads
// from cache.
func buildTemporalColumnarIndexLocked(tt *TemporalTree, variant temporalBoundsVariant) *temporalColumnarIndex {
	groups, entries := 0, 0
	for i := range tt.leafRefs {
		leaves := tt.leafArena.Get(tt.leafRefs[i].Ref)
		if len(leaves) == 0 {
			continue
		}
		leaf := &leaves[0]
		groups += int(leaf.Len)
		for j := uint32(0); j < leaf.Len; j++ {
			entries += int(leaf.Nodes[j].Len)
		}
	}

	idx := &temporalColumnarIndex{
		ts:       make([]int64, 0, groups),
		groupOff: make([]uint32, 1, groups+1),
		ids:      make([]uint64, 0, entries),
		norms:    make([]float32, 0, entries),
		entries:  entries,
		variant:  variant,
	}

	for li := range tt.leafRefs {
		leaves := tt.leafArena.Get(tt.leafRefs[li].Ref)
		if len(leaves) == 0 {
			continue
		}
		leaf := &leaves[0]
		for j := uint32(0); j < leaf.Len; j++ {
			node := &leaf.Nodes[j]
			idx.ts = append(idx.ts, node.Timestamp)
			entries := tt.entryArena.Get(memory.SliceRef{
				Offset: uint64(node.Offset), // #nosec G115
				Len:    node.Len,
				Cap:    node.Len,
			})
			for k := range entries {
				idx.ids = append(idx.ids, entries[k].ID)
				idx.norms = append(idx.norms, entries[k].Norm)
			}
			idx.groupOff = append(idx.groupOff, uint32(len(idx.ids))) // #nosec G115
		}
	}

	idx.entries = len(idx.ids)
	return idx
}

// rebuildColumnarIndex forces a fresh columnar snapshot and publishes it.
func (tt *TemporalTree) rebuildColumnarIndex() {
	tt.columnarBuild.Lock()
	defer tt.columnarBuild.Unlock()

	tt.mu.Lock()
	tt.columnar.Store(buildTemporalColumnarIndexLocked(tt, temporalBoundsAuto))
	tt.columnarStale.Store(0)
	tt.columnarMisses.Store(0)
	tt.columnarDirty.Store(false)
	tt.mu.Unlock()
}

// columnarSnapshot returns a snapshot that is guaranteed to contain every
// insert performed so far, or nil when the caller must fall back to the chunked
// walk. A nil result is always correct to treat as "no snapshot"; a stale
// snapshot never is, which is why inserts flip columnarDirty before touching the
// arenas and why the rebuild below runs under the tree's write lock.
func (tt *TemporalTree) columnarSnapshot() *temporalColumnarIndex {
	if tt.columnarOff.Load() {
		return nil
	}
	if !tt.columnarDirty.Load() {
		return tt.columnar.Load()
	}

	tt.columnarBuild.Lock()
	defer tt.columnarBuild.Unlock()

	if !tt.columnarDirty.Load() {
		return tt.columnar.Load()
	}

	// Rebuilding is O(n), so it is amortized over a growth window. The miss
	// counter bounds how long a read-heavy workload keeps paying the chunked
	// walk after a single write.
	threshold := int64(temporalColumnarRebuildMin)
	if nodes := int64(tt.nodeCount.Load()); nodes > threshold*temporalColumnarRebuildFactor {
		threshold = nodes / temporalColumnarRebuildFactor
	}
	if tt.columnarStale.Load() < threshold && tt.columnarMisses.Load() < temporalColumnarMissBudget {
		tt.columnarMisses.Add(1)
		return nil
	}

	tt.mu.Lock()
	snap := buildTemporalColumnarIndexLocked(tt, temporalBoundsAuto)
	tt.columnar.Store(snap)
	tt.columnarStale.Store(0)
	tt.columnarMisses.Store(0)
	tt.columnarDirty.Store(false)
	tt.mu.Unlock()
	return snap
}

const (
	// temporalColumnarRebuildMin is the smallest number of inserted timestamps
	// that justifies a snapshot rebuild.
	temporalColumnarRebuildMin = 64
	// temporalColumnarRebuildFactor spreads rebuild cost over a 1/8 growth
	// window.
	temporalColumnarRebuildFactor = 8
	// temporalColumnarMissBudget caps how many chunked fallbacks happen after a
	// write before the next query rebuilds regardless of staleness.
	temporalColumnarMissBudget = 64
)

// markColumnarDirty invalidates the published snapshot. It must be called
// before the insert mutates the arenas.
func (tt *TemporalTree) markColumnarDirty() {
	tt.columnarDirty.Store(true)
	tt.columnarStale.Add(1)
}

// columnarIndex is the snapshot currently published for the tree, or nil.
func (tt *TemporalTree) columnarIndex() *temporalColumnarIndex {
	return tt.columnar.Load()
}

// setColumnarDisabled turns the columnar query path off, forcing every range
// query back onto the chunked walk. It is the kill switch the parity tests use
// to obtain a reference result from the same tree state.
func (tt *TemporalTree) setColumnarDisabled(disabled bool) {
	tt.columnarOff.Store(disabled)
}

// search returns the index of the first group whose timestamp is >= x.
func (c *temporalColumnarIndex) search(x int64) int {
	return lowerBoundSelect(c.variant, c.ts, x)
}

// searchAfter returns the index of the first group whose timestamp is > x.
// Timestamps are distinct within a group column, so the strict upper bound is
// the lower bound advanced past an exact hit.
func (c *temporalColumnarIndex) searchAfter(x int64) int {
	i := c.search(x)
	if i < len(c.ts) && c.ts[i] == x {
		i++
	}
	return i
}

// groupRange converts the inclusive timestamp range [start, end] into the
// half-open group range covering it. Reversed or out-of-tree bounds collapse to
// an empty range.
func (c *temporalColumnarIndex) groupRange(start, end int64) (int, int) {
	if len(c.ts) == 0 {
		return 0, 0
	}
	lo := c.search(start)
	hi := c.searchAfter(end)
	if hi <= lo {
		return 0, 0
	}
	return lo, hi
}

// entryRange converts a half-open group range into the half-open entry range it
// covers.
func (c *temporalColumnarIndex) entryRange(lo, hi int) (int, int) {
	return int(c.groupOff[lo]), int(c.groupOff[hi])
}

// getRange returns every entry whose timestamp lies in [start, end], ascending.
func (c *temporalColumnarIndex) getRange(start, end int64) []uint64 {
	lo, hi := c.groupRange(start, end)
	if hi <= lo {
		return nil
	}
	metrics.TemporalQueryScannedNodesTotal.Add(float64(hi - lo))
	s, e := c.entryRange(lo, hi)
	out := make([]uint64, e-s)
	copy(out, c.ids[s:e])
	return out
}

// getRangeReversed returns every entry whose timestamp lies in [start, end],
// newest first and, within one timestamp, in reverse insertion order.
func (c *temporalColumnarIndex) getRangeReversed(start, end int64) []uint64 {
	lo, hi := c.groupRange(start, end)
	if hi <= lo {
		return nil
	}
	metrics.TemporalQueryScannedNodesTotal.Add(float64(hi - lo))
	s, e := c.entryRange(lo, hi)
	out := make([]uint64, e-s)
	pos := 0
	for i := hi - 1; i >= lo; i-- {
		gs, ge := int(c.groupOff[i]), int(c.groupOff[i+1])
		for k := ge - 1; k >= gs; k-- {
			out[pos] = c.ids[k]
			pos++
		}
	}
	return out
}

// getUniqueIDsInRange returns the distinct ids in [start, end], newest
// occurrence first.
func (c *temporalColumnarIndex) getUniqueIDsInRange(start, end int64) []uint64 {
	lo, hi := c.groupRange(start, end)
	if hi <= lo {
		return nil
	}
	metrics.TemporalQueryScannedNodesTotal.Add(float64(hi - lo))
	s, e := c.entryRange(lo, hi)

	uniqueIDs := temporalIDMapPool.Get().(map[uint64]struct{})
	defer func() {
		clear(uniqueIDs)
		temporalIDMapPool.Put(uniqueIDs)
	}()

	capacity := e - s
	if capacity > 1024 {
		capacity = 1024
	}
	results := make([]uint64, 0, capacity)
	for i := hi - 1; i >= lo; i-- {
		gs, ge := int(c.groupOff[i]), int(c.groupOff[i+1])
		for k := ge - 1; k >= gs; k-- {
			id := c.ids[k]
			if _, exists := uniqueIDs[id]; !exists {
				results = append(results, id)
				uniqueIDs[id] = struct{}{}
			}
		}
	}
	return results
}

// getEarliest returns the first n entries in ascending order.
func (c *temporalColumnarIndex) getEarliest(n int) []uint64 {
	if n <= 0 || c.entries == 0 {
		return nil
	}
	if n > c.entries {
		n = c.entries
	}
	metrics.TemporalQueryScannedNodesTotal.Add(float64(c.groupEntryCount(0, n)))
	out := make([]uint64, n)
	copy(out, c.ids[:n])
	return out
}

// getLatest returns the last n entries, newest first.
func (c *temporalColumnarIndex) getLatest(n int) []uint64 {
	if n <= 0 || c.entries == 0 {
		return nil
	}
	if n > c.entries {
		n = c.entries
	}
	metrics.TemporalQueryScannedNodesTotal.Add(float64(c.groupEntryCount(c.entries-n, c.entries)))
	out := make([]uint64, n)
	for i := range out {
		out[i] = c.ids[c.entries-1-i]
	}
	return out
}

// getUniqueLatest returns up to n distinct ids from the newest entries, newest
// occurrence first. Like the chunked walk it keeps consuming entries until n
// distinct ids have been collected, so duplicated ids can make it scan further
// back than n entries.
func (c *temporalColumnarIndex) getUniqueLatest(n int) []uint64 {
	if n <= 0 || c.entries == 0 {
		return nil
	}

	uniqueIDs := temporalIDMapPool.Get().(map[uint64]struct{})
	defer func() {
		clear(uniqueIDs)
		temporalIDMapPool.Put(uniqueIDs)
	}()

	capacity := n
	if capacity > c.entries {
		capacity = c.entries
	}
	results := make([]uint64, 0, capacity)
	groups := 0
	for g := len(c.ts) - 1; g >= 0 && len(results) < n; g-- {
		groups++
		gs, ge := int(c.groupOff[g]), int(c.groupOff[g+1])
		for k := ge - 1; k >= gs; k-- {
			id := c.ids[k]
			if _, exists := uniqueIDs[id]; !exists {
				results = append(results, id)
				uniqueIDs[id] = struct{}{}
				if len(results) >= n {
					break
				}
			}
		}
	}
	metrics.TemporalQueryScannedNodesTotal.Add(float64(groups))
	return results
}

// groupEntryCount reports how many timestamp groups cover the half-open entry
// range [from, to), so the scanned-node metric stays comparable with the chunked
// walk.
func (c *temporalColumnarIndex) groupEntryCount(from, to int) int {
	if len(c.groupOff) == 0 {
		return 0
	}
	first := lowerBoundU32(c.groupOff, uint32(from)) // #nosec G115
	last := lowerBoundU32(c.groupOff, uint32(to))    // #nosec G115
	if last > first {
		return last - first
	}
	return 0
}

// lowerBoundU32 returns the index of the first element of the ascending slice a
// that is >= x.
func lowerBoundU32(a []uint32, x uint32) int {
	lo, hi := 0, len(a)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if a[mid] < x {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// GetRange returns all vector IDs within the specified timestamp range.
func (tt *TemporalTree) GetRange(start, end int64) []uint64 {
	if c := tt.columnarSnapshot(); c != nil {
		return c.getRange(start, end)
	}
	return tt.getRangeChunked(start, end)
}

// GetRangeReversed returns all vector IDs within the specified timestamp range
// in descending order.
func (tt *TemporalTree) GetRangeReversed(start, end int64) []uint64 {
	if c := tt.columnarSnapshot(); c != nil {
		return c.getRangeReversed(start, end)
	}
	return tt.getRangeReversedChunked(start, end)
}

// GetUniqueIDsInRange returns unique vector IDs within the specified timestamp
// range, keeping only the most recent version of each ID.
func (tt *TemporalTree) GetUniqueIDsInRange(start, end int64) []uint64 {
	if c := tt.columnarSnapshot(); c != nil {
		return c.getUniqueIDsInRange(start, end)
	}
	return tt.getUniqueIDsInRangeChunked(start, end)
}

// GetEarliest returns the vector IDs from the first n timestamps.
func (tt *TemporalTree) GetEarliest(n int) []uint64 {
	if c := tt.columnarSnapshot(); c != nil {
		return c.getEarliest(n)
	}
	return tt.getEarliestChunked(n)
}

// GetLatest returns the vector IDs from the last n timestamps.
func (tt *TemporalTree) GetLatest(n int) []uint64 {
	if c := tt.columnarSnapshot(); c != nil {
		return c.getLatest(n)
	}
	return tt.getLatestChunked(n)
}

// GetUniqueLatest returns the n most recent unique vector IDs.
func (tt *TemporalTree) GetUniqueLatest(n int) []uint64 {
	if c := tt.columnarSnapshot(); c != nil {
		return c.getUniqueLatest(n)
	}
	return tt.getUniqueLatestChunked(n)
}
