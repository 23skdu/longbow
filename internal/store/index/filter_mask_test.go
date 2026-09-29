package index

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// filterShape describes a filter to build for the parity tests.
type filterShape struct {
	name string
	// universe is the exclusive upper bound of the ids the filter may contain.
	universe int
	// build populates the filter with ids drawn from [0, universe).
	build func(r *rand.Rand, bm *roaring.Bitmap)
}

func parityFilterShapes() []filterShape {
	const n = 20_000
	return []filterShape{
		{
			name:     "empty",
			universe: n,
			build:    func(*rand.Rand, *roaring.Bitmap) {},
		},
		{
			name:     "all_match",
			universe: n,
			build: func(_ *rand.Rand, bm *roaring.Bitmap) {
				bm.AddRange(0, n)
			},
		},
		{
			name:     "single_first",
			universe: n,
			build: func(_ *rand.Rand, bm *roaring.Bitmap) {
				bm.Add(0)
			},
		},
		{
			name:     "single_last",
			universe: n,
			build: func(_ *rand.Rand, bm *roaring.Bitmap) {
				bm.Add(n - 1)
			},
		},
		{
			name:     "sparse_1pct",
			universe: n,
			build: func(r *rand.Rand, bm *roaring.Bitmap) {
				for i := 0; i < n/100; i++ {
					bm.Add(uint32(r.Intn(n))) // #nosec G115
				}
			},
		},
		{
			name:     "selective_10pct",
			universe: n,
			build: func(r *rand.Rand, bm *roaring.Bitmap) {
				for i := 0; i < n/10; i++ {
					bm.Add(uint32(r.Intn(n))) // #nosec G115
				}
			},
		},
		{
			name:     "selective_50pct",
			universe: n,
			build: func(r *rand.Rand, bm *roaring.Bitmap) {
				for i := 0; i < n/2; i++ {
					bm.Add(uint32(r.Intn(n))) // #nosec G115
				}
			},
		},
		{
			name:     "selective_90pct",
			universe: n,
			build: func(r *rand.Rand, bm *roaring.Bitmap) {
				for i := 0; i < 9*n/10; i++ {
					bm.Add(uint32(r.Intn(n))) // #nosec G115
				}
			},
		},
		{
			name:     "word_boundaries",
			universe: 300,
			build: func(_ *rand.Rand, bm *roaring.Bitmap) {
				for id := 0; id < 300; id += 64 {
					bm.Add(uint32(id)) // #nosec G115
				}
				bm.Add(299)
			},
		},
		{
			name:     "tail_word_only",
			universe: 64,
			build: func(_ *rand.Rand, bm *roaring.Bitmap) {
				bm.Add(63)
			},
		},
	}
}

func buildParityFilter(t *testing.T, s filterShape, seed int64) *roaring.Bitmap {
	t.Helper()
	bm := roaring.New()
	s.build(rand.New(rand.NewSource(seed)), bm) // #nosec G404
	return bm
}

// TestFilterMaskParityWithRoaring is the core guarantee: densifying a filter
// admits exactly the ids the roaring bitmap admits, for every id in the filter's
// universe and beyond it.
func TestFilterMaskParityWithRoaring(t *testing.T) {
	for _, s := range parityFilterShapes() {
		t.Run(s.name, func(t *testing.T) {
			bm := buildParityFilter(t, s, 1)
			mask, _ := buildFilterMask(bm, nil)
			require.NotNil(t, mask)

			// Probe every id of the universe plus the out-of-range ids a
			// traversal can hand us.
			ids := make([]uint32, 0, s.universe+4)
			for i := 0; i <= s.universe+1; i++ {
				ids = append(ids, uint32(i)) // #nosec G115
			}
			ids = append(ids, math.MaxUint32, math.MaxUint32-1, 1<<24)

			for _, id := range ids {
				assert.Equal(t, bm.Contains(id), mask.allows(id),
					"id %d: dense mask disagrees with roaring", id)
			}
		})
	}
}

// TestFilterMaskParityForcedRoaring covers the fallback representation: when the
// dense conversion is skipped, the mask must probe the roaring filter itself and
// therefore behave identically.
func TestFilterMaskParityForcedRoaring(t *testing.T) {
	forceFilterMaskRoaring.Store(true)
	t.Cleanup(func() { forceFilterMaskRoaring.Store(false) })

	for _, s := range parityFilterShapes() {
		t.Run(s.name, func(t *testing.T) {
			bm := buildParityFilter(t, s, 2)
			mask, _ := buildFilterMask(bm, nil)
			require.NotNil(t, mask)
			if !bm.IsEmpty() {
				require.False(t, mask.densified(), "forced fallback must not densify")
				require.Same(t, bm, mask.roaring, "fallback must probe the roaring filter itself")
			}

			for id := uint32(0); id < uint32(s.universe+2); id++ { // #nosec G115
				assert.Equal(t, bm.Contains(id), mask.allows(id), "id %d", id)
			}
		})
	}
}

// TestFilterMaskNoFilter checks that an absent filter yields no mask at all,
// which the traversal reads as "unfiltered".
func TestFilterMaskNoFilter(t *testing.T) {
	mask, scratch := buildFilterMask(nil, nil)
	assert.Nil(t, mask)
	assert.Nil(t, scratch)
}

// TestFilterMaskEmptyFilterRejectsAll guards the subtle case that a nil dense
// vector would be indistinguishable from "no filter": an empty filter must
// reject every candidate, exactly as roaring.Contains does.
func TestFilterMaskEmptyFilterRejectsAll(t *testing.T) {
	empty := roaring.New()
	mask, _ := buildFilterMask(empty, nil)
	require.NotNil(t, mask)
	require.NotNil(t, mask.dense, "dense must be non-nil so the roaring fallback is not taken")
	require.True(t, mask.densified(), "an empty filter is trivially dense")

	for _, id := range []uint32{0, 1, 63, 64, 1 << 20, math.MaxUint32} {
		assert.False(t, mask.allows(id), "id %d must be rejected by an empty filter", id)
		assert.False(t, empty.Contains(id))
	}
}

// TestFilterMaskFallbackForSparseHugeUniverse is the memory guard: a handful of
// ids scattered over a huge id space must not be turned into a giant dense
// bitmask, and the fallback must still be exactly correct.
func TestFilterMaskFallbackForSparseHugeUniverse(t *testing.T) {
	// 8 ids spread over a 4e9 id universe. A dense bitmask would be 500 MB.
	const hugeMaxID = 4_000_000_000
	bm := roaring.New()
	spare := []uint32{0, 1, 63, 64, 65, 1_000_000, 2_000_000_000, hugeMaxID}
	for _, id := range spare {
		bm.Add(id)
	}

	mask, scratch := buildFilterMask(bm, nil)
	require.NotNil(t, mask)
	require.False(t, mask.densified(), "sparse filter over a huge id space must fall back to roaring")
	require.Nil(t, mask.dense, "fallback must not allocate a dense bitvector")
	require.Nil(t, scratch, "fallback must not consume or grow the scratch buffer")
	require.Equal(t, 0, cap(scratch), "fallback must not retain a %d word buffer", hugeMaxID/64)

	// The policy itself must reject it, independent of the wiring.
	require.False(t, densifyWorthwhile(int(bm.DenseSize()), bm.Stats()))

	// Fallback semantics are unchanged.
	for _, id := range spare {
		assert.True(t, mask.allows(id), "id %d must be admitted", id)
	}
	for _, id := range []uint32{2, 62, 66, 999_999, 1_000_001, 3_999_999_999} {
		assert.False(t, mask.allows(id), "id %d must be rejected", id)
	}
}

// TestFilterMaskConversionPolicy pins the two conversion thresholds: the
// memory cap on the bitmask, and the per-array-value cap on the conversion work.
func TestFilterMaskConversionPolicy(t *testing.T) {
	const (
		maxBytes = filterMaskMaxBytes
		maxArray = filterMaskMaxArrayValues
	)

	// The memory cap is the only limit for a filter that converts by bulk copy:
	// its cardinality does not matter, only the size of the bitmask.
	cheap := roaring.Statistics{ArrayContainerValues: 1, Cardinality: 1 << 40}
	lastAllowed := maxBytes / 8
	assert.True(t, densifyWorthwhile(lastAllowed, cheap), "1 MiB bitmask is allowed")
	assert.False(t, densifyWorthwhile(lastAllowed+1, cheap), "1 MiB + 1 word is not")

	// Array-container values are charged at ~1.3 ns each, and only they.
	assert.True(t, densifyWorthwhile(16, roaring.Statistics{ArrayContainerValues: maxArray}))
	assert.False(t, densifyWorthwhile(16, roaring.Statistics{ArrayContainerValues: maxArray + 1}),
		"one array value past the budget must fall back")

	// Bitmap-container values convert by memmove and are never charged, however
	// many there are.
	assert.True(t, densifyWorthwhile(16, roaring.Statistics{
		ArrayContainerValues:  0,
		BitmapContainerValues: 1 << 30,
		Cardinality:           1 << 30,
	}))

	// Run containers are charged, since they are written word by word.
	assert.False(t, densifyWorthwhile(16, roaring.Statistics{
		RunContainers:      1,
		RunContainerValues: maxArray + 1,
	}))

	// A dense bitmask is only ever built for a filter whose own universe is the
	// bitmask size, so a filter larger than the cap is never allocated.
	oversized := roaring.New()
	oversized.Add(1 << 30)
	require.False(t, densifyWorthwhile(int(oversized.DenseSize()), oversized.Stats())) // #nosec G115
}

// TestFilterMaskConvertsRunContainers proves the dense export handles roaring's
// run container representation, which WriteDenseTo reaches through a different
// branch than the array and bitmap containers.
func TestFilterMaskConvertsRunContainers(t *testing.T) {
	bm := roaring.New()
	bm.AddRange(100, 5_000)
	bm.AddRange(70_000, 71_000)
	bm.AddRange(140_000, 140_010)
	bm.RunOptimize()

	stats := bm.Stats()
	require.Positive(t, stats.RunContainers, "expected run containers, got %+v", stats)

	mask, _ := buildFilterMask(bm, nil)
	require.True(t, mask.densified(), "run containers over a small universe must densify")
	for id := uint32(0); id < 200_000; id++ { // #nosec G115
		assert.Equal(t, bm.Contains(id), mask.allows(id), "id %d", id)
	}
}

// TestFilterMaskScratchIsZeroedOnReuse guards against stale bits: the scratch
// buffer is recycled across searches, so a leftover bit would admit a candidate
// the roaring filter rejects.
func TestFilterMaskScratchIsZeroedOnReuse(t *testing.T) {
	// A filter that densifies and fills the low words.
	first := roaring.New()
	first.AddRange(0, 64)
	// A much smaller filter, so the recycled buffer must be truncated and zeroed.
	second := roaring.New()
	second.AddRange(0, 2)

	scratch := make(types.BitVector, 0, 64)
	maskA, scratch := buildFilterMask(first, scratch)
	require.True(t, maskA.densified())
	for id := uint32(0); id < 64; id++ { // #nosec G115
		require.True(t, maskA.allows(id))
	}

	maskB, _ := buildFilterMask(second, scratch)
	require.True(t, maskB.densified())
	require.Equal(t, 1, len(maskB.dense), "bitvector must shrink to the filter's own universe")
	assert.False(t, maskB.allows(2), "stale bit leaked from the previous filter")
	assert.False(t, maskB.allows(63))
	assert.True(t, maskB.allows(0))
	assert.True(t, maskB.allows(1))
}

// TestResizeBitVectorZeroesReusedBuffer covers the scratch plumbing directly.
func TestResizeBitVectorZeroesReusedBuffer(t *testing.T) {
	dirty := make(types.BitVector, 8)
	for i := range dirty {
		dirty[i] = ^uint64(0)
	}

	scratch := make(types.BitVector, 0, 8)
	scratch = append(scratch, dirty...)

	grown := resizeBitVector(scratch, 8, true)
	assert.Equal(t, 8, len(grown))
	assert.Equal(t, 0, grown.Count(), "reused buffer must be zeroed")

	shrunk := resizeBitVector(grown, 2, true)
	assert.Equal(t, 2, len(shrunk))
	assert.Equal(t, 0, shrunk.Count())

	// keep=false must not hand back (or grow) the caller's buffer.
	assert.Equal(t, 8, len(scratch))
	assert.Equal(t, 8, cap(scratch))
	fresh := resizeBitVector(scratch, 4, false)
	assert.Equal(t, 4, len(fresh))
	assert.Equal(t, 4, cap(fresh))
	assert.Equal(t, 8, cap(scratch), "caller's buffer must be untouched")
}

// --- search-level parity -----------------------------------------------------

type idDist struct {
	ID   uint32
	Dist float32
}

func sortedCandidates(res []types.Candidate) []idDist {
	out := make([]idDist, 0, len(res))
	for _, c := range res {
		out = append(out, idDist{ID: c.ID, Dist: c.Dist})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID != out[j].ID {
			return out[i].ID < out[j].ID
		}
		return out[i].Dist < out[j].Dist
	})
	return out
}

func sortedResults(res []types.SearchResult) []idDist {
	out := make([]idDist, 0, len(res))
	for _, r := range res {
		out = append(out, idDist{ID: uint32(r.ID), Dist: r.Distance}) // #nosec G115
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID != out[j].ID {
			return out[i].ID < out[j].ID
		}
		return out[i].Dist < out[j].Dist
	})
	return out
}

// buildParityIndex creates a deterministic float32 HNSW index of n vectors.
func buildParityIndex(tb testing.TB, n, dims int, seed int64) (*ArrowHNSW, arrow.RecordBatch) {
	tb.Helper()
	mem := memory.NewGoAllocator()
	r := rand.New(rand.NewSource(seed)) // #nosec G404

	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(r.NormFloat64())
		}
		vecs[i] = v
	}
	rec := MakeBatchTestRecord(mem, dims, vecs)
	rec.Retain()

	ds := NewMockDataset("filter_mask_parity", rec.Schema())
	ds.Records = append(ds.Records, rec)

	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeFloat32
	cfg.Dims = dims
	cfg.M = 16
	cfg.EfConstruction = 100
	idx := NewArrowHNSW(ds, &cfg, nil)

	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)
	for i := range rowIdxs {
		rowIdxs[i] = i
	}
	_, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	require.NoError(tb, err)
	return idx, rec
}

// withFilterMask runs fn with the search pool's roaring fallback pinned, so the
// two probe implementations can be compared on the same filter.
func withFilterMask(force bool, fn func()) {
	prev := forceFilterMaskRoaring.Load()
	forceFilterMaskRoaring.Store(force)
	defer forceFilterMaskRoaring.Store(prev)
	fn()
}

// TestFilteredSearchParity_EndToEnd is the end-to-end A/B: the same filtered
// searches, the same index, the same seeds, once with the dense bitmask and once
// with the roaring probe. Result sets must be identical.
func TestFilteredSearchParity_EndToEnd(t *testing.T) {
	const (
		n     = 2_000
		dims  = 32
		query = 10
	)
	idx, rec := buildParityIndex(t, n, dims, 7)
	defer rec.Release()

	filters := []struct {
		name string
		ids  []uint32
	}{
		{name: "all_match", ids: allIDs(n)},
		{name: "empty", ids: nil},
		{name: "selective_10pct", ids: stratifiedIDs(n, 10, 11)},
		{name: "selective_50pct", ids: stratifiedIDs(n, 50, 12)},
		{name: "selective_90pct", ids: stratifiedIDs(n, 90, 13)},
		{name: "first_10", ids: seqIDs(0, 10)},
		{name: "last_10", ids: seqIDs(n-10, n)},
		{name: "word_boundaries", ids: []uint32{0, 63, 64, 65, 127, 128, 191, 192}},
		{name: "singles", ids: []uint32{0, 1, 999, 1000, n - 1}},
	}

	r := rand.New(rand.NewSource(42)) // #nosec G404
	for _, f := range filters {
		t.Run(f.name, func(t *testing.T) {
			bm := roaring.New()
			bm.AddMany(f.ids)

			// The filter must densify for the interesting shapes, otherwise
			// this subtest is silently comparing roaring against roaring.
			mask, _ := buildFilterMask(bm, nil)
			require.NotNil(t, mask)

			for qi := 0; qi < 5; qi++ {
				q := make([]float32, dims)
				for j := range q {
					q[j] = float32(r.NormFloat64())
				}

				var denseRes, roaringRes []types.SearchResult
				withFilterMask(false, func() {
					var err error
					denseRes, err = idx.SearchVectorsWithBitmap(context.Background(), q, query, bm.Clone(), nil)
					require.NoError(t, err)
				})
				withFilterMask(true, func() {
					var err error
					roaringRes, err = idx.SearchVectorsWithBitmap(context.Background(), q, query, bm.Clone(), nil)
					require.NoError(t, err)
				})

				require.Equal(t, sortedResults(denseRes), sortedResults(roaringRes),
					"query %d: dense and roaring probe disagree\ndense=%v\nroaring=%v",
					qi, sortedResults(denseRes), sortedResults(roaringRes))

				// Whatever the path, every hit must be admitted by the filter.
				for _, hit := range denseRes {
					assert.True(t, bm.Contains(uint32(hit.ID)), "id %d not in filter", hit.ID) // #nosec G115
				}
			}
		})
	}
}

// TestFilteredSearchParity_StructuredPredicates covers the "filtered",
// "filteredbool" and "filteredstring" style structured predicates that ride
// alongside the bitmap filter, and asserts the two probe implementations agree.
func TestFilteredSearchParity_StructuredPredicates(t *testing.T) {
	const (
		n     = 1_500
		dims  = 16
		query = 10
	)
	idx, rec := buildParityIndex(t, n, dims, 11)
	defer rec.Release()

	// Synthetic structured metadata per id: a numeric field, a bool field and a
	// string field, mirroring the column types a filtered dataset carries.
	meta := make([]struct {
		num  int64
		flag bool
		str  string
	}, n)
	cats := []string{"alpha", "beta", "gamma", "delta"}
	for i := range meta {
		meta[i] = struct {
			num  int64
			flag bool
			str  string
		}{
			num:  int64(i % 5),
			flag: i%3 == 0,
			str:  cats[i%len(cats)],
		}
	}

	styles := []struct {
		name   string
		match  func(id uint32) bool
		predic types.HNSWPredicate
	}{
		{
			name:  "filtered_num_eq",
			match: func(id uint32) bool { return meta[id].num == 2 },
		},
		{
			name:  "filteredbool_true",
			match: func(id uint32) bool { return meta[id].flag },
		},
		{
			name:  "filteredstring_eq",
			match: func(id uint32) bool { return meta[id].str == "beta" },
		},
		{
			name:  "filteredstring_prefix",
			match: func(id uint32) bool { return meta[id].str == "alpha" || meta[id].str == "gamma" },
		},
		{
			name:  "all_match",
			match: func(uint32) bool { return true },
		},
		{
			name:  "empty",
			match: func(uint32) bool { return false },
		},
	}

	for _, st := range styles {
		t.Run(st.name, func(t *testing.T) {
			bm := roaring.New()
			for id := 0; id < n; id++ {
				if st.match(uint32(id)) { // #nosec G115
					bm.Add(uint32(id)) // #nosec G115
				}
			}

			opts := types.SearchOptions{Predicate: st.predic}
			r := rand.New(rand.NewSource(5)) // #nosec G404
			for qi := 0; qi < 5; qi++ {
				q := make([]float32, dims)
				for j := range q {
					q[j] = float32(r.NormFloat64())
				}

				var denseRes, roaringRes []types.SearchResult
				withFilterMask(false, func() {
					var err error
					denseRes, err = idx.SearchVectorsWithBitmap(context.Background(), q, query, bm.Clone(), opts)
					require.NoError(t, err)
				})
				withFilterMask(true, func() {
					var err error
					roaringRes, err = idx.SearchVectorsWithBitmap(context.Background(), q, query, bm.Clone(), opts)
					require.NoError(t, err)
				})

				if bm.IsEmpty() {
					// Empty filters short-circuit before traversal; both paths
					// must agree that there is nothing to return.
					assert.Empty(t, denseRes)
					assert.Empty(t, roaringRes)
					continue
				}
				require.Equal(t, sortedResults(denseRes), sortedResults(roaringRes),
					"query %d: dense and roaring probe disagree", qi)
				for _, hit := range denseRes {
					assert.True(t, st.match(uint32(hit.ID)), "id %d must satisfy the predicate", hit.ID) // #nosec G115
				}
			}
		})
	}
}

// TestFilteredSearchParity_InRange covers the range-search path, which reuses
// the same mask for both the traversal and the final result filter.
func TestFilteredSearchParity_InRange(t *testing.T) {
	const (
		n        = 1_000
		dims     = 16
		maxRange = 20
	)
	idx, rec := buildParityIndex(t, n, dims, 13)
	defer rec.Release()

	ds, ok := idx.dataset.(*MockDataset)
	require.True(t, ok, "expected the mock dataset to be attached")
	wrapper := &bitmapFilterDataset{MockDataset: ds}
	idx.dataset = wrapper
	t.Cleanup(func() { idx.dataset = ds })

	// The wrapper ignores the filter and serves wrapper.filter, but
	// SearchVectorsInRange only asks the dataset for a bitset when a filter is
	// present, so one has to be supplied.
	dummyFilters := []query.Filter{{Field: "id_col", Operator: "eq", Value: "0"}}

	for _, s := range []struct {
		name string
		ids  []uint32
	}{
		{name: "all_match", ids: allIDs(n)},
		{name: "selective_50pct", ids: stratifiedIDs(n, 50, 21)},
		{name: "sparse", ids: []uint32{0, 500, 999}},
		{name: "empty", ids: nil},
	} {
		t.Run(s.name, func(t *testing.T) {
			wrapper.filter = roaringFrom(s.ids)

			r := rand.New(rand.NewSource(6)) // #nosec G404
			for qi := 0; qi < 3; qi++ {
				q := make([]float32, dims)
				for j := range q {
					q[j] = float32(r.NormFloat64())
				}

				var denseRes, roaringRes []types.SearchResult
				withFilterMask(false, func() {
					var err error
					denseRes, err = idx.SearchVectorsInRange(context.Background(), q, maxRange, dummyFilters, nil)
					require.NoError(t, err)
				})
				withFilterMask(true, func() {
					var err error
					roaringRes, err = idx.SearchVectorsInRange(context.Background(), q, maxRange, dummyFilters, nil)
					require.NoError(t, err)
				})
				require.Equal(t, sortedResults(denseRes), sortedResults(roaringRes), "query %d", qi)
				for _, hit := range denseRes {
					assert.True(t, wrapper.filter.Contains(uint32(hit.ID)), "id %d not in filter", hit.ID) // #nosec G115
				}
			}
		})
	}
}

// TestFilterMaskConcurrentUse runs filtered searches and mask conversions
// concurrently so -race can prove the mask is search-scoped: no shared mutable
// state, no leaked filterBits, no torn mask.
func TestFilterMaskConcurrentUse(t *testing.T) {
	const (
		n       = 1_200
		dims    = 16
		workers = 8
		rounds  = 25
	)
	idx, rec := buildParityIndex(t, n, dims, 17)
	defer rec.Release()

	// A pool of filters with wildly different sizes, so concurrent conversions
	// compete for scratch buffers of different lengths.
	filters := make([]*roaring.Bitmap, 0, 4)
	all := roaring.New()
	all.AddRange(0, n)
	filters = append(filters, all)
	filters = append(filters, roaringFrom(stratifiedIDs(n, 10, 31)))
	filters = append(filters, roaringFrom(stratifiedIDs(n, 90, 32)))
	filters = append(filters, roaringFrom([]uint32{0, 1, 2, 3, n - 1}))

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w))) // #nosec G404
			for round := 0; round < rounds; round++ {
				bm := filters[(w+round)%len(filters)]
				q := make([]float32, dims)
				for j := range q {
					q[j] = float32(r.NormFloat64())
				}
				res, err := idx.SearchVectorsWithBitmap(context.Background(), q, 10, bm, nil)
				if err != nil {
					errCh <- err
					return
				}
				for _, hit := range res {
					if !bm.Contains(uint32(hit.ID)) { // #nosec G115
						errCh <- fmt.Errorf("worker %d: id %d not in filter", w, hit.ID)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

// TestFilterMaskDoesNotOutliveSearch verifies the pooled context hands the mask
// back unreferenced, so a recycled context cannot inherit a previous search's
// filter.
func TestFilterMaskDoesNotOutliveSearch(t *testing.T) {
	const n = 400
	idx, rec := buildParityIndex(t, n, 8, 19)
	defer rec.Release()

	pool := idx.searchPool
	q := make([]float32, 8)
	q[0] = 1

	bm := roaring.New()
	bm.AddRange(0, n)
	if _, err := idx.SearchVectorsWithBitmap(context.Background(), q, 5, bm, nil); err != nil {
		t.Fatal(err)
	}

	// Drain whatever the search returned to the pool and check every context is
	// clean. The pool may hand back contexts from earlier searches too.
	for i := 0; i < 16; i++ {
		ctx := pool.Get()
		if ctx.filterMask != nil {
			t.Fatalf("pooled context retained a filter mask: %+v", ctx.filterMask)
		}
		if ctx.filterBitmap != nil {
			t.Fatal("pooled context retained a filter bitmap")
		}
		pool.Put(ctx)
	}
}

// --- helpers -----------------------------------------------------------------

// bitmapFilterDataset lets a test drive SearchVectorsInRange, which only accepts
// filters it materialises itself through the dataset.
type bitmapFilterDataset struct {
	*MockDataset
	filter *roaring.Bitmap
}

func (d *bitmapFilterDataset) GenerateFilterBitset([]query.Filter, types.FilterExpr) (*types.Bitset, error) {
	return types.NewBitsetFromRoaring(d.filter.Clone()), nil
}

func allIDs(n int) []uint32 {
	ids := make([]uint32, n)
	for i := range ids {
		ids[i] = uint32(i) // #nosec G115
	}
	return ids
}

func seqIDs(from, to int) []uint32 {
	ids := make([]uint32, 0, to-from)
	for i := from; i < to; i++ {
		ids = append(ids, uint32(i)) // #nosec G115
	}
	return ids
}

// stratifiedIDs returns every pct-th id of [0, n), so the filter is spread over
// the whole universe rather than clustered in one roaring container.
func stratifiedIDs(n, pct, seed int) []uint32 {
	r := rand.New(rand.NewSource(int64(seed))) // #nosec G404
	step := 100 / pct
	ids := make([]uint32, 0, n*pct/100)
	for i := 0; i < n; i += step {
		ids = append(ids, uint32(i+r.Intn(step))) // #nosec G115
	}
	return ids
}

func roaringFrom(ids []uint32) *roaring.Bitmap {
	bm := roaring.New()
	bm.AddMany(ids)
	return bm
}
