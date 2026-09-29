package store

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/memory"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

// columnarTestCase describes one insert workload used by the parity tests.
type columnarTestCase struct {
	name       string
	timestamps []int64
	ids        []uint64
	norms      []float32
}

func newColumnarTestTree(t *testing.T, tc columnarTestCase) *TemporalTree {
	t.Helper()
	tt := NewTemporalTree(memory.NewSlabArena(64 * 1024 * 1024))
	for i := range tc.timestamps {
		tt.Insert(tc.timestamps[i], tc.ids[i], tc.norms[i])
	}
	tt.rebuildColumnarIndex()
	return tt
}

func buildColumnarCases(seed int64) []columnarTestCase {
	r := rand.New(rand.NewSource(seed))
	mk := func(name string, n int, tsFn func(i int) int64, idFn func(i int) uint64) columnarTestCase {
		tc := columnarTestCase{name: name}
		tc.timestamps = make([]int64, n)
		tc.ids = make([]uint64, n)
		tc.norms = make([]float32, n)
		for i := 0; i < n; i++ {
			tc.timestamps[i] = tsFn(i)
			tc.ids[i] = idFn(i)
			tc.norms[i] = float32(i%97) * 0.5
		}
		return tc
	}

	cases := []columnarTestCase{
		mk("empty", 0, nil, nil),
		mk("single", 1, func(int) int64 { return 1000 }, func(int) uint64 { return 7 }),
		mk("all-identical-ts", 500, func(int) int64 { return 42 }, func(i int) uint64 { return uint64(i) }),
		mk("duplicates-heavy", 4000, func(i int) int64 { return int64(i/3) * 10 }, func(i int) uint64 { return uint64(i % 37) }),
		mk("dense-distinct", 10000, func(i int) int64 { return int64(i) }, func(i int) uint64 { return uint64(i) }),
		mk("large-distinct", 12345, func(i int) int64 { return int64(i)*7 + 3 }, func(i int) uint64 { return uint64(i) }),
		mk("negative-and-epoch", 3000, func(i int) int64 { return int64(i) - 1500 }, func(i int) uint64 { return uint64(i) }),
		mk("extreme-int64", 2000, func(i int) int64 {
			switch i % 4 {
			case 0:
				return math.MinInt64
			case 1:
				return math.MaxInt64
			default:
				return int64(i) - 1000
			}
		}, func(i int) uint64 { return uint64(i) }),
		mk("repeat-ids", 6000, func(i int) int64 { return int64(i % 500) }, func(i int) uint64 { return uint64(i % 11) }),
		mk("random-10k", 10000, func(int) int64 { return 0 }, func(int) uint64 { return 0 }),
	}

	random := &cases[len(cases)-1]
	for i := range random.timestamps {
		random.timestamps[i] = r.Int63n(20000) - 5000
		random.ids[i] = r.Uint64() % 900
	}
	return cases
}

func columnarProbeRanges(tc columnarTestCase) [][2]int64 {
	ranges := [][2]int64{
		{math.MinInt64, math.MaxInt64},
		{0, 0},
		{math.MaxInt64, math.MaxInt64},
		{math.MinInt64, math.MinInt64},
		{1, -1},
		{-1, 1},
		{math.MinInt64, 0},
		{0, math.MaxInt64},
	}
	if len(tc.timestamps) == 0 {
		return ranges
	}
	minTs, maxTs := tc.timestamps[0], tc.timestamps[0]
	for _, v := range tc.timestamps {
		if v < minTs {
			minTs = v
		}
		if v > maxTs {
			maxTs = v
		}
	}
	rng := rand.New(rand.NewSource(int64(len(tc.timestamps)) + 1))
	for i := 0; i < 60; i++ {
		a := minTs + rng.Int63n(maxTs-minTs+3) - 1
		b := minTs + rng.Int63n(maxTs-minTs+3) - 1
		ranges = append(ranges, [2]int64{a, b})
	}
	return ranges
}

func temporalAssertSameIDs(t *testing.T, label string, got, want []uint64) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: columnar/chunked mismatch (%d vs %d)\n got=%v\nwant=%v",
			label, len(got), len(want), temporalPreview(got), temporalPreview(want))
	}
}

func temporalPreview(ids []uint64) []uint64 {
	if len(ids) <= 16 {
		return ids
	}
	out := append([]uint64(nil), ids[:8]...)
	return append(out, append([]uint64{0}, ids[len(ids)-8:]...)...)
}

// TestTemporalColumnarParity is the core guarantee: every query answered from
// the columnar snapshot must return exactly what the chunked walk returns,
// including ordering, duplicates and nil-versus-empty results.
func TestTemporalColumnarParity(t *testing.T) {
	for _, tc := range buildColumnarCases(20240607) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tt := newColumnarTestTree(t, tc)
			if tt.columnarIndex() == nil {
				t.Fatal("expected a published columnar snapshot")
			}
			for _, rg := range columnarProbeRanges(tc) {
				start, end := rg[0], rg[1]
				label := fmt.Sprintf("range[%d,%d]", start, end)
				temporalAssertSameIDs(t, "GetRange "+label, tt.GetRange(start, end), tt.getRangeChunked(start, end))
				temporalAssertSameIDs(t, "GetRangeReversed "+label, tt.GetRangeReversed(start, end), tt.getRangeReversedChunked(start, end))
				temporalAssertSameIDs(t, "GetUniqueIDsInRange "+label, tt.GetUniqueIDsInRange(start, end), tt.getUniqueIDsInRangeChunked(start, end))
			}
			for _, n := range []int{-1, 0, 1, 2, 3, 17, 1023, 1024, 1025, 5000, len(tc.ids) + 10} {
				label := fmt.Sprintf("n=%d", n)
				temporalAssertSameIDs(t, "GetLatest "+label, tt.GetLatest(n), tt.getLatestChunked(n))
				temporalAssertSameIDs(t, "GetEarliest "+label, tt.GetEarliest(n), tt.getEarliestChunked(n))
				temporalAssertSameIDs(t, "GetUniqueLatest "+label, tt.GetUniqueLatest(n), tt.getUniqueLatestChunked(n))
			}
		})
	}
}

// TestTemporalColumnarParityOutOfOrder forces the chunked structure through its
// leaf-splitting path before the snapshot is taken.
func TestTemporalColumnarParityOutOfOrder(t *testing.T) {
	r := rand.New(rand.NewSource(99))
	n := 5000
	timestamps := make([]int64, n)
	ids := make([]uint64, n)
	norms := make([]float32, n)
	for i := 0; i < n; i++ {
		timestamps[i] = int64(r.Intn(500))
		ids[i] = uint64(r.Intn(800))
		norms[i] = float32(i)
	}

	tt := NewTemporalTree(memory.NewSlabArena(32 * 1024 * 1024))
	for i := 0; i < n; i++ {
		tt.Insert(timestamps[i], ids[i], norms[i])
	}
	tt.rebuildColumnarIndex()

	distinct := make([]int64, 0, n)
	seen := map[int64]bool{}
	for _, v := range timestamps {
		if !seen[v] {
			seen[v] = true
			distinct = append(distinct, v)
		}
	}
	sort.Slice(distinct, func(i, j int) bool { return distinct[i] < distinct[j] })

	for i := 0; i < 12 && i < len(distinct); i++ {
		start := distinct[i]
		for j := 0; j < 12 && j < len(distinct); j++ {
			end := distinct[len(distinct)-1-j]
			assertRange := func(name string, got, want []uint64) {
				t.Helper()
				temporalAssertSameIDs(t, fmt.Sprintf("%s[%d,%d]", name, start, end), got, want)
			}
			assertRange("GetRange", tt.GetRange(start, end), tt.getRangeChunked(start, end))
			assertRange("GetRangeReversed", tt.GetRangeReversed(start, end), tt.getRangeReversedChunked(start, end))
			assertRange("GetUniqueIDsInRange", tt.GetUniqueIDsInRange(start, end), tt.getUniqueIDsInRangeChunked(start, end))
		}
	}
}

// TestTemporalColumnarIncrementalParity covers the publish/rebuild lifecycle:
// every insert invalidates the snapshot, and queries must stay correct across
// the window where the tree is served from the chunked walk.
func TestTemporalColumnarIncrementalParity(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	tt := NewTemporalTree(memory.NewSlabArena(32 * 1024 * 1024))
	tt.rebuildColumnarIndex()

	ts := int64(0)
	ids := uint64(0)
	for round := 0; round < 400; round++ {
		for k := 0; k < r.Intn(40)+1; k++ {
			ts += r.Int63n(5)
			tt.Insert(ts, ids, float32(ids%31))
			ids++
		}
		for probe := 0; probe < 6; probe++ {
			lo := ts - r.Int63n(ts+10)
			hi := lo + r.Int63n(ts-lo+10)
			temporalAssertSameIDs(t, fmt.Sprintf("round%d GetRange[%d,%d]", round, lo, hi),
				tt.GetRange(lo, hi), tt.getRangeChunked(lo, hi))
			temporalAssertSameIDs(t, fmt.Sprintf("round%d GetUniqueIDsInRange[%d,%d]", round, lo, hi),
				tt.GetUniqueIDsInRange(lo, hi), tt.getUniqueIDsInRangeChunked(lo, hi))
		}
	}
}

// TestTemporalColumnarSnapshotFreshness asserts the snapshot is only served when
// it reflects every insert.
func TestTemporalColumnarSnapshotFreshness(t *testing.T) {
	tt := NewTemporalTree(memory.NewSlabArena(4 * 1024 * 1024))
	tt.rebuildColumnarIndex()
	if tt.columnarSnapshot() == nil {
		t.Fatal("empty tree should still serve an empty snapshot")
	}

	tt.Insert(10, 1, 1)
	if tt.columnarSnapshot() != nil {
		t.Fatal("stale snapshot must not be served after an insert")
	}
	if got := tt.GetRange(0, 100); len(got) != 1 {
		t.Fatalf("GetRange during rebuild window = %v", got)
	}

	tt.rebuildColumnarIndex()
	if got := tt.GetRange(0, 100); len(got) != 1 {
		t.Fatalf("GetRange after rebuild = %v", got)
	}
	if snap := tt.columnarSnapshot(); snap == nil {
		t.Fatal("expected fresh snapshot after rebuild")
	}
}

// TestTemporalColumnarLowerBound covers the sorted-search kernel directly: every
// position, both boundaries, the scalar tail and the empty slice.
func TestTemporalColumnarLowerBound(t *testing.T) {
	sizes := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33, 63, 64, 65, 127, 1000, 4097}
	kernels := []struct {
		name string
		fn   func([]int64, int64) int
	}{
		{"scalar", lowerBoundScalar},
		{"unrolled", lowerBoundUnrolled},
		{"sort.Search", sortSearchLowerBound},
		{"auto", func(ts []int64, x int64) int { return lowerBoundSelect(temporalBoundsAuto, ts, x) }},
		{"simd", func(ts []int64, x int64) int { return lowerBoundSelect(temporalBoundsSIMD, ts, x) }},
	}

	for _, n := range sizes {
		ts := make([]int64, n)
		for i := range ts {
			ts[i] = int64(i) * 5
		}
		probes := []int64{math.MinInt64, math.MaxInt64, -1, 0, 1}
		for i := 0; i <= n; i++ {
			probes = append(probes, int64(i)*5, int64(i)*5-1, int64(i)*5+1)
		}
		for _, k := range kernels {
			for _, x := range probes {
				want := sort.Search(n, func(i int) bool { return ts[i] >= x })
				if got := k.fn(ts, x); got != want {
					t.Fatalf("%s n=%d x=%d: got %d want %d", k.name, n, x, got, want)
				}
			}
		}
	}
}

// TestTemporalColumnarLowerBoundDuplicates pins the lower-bound contract on a
// column with repeated timestamps, which is the shape the chunked tree produces
// when many vectors share a timestamp.
func TestTemporalColumnarLowerBoundDuplicates(t *testing.T) {
	ts := []int64{-5, -5, -5, 0, 0, 3, 3, 3, 3, 7, 7, 9, 100, 100, 100}
	for _, x := range []int64{math.MinInt64, -6, -5, -4, 0, 1, 3, 4, 7, 8, 9, 10, 100, 101, math.MaxInt64} {
		want := sort.Search(len(ts), func(i int) bool { return ts[i] >= x })
		if got := lowerBoundScalar(ts, x); got != want {
			t.Fatalf("scalar x=%d: got %d want %d", x, got, want)
		}
		if got := lowerBoundUnrolled(ts, x); got != want {
			t.Fatalf("unrolled x=%d: got %d want %d", x, got, want)
		}
		if got := lowerBoundSelect(temporalBoundsSIMD, ts, x); got != want {
			t.Fatalf("simd x=%d: got %d want %d", x, got, want)
		}
		if got := lowerBoundSelect(temporalBoundsAuto, ts, x); got != want {
			t.Fatalf("auto x=%d: got %d want %d", x, got, want)
		}
	}
}

// TestTemporalColumnarSIMDVsScalar forces every kernel against every other on
// the same data, including arrays too short for any SIMD size threshold.
func TestTemporalColumnarSIMDVsScalar(t *testing.T) {
	if !temporalHasAVX2 {
		t.Skip("AVX2 unavailable; the SIMD variant already resolves to the scalar kernel")
	}
	r := rand.New(rand.NewSource(11))
	for iter := 0; iter < 600; iter++ {
		n := r.Intn(400)
		ts := make([]int64, n)
		for i := range ts {
			ts[i] = int64(r.Intn(64) - 32)
		}
		sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
		for k := 0; k < 25; k++ {
			var x int64
			switch k {
			case 0:
				x = math.MinInt64
			case 1:
				x = math.MaxInt64
			default:
				x = int64(r.Intn(90) - 40)
			}
			simdIdx := lowerBoundSelect(temporalBoundsSIMD, ts, x)
			scalarIdx := lowerBoundSelect(temporalBoundsScalar, ts, x)
			unrolledIdx := lowerBoundSelect(temporalBoundsUnrolled, ts, x)
			autoIdx := lowerBoundSelect(temporalBoundsAuto, ts, x)
			refIdx := sort.Search(n, func(i int) bool { return ts[i] >= x })
			if simdIdx != scalarIdx || simdIdx != unrolledIdx || simdIdx != autoIdx || simdIdx != refIdx {
				t.Fatalf("n=%d x=%d: simd=%d scalar=%d unrolled=%d auto=%d ref=%d",
					n, x, simdIdx, scalarIdx, unrolledIdx, autoIdx, refIdx)
			}
		}
	}
}

// TestTemporalColumnarColumnLayout asserts the struct-of-arrays invariants the
// range arithmetic relies on.
func TestTemporalColumnarColumnLayout(t *testing.T) {
	tt := NewTemporalTree(memory.NewSlabArena(8 * 1024 * 1024))
	for i := 0; i < 3000; i++ {
		tt.Insert(int64(i/2)*3, uint64(i), float32(i))
	}
	tt.rebuildColumnarIndex()

	c := tt.columnarIndex()
	if c == nil {
		t.Fatal("expected a snapshot")
	}
	if len(c.groupOff) != len(c.ts)+1 {
		t.Fatalf("groupOff len %d, ts len %d", len(c.groupOff), len(c.ts))
	}
	if c.groupOff[0] != 0 {
		t.Fatalf("groupOff[0] = %d", c.groupOff[0])
	}
	if int(c.groupOff[len(c.groupOff)-1]) != c.entries {
		t.Fatalf("groupOff tail %d, entries %d", c.groupOff[len(c.groupOff)-1], c.entries)
	}
	if len(c.ids) != c.entries || len(c.norms) != c.entries {
		t.Fatalf("column lengths ids=%d norms=%d entries=%d", len(c.ids), len(c.norms), c.entries)
	}
	for i := 1; i < len(c.ts); i++ {
		if c.ts[i] <= c.ts[i-1] {
			t.Fatalf("ts not strictly ascending at %d: %d then %d", i, c.ts[i-1], c.ts[i])
		}
	}
	for i := 1; i < len(c.groupOff); i++ {
		if c.groupOff[i] < c.groupOff[i-1] {
			t.Fatalf("groupOff not monotonic at %d", i)
		}
	}
	// A group range must address exactly the entries of its groups.
	lo, hi := c.groupRange(30, 60)
	s, e := c.entryRange(lo, hi)
	if e-s != int(c.groupOff[hi]-c.groupOff[lo]) {
		t.Fatalf("entryRange %d..%d disagrees with groupOff", s, e)
	}
}

// TestTemporalColumnarConcurrentReadersAndRebuild runs many readers while a
// writer keeps inserting and a second goroutine republishes snapshots, so the
// atomic pointer swap and the dirty flag are exercised under -race.
func TestTemporalColumnarConcurrentReadersAndRebuild(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency test in short mode")
	}
	tt := NewTemporalTree(memory.NewSlabArena(64 * 1024 * 1024))
	base := int64(0)
	for i := 0; i < 20000; i++ {
		tt.Insert(base+int64(i), uint64(i), 1)
	}
	tt.rebuildColumnarIndex()

	var (
		wg      sync.WaitGroup
		stop    = make(chan struct{})
		reads   = 8
		writers = 2
	)
	ts := atomicInt64Of(20000)

	for i := 0; i < reads; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id) + 1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				end := ts.Load()
				start := r.Int63n(end + 1)
				_ = tt.GetRange(start, end)
				_ = tt.GetRangeReversed(start, end)
				_ = tt.GetUniqueIDsInRange(start, end)
				_ = tt.GetLatest(64)
				_ = tt.GetUniqueLatest(64)
				_ = tt.GetEarliest(64)
			}
		}(int64(i))
	}

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for k := 0; k < 64; k++ {
					next := ts.Add(1)
					tt.Insert(next, uint64(next), 1)
				}
				tt.rebuildColumnarIndex()
			}
		}(int64(i))
	}

	time.Sleep(400 * time.Millisecond)
	close(stop)
	wg.Wait()

	// After the dust settles the snapshot must be usable and correct.
	tt.rebuildColumnarIndex()
	final := ts.Load()
	got := tt.GetRange(0, final)
	want := tt.getRangeChunked(0, final)
	temporalAssertSameIDs(t, "post-race GetRange", got, want)
}

type atomicI64 struct {
	mu sync.Mutex
	v  int64
}

func (a *atomicI64) Load() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.v
}

func (a *atomicI64) Add(d int64) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.v += d
	return a.v
}

func atomicInt64Of(v int64) *atomicI64 { return &atomicI64{v: v} }

func sortSearchLowerBound(ts []int64, x int64) int {
	return sort.Search(len(ts), func(i int) bool { return ts[i] >= x })
}

// newColumnarBenchmarkIndex builds a TemporalIndex whose timestamps are evenly
// spaced by step starting at baseTs, so a range of w steps selects exactly w
// ids. The wall-clock sliding benchmark needs the corpus to sit just before now.
func newColumnarBenchmarkIndex(b *testing.B, n int, columnar bool, baseTs, step int64) *TemporalIndex {
	b.Helper()
	ti := NewTemporalIndex(1)
	ti.SetAsyncIngestion(false)

	ids := make([]uint64, n)
	vectors := make([][]float32, n)
	timestamps := make([]int64, n)
	for i := 0; i < n; i++ {
		ids[i] = uint64(i)
		vectors[i] = []float32{float32(i%7) + 1}
		timestamps[i] = baseTs + int64(i)*step
	}
	if err := ti.AddBatch(ids, vectors, timestamps, nil); err != nil {
		b.Fatalf("AddBatch: %v", err)
	}
	ti.temporalTree.Load().setColumnarDisabled(!columnar)
	ti.temporalTree.Load().rebuildColumnarIndex()
	if columnar && ti.temporalTree.Load().columnarIndex() == nil {
		b.Fatal("expected a columnar snapshot")
	}
	return ti
}

func columnarBenchSizes() []int { return []int{1000, 10000, 100000} }

func BenchmarkTemporalIndex_SearchAsOf(b *testing.B) {
	ctx := context.Background()
	for _, n := range columnarBenchSizes() {
		for _, mode := range []string{"columnar", "chunked"} {
			ti := newColumnarBenchmarkIndex(b, n, mode == "columnar", 0, 10)
			b.Run(fmt.Sprintf("n=%d/%s", n, mode), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; b.Loop(); i++ {
					if _, err := ti.SearchAsOf(ctx, int64(n/2+i), 10); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkTemporalIndex_SearchRange(b *testing.B) {
	ctx := context.Background()
	for _, n := range columnarBenchSizes() {
		for _, mode := range []string{"columnar", "chunked"} {
			ti := newColumnarBenchmarkIndex(b, n, mode == "columnar", 0, 10)
			window := n / 100
			b.Run(fmt.Sprintf("n=%d/%s", n, mode), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; b.Loop(); i++ {
					start := int64((i * 37) % (n - window))
					if _, err := ti.SearchRange(ctx, start, start+int64(window), 10); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkTemporalIndex_SearchSlidingWindow(b *testing.B) {
	ctx := context.Background()
	for _, n := range columnarBenchSizes() {
		for _, mode := range []string{"columnar", "chunked"} {
			ti := newColumnarBenchmarkIndex(b, n, mode == "columnar", 0, 10)
			b.Run(fmt.Sprintf("n=%d/%s", n, mode), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; b.Loop(); i++ {
					if _, err := ti.SearchSlidingWindow(ctx, 1000, 10); err != nil {
						b.Fatal(err)
					}
					_ = i
				}
			})
		}
	}
}

func BenchmarkTemporalIndex_SearchSlidingWindowByTime(b *testing.B) {
	ctx := context.Background()
	for _, n := range columnarBenchSizes() {
		for _, mode := range []string{"columnar", "chunked"} {
			now := time.Now().UnixNano()
			// A corpus of n ids spaced 1us apart ending at build time. The
			// window is deliberately wider than the corpus so the query
			// result does not drift with wall-clock time; this measures the
			// full-corpus time-window scan.
			ti := newColumnarBenchmarkIndex(b, n, mode == "columnar", now-int64(n)*1000, 1000)
			b.Run(fmt.Sprintf("n=%d/%s", n, mode), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; b.Loop(); i++ {
					if _, err := ti.SearchSlidingWindowByTime(ctx, time.Duration(n)*time.Microsecond+time.Second, 10); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// benchmarkColumnarIndex builds an immutable columnar snapshot of n distinct
// ascending timestamps.
func benchmarkColumnarIndex(n int) *temporalColumnarIndex {
	ts := make([]int64, n)
	ids := make([]uint64, n)
	norms := make([]float32, n)
	groupOff := make([]uint32, n+1)
	for i := 0; i < n; i++ {
		ts[i] = int64(i) * 10
		ids[i] = uint64(i)
		norms[i] = 1
		groupOff[i+1] = uint32(i + 1) // #nosec G115
	}
	return &temporalColumnarIndex{ts: ts, groupOff: groupOff, ids: ids, norms: norms, entries: n}
}

// BenchmarkTemporalColumnarLowerBound compares the sorted-search kernels at the
// sizes the columnar index actually uses, against the sort.Search baseline the
// chunked tree used and against a linear scan to expose the crossover.
func BenchmarkTemporalColumnarLowerBound(b *testing.B) {
	kernels := []struct {
		name string
		fn   func([]int64, int64) int
	}{
		{"avx2", func(ts []int64, x int64) int { return lowerBoundSelect(temporalBoundsSIMD, ts, x) }},
		{"scalar", func(ts []int64, x int64) int { return lowerBoundSelect(temporalBoundsScalar, ts, x) }},
		{"unrolled4", func(ts []int64, x int64) int { return lowerBoundSelect(temporalBoundsUnrolled, ts, x) }},
		{"sort.Search", sortSearchLowerBound},
		{"linear", linearLowerBound},
	}
	for _, n := range []int{4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576} {
		c := benchmarkColumnarIndex(n)
		probes := make([]int64, 4096)
		r := rand.New(rand.NewSource(int64(n)))
		for i := range probes {
			probes[i] = r.Int63n(int64(n) * 10)
		}
		for _, k := range kernels {
			b.Run(fmt.Sprintf("n=%d/%s", n, k.name), func(b *testing.B) {
				var sink int
				b.ReportAllocs()
				for i := 0; b.Loop(); i++ {
					sink += k.fn(c.ts, probes[i&4095])
				}
				if sink < 0 {
					b.Fatal("unreachable")
				}
			})
		}
	}
}

func linearLowerBound(ts []int64, x int64) int {
	for i := range ts {
		if ts[i] >= x {
			return i
		}
	}
	return len(ts)
}

// BenchmarkTemporalTree_ColumnarVsChunked is the structure-level before/after
// for the tree queries the temporal search modes depend on.
func BenchmarkTemporalTree_ColumnarVsChunked(b *testing.B) {
	for _, n := range columnarBenchSizes() {
		timestamps := make([]int64, n)
		ids := make([]uint64, n)
		norms := make([]float32, n)
		for i := 0; i < n; i++ {
			timestamps[i] = int64(i) * 10
			ids[i] = uint64(i % 5000)
			norms[i] = 1
		}
		tt := NewTemporalTree(memory.NewSlabArena(256 * 1024 * 1024))
		tt.InsertBatch(timestamps, ids, norms)
		tt.rebuildColumnarIndex()
		c := tt.columnarIndex()
		if c == nil {
			b.Fatal("expected a snapshot")
		}

		b.Run(fmt.Sprintf("n=%d/GetRange/columnar", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				_ = c.getRange(int64((i*13)%n)*10, int64((i*13)%n)*10+5000)
			}
		})
		b.Run(fmt.Sprintf("n=%d/GetRange/chunked", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				_ = tt.getRangeChunked(int64((i*13)%n)*10, int64((i*13)%n)*10+5000)
			}
		})
		b.Run(fmt.Sprintf("n=%d/GetUniqueIDsInRange/columnar", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				_ = c.getUniqueIDsInRange(0, int64(n)*10-int64(i%1000))
			}
		})
		b.Run(fmt.Sprintf("n=%d/GetUniqueIDsInRange/chunked", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				_ = tt.getUniqueIDsInRangeChunked(0, int64(n)*10-int64(i%1000))
			}
		})
		b.Run(fmt.Sprintf("n=%d/GetUniqueLatest/columnar", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				_ = c.getUniqueLatest(1000)
			}
		})
		b.Run(fmt.Sprintf("n=%d/GetUniqueLatest/chunked", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				_ = tt.getUniqueLatestChunked(1000)
			}
		})
	}
}

// BenchmarkTemporalColumnarRebuild measures the amortized cost of republishing
// the snapshot, which is what an insert batch pays.
func BenchmarkTemporalColumnarRebuild(b *testing.B) {
	for _, n := range columnarBenchSizes() {
		timestamps := make([]int64, n)
		ids := make([]uint64, n)
		norms := make([]float32, n)
		for i := 0; i < n; i++ {
			timestamps[i] = int64(i)
			ids[i] = uint64(i)
			norms[i] = 1
		}
		tt := NewTemporalTree(memory.NewSlabArena(256 * 1024 * 1024))
		tt.InsertBatch(timestamps, ids, norms)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tt.rebuildColumnarIndex()
			}
		})
	}
}

// BenchmarkTemporalTree_InsertBatch measures the write path, which now ends with
// a columnar snapshot rebuild, so the two can be compared directly.
func BenchmarkTemporalTree_InsertBatch(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		timestamps := make([]int64, n)
		ids := make([]uint64, n)
		norms := make([]float32, n)
		for i := 0; i < n; i++ {
			timestamps[i] = int64(i)
			ids[i] = uint64(i)
			norms[i] = 1
		}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tt := NewTemporalTree(memory.NewSlabArena(256 * 1024 * 1024))
				tt.InsertBatch(timestamps, ids, norms)
			}
		})
	}
}

// TestTemporalColumnarSearchModeParity is the end-to-end parity check for the
// public temporal search modes: two identically loaded indexes, one with the
// columnar snapshot enabled and one with it disabled, must return identical
// result sets for every query shape.
func TestTemporalColumnarSearchModeParity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end parity in short mode")
	}
	dim := 8
	base := time.Now().Add(-2 * time.Hour).UnixNano()

	newIndex := func(t *testing.T, n int, tsFn func(i int) int64, columnar bool) *TemporalIndex {
		t.Helper()
		ti := NewTemporalIndex(dim)
		ti.SetAsyncIngestion(false)
		ids := make([]uint64, n)
		vectors := make([][]float32, n)
		timestamps := make([]int64, n)
		for i := 0; i < n; i++ {
			ids[i] = uint64(i)
			v := make([]float32, dim)
			v[i%dim] = float32(i%13) + 1
			vectors[i] = v
			timestamps[i] = tsFn(i)
		}
		if err := ti.AddBatch(ids, vectors, timestamps, nil); err != nil {
			t.Fatalf("AddBatch: %v", err)
		}
		tt := ti.temporalTree.Load()
		tt.setColumnarDisabled(!columnar)
		tt.rebuildColumnarIndex()
		if columnar && tt.columnarIndex() == nil {
			t.Fatal("expected a columnar snapshot")
		}
		return ti
	}

	r := rand.New(rand.NewSource(3))
	workloads := []struct {
		name string
		n    int
		ts   func(i int) int64
	}{
		{"distinct-2k", 2000, func(i int) int64 { return base + int64(i)*1_000_000 }},
		{"repeat-timestamps-2k", 2000, func(i int) int64 { return base + int64(i%50)*1_000_000 }},
		{"repeat-ids-1k", 1000, func(i int) int64 { return base + int64(i%7)*1_000_000 }},
	}

	ctx := context.Background()
	for _, w := range workloads {
		w := w
		t.Run(w.name, func(t *testing.T) {
			col := newIndex(t, w.n, w.ts, true)
			chk := newIndex(t, w.n, w.ts, false)

			check := func(label string, got, want []lbtypes.SearchResult) {
				t.Helper()
				if len(got) != len(want) {
					t.Fatalf("%s: len %d != %d", label, len(got), len(want))
				}
				for i := range got {
					if got[i].ID != want[i].ID || got[i].Distance != want[i].Distance || got[i].Score != want[i].Score {
						t.Fatalf("%s: result %d = %+v want %+v", label, i, got[i], want[i])
					}
				}
			}

			for probe := 0; probe < 24; probe++ {
				ts := base + int64(r.Intn(w.n))*1_000_000
				got, err := col.SearchAsOf(ctx, ts, 10)
				if err != nil {
					t.Fatalf("SearchAsOf: %v", err)
				}
				want, err := chk.SearchAsOf(ctx, ts, 10)
				if err != nil {
					t.Fatalf("SearchAsOf: %v", err)
				}
				check("SearchAsOf", got, want)
			}

			for probe := 0; probe < 24; probe++ {
				s := base + int64(r.Intn(w.n))*1_000_000
				e := s + int64(r.Intn(400))*1_000_000
				got, err := col.SearchRange(ctx, s, e, 10)
				if err != nil {
					t.Fatalf("SearchRange: %v", err)
				}
				want, err := chk.SearchRange(ctx, s, e, 10)
				if err != nil {
					t.Fatalf("SearchRange: %v", err)
				}
				check("SearchRange", got, want)
			}

			for _, window := range []int{1, 7, 50, 500, 5000} {
				got, err := col.SearchSlidingWindow(ctx, window, 10)
				if err != nil {
					t.Fatalf("SearchSlidingWindow: %v", err)
				}
				want, err := chk.SearchSlidingWindow(ctx, window, 10)
				if err != nil {
					t.Fatalf("SearchSlidingWindow: %v", err)
				}
				check(fmt.Sprintf("SearchSlidingWindow(%d)", window), got, want)
			}

			// The whole corpus sits inside a 24h window, so the wall-clock
			// sliding search is stable across both indexes.
			for _, d := range []time.Duration{time.Second, time.Minute, time.Hour, 24 * time.Hour} {
				got, err := col.SearchSlidingWindowByTime(ctx, d, 10)
				if err != nil {
					t.Fatalf("SearchSlidingWindowByTime: %v", err)
				}
				want, err := chk.SearchSlidingWindowByTime(ctx, d, 10)
				if err != nil {
					t.Fatalf("SearchSlidingWindowByTime: %v", err)
				}
				check(fmt.Sprintf("SearchSlidingWindowByTime(%v)", d), got, want)
			}
		})
	}
}
