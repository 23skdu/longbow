package temporal

// P2 temporal range tests: an independent brute-force reference for the range,
// sliding-window, and time-window searches, plus chunk-boundary coverage and a
// scalability guard for the sorted index.

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// temporalTestOp is one recorded insert/update. The reference implementation is
// built only from this log, never from the index.
type temporalTestOp struct {
	ID  uint64
	TS  int64
	Vec []float32
}

// temporalRefIndex is an exhaustive reference view of the operation log.
type temporalRefIndex struct {
	ids      []uint64 // distinct ids, ascending
	versions map[uint64][]temporalTestOp
	byTS     []temporalTestOp // timestamp order, insertion order within a tie
}

func newTemporalRefIndex(ops []temporalTestOp) *temporalRefIndex {
	ref := &temporalRefIndex{versions: make(map[uint64][]temporalTestOp, len(ops))}
	for _, op := range ops {
		if _, ok := ref.versions[op.ID]; !ok {
			ref.ids = append(ref.ids, op.ID)
		}
		ref.versions[op.ID] = append(ref.versions[op.ID], op)
	}
	sort.Slice(ref.ids, func(i, j int) bool { return ref.ids[i] < ref.ids[j] })
	ref.byTS = append(ref.byTS, ops...)
	sort.SliceStable(ref.byTS, func(i, j int) bool { return ref.byTS[i].TS < ref.byTS[j].TS })
	return ref
}

// activeAt returns the version that SearchRange/GetVersionsAtBatch treat as
// active at t: the last inserted version with timestamp <= t. For an id whose
// timestamps are non-decreasing this is also the greatest timestamp <= t, which
// is what the sorted history column computes.
func (r *temporalRefIndex) activeAt(id uint64, t int64) (temporalTestOp, bool) {
	vers := r.versions[id]
	for i := len(vers) - 1; i >= 0; i-- {
		if vers[i].TS <= t {
			return vers[i], true
		}
	}
	return temporalTestOp{}, false
}

func (r *temporalRefIndex) latest(id uint64) (temporalTestOp, bool) {
	vers := r.versions[id]
	if len(vers) == 0 {
		return temporalTestOp{}, false
	}
	return vers[len(vers)-1], true
}

type temporalRefCandidate struct {
	id   uint64
	norm float32
}

func temporalNorm(v []float32) float32 {
	var sum float32
	for _, x := range v {
		sum += x * x
	}
	return sum
}

func temporalRefResults(cands []temporalRefCandidate, k int) []lbtypes.SearchResult {
	if k <= 0 {
		return []lbtypes.SearchResult{}
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].norm != cands[j].norm {
			return cands[i].norm < cands[j].norm
		}
		return cands[i].id < cands[j].id
	})
	if len(cands) > k {
		cands = cands[:k]
	}
	results := make([]lbtypes.SearchResult, len(cands))
	for i, c := range cands {
		results[i] = lbtypes.SearchResult{
			ID:       lbtypes.VectorID(c.id), // #nosec G115
			Distance: c.norm,
			Score:    1.0 / (1.0 + c.norm),
		}
	}
	return results
}

// refRange mirrors SearchRange: one candidate per id (the version active at the
// range end), kept only when that version's timestamp is inside the range.
func (r *temporalRefIndex) refRange(start, end int64, k int) []lbtypes.SearchResult {
	if k <= 0 || start > end {
		return []lbtypes.SearchResult{}
	}
	cands := make([]temporalRefCandidate, 0, len(r.ids))
	for _, id := range r.ids {
		op, ok := r.activeAt(id, end)
		if !ok || op.TS < start {
			continue
		}
		cands = append(cands, temporalRefCandidate{id: id, norm: temporalNorm(op.Vec)})
	}
	return temporalRefResults(cands, k)
}

// refSlidingWindow mirrors SearchSlidingWindow: walk the timestamp-ordered
// entries newest-first, collect distinct ids until n are found, and score each
// with its latest stored version.
func (r *temporalRefIndex) refSlidingWindow(n, k int) []lbtypes.SearchResult {
	if n <= 0 || k <= 0 {
		return []lbtypes.SearchResult{}
	}
	seen := make(map[uint64]struct{}, n)
	ids := make([]uint64, 0, n)
	for i := len(r.byTS) - 1; i >= 0 && len(ids) < n; i-- {
		id := r.byTS[i].ID
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	cands := make([]temporalRefCandidate, 0, len(ids))
	for _, id := range ids {
		op, ok := r.latest(id)
		if !ok {
			continue
		}
		cands = append(cands, temporalRefCandidate{id: id, norm: temporalNorm(op.Vec)})
	}
	return temporalRefResults(cands, k)
}

func requireTemporalResultsEqual(t *testing.T, label string, got, want []lbtypes.SearchResult) {
	t.Helper()
	require.Equal(t, len(want), len(got), "%s: result count", label)
	for i := range want {
		require.Equal(t, want[i].ID, got[i].ID, "%s: result %d id (got %d want %d)", label, i, got[i].ID, want[i].ID)
		require.InDelta(t, float64(want[i].Distance), float64(got[i].Distance), 1e-5,
			"%s: result %d distance (id %d)", label, i, want[i].ID)
		require.InDelta(t, float64(want[i].Score), float64(got[i].Score), 1e-6,
			"%s: result %d score (id %d)", label, i, want[i].ID)
	}
}

// buildTemporalTestIndex inserts ops in order, then pins the query path to
// either the columnar snapshot or the chunked walk.
func buildTemporalTestIndex(t *testing.T, ops []temporalTestOp, columnar bool) *TemporalIndex {
	t.Helper()
	ti := NewTemporalIndex(4)
	ti.SetAsyncIngestion(false)
	t.Cleanup(func() { _ = ti.Close() })
	for i, op := range ops {
		if err := ti.Add(op.ID, op.Vec, op.TS, nil); err != nil {
			t.Fatalf("add op %d (id=%d ts=%d): %v", i, op.ID, op.TS, err)
		}
	}
	tree := ti.temporalTree.Load()
	tree.setColumnarDisabled(!columnar)
	tree.rebuildColumnarIndex()
	if columnar && tree.columnarIndex() == nil {
		t.Fatal("expected a columnar snapshot")
	}
	return ti
}

func temporalParityWorkloads(base int64) map[string][]temporalTestOp {
	val := func(i int) []float32 {
		values := []float32{1, 2, 3, 4, 5, 2, 1, 6, 3, 4}
		return []float32{values[i%len(values)], 0, 0, 0}
	}

	workloads := make(map[string][]temporalTestOp)

	// 5000 distinct ids: a full-range query is larger than the old 4096
	// candidate cap, so an oldest-first or newest-first truncation shows up as a
	// wrong top-k.
	distinct := make([]temporalTestOp, 5000)
	for i := range distinct {
		distinct[i] = temporalTestOp{ID: uint64(i), TS: base + int64(i)*1000, Vec: val(i)}
	}
	workloads["distinct-5k"] = distinct

	// 5 updates for each of 400 ids, globally timestamp ordered.
	var repeated []temporalTestOp
	for round := 0; round < 5; round++ {
		for id := 0; id < 400; id++ {
			i := round*400 + id
			repeated = append(repeated, temporalTestOp{
				ID:  uint64(id),
				TS:  base + int64(i)*1000,
				Vec: val(round*7 + id),
			})
		}
	}
	workloads["repeated-ids"] = repeated

	// Many entries sharing each timestamp, with duplicate ids inside a group.
	var bursts []temporalTestOp
	for group := 0; group < 60; group++ {
		for j := 0; j < 20; j++ {
			bursts = append(bursts, temporalTestOp{
				ID:  uint64((group*20 + j) % 120),
				TS:  base + int64(group)*1_000_000,
				Vec: val(group + j),
			})
		}
	}
	workloads["timestamp-bursts"] = bursts

	// Insertion order shuffled across ids while each id's own timestamps stay
	// non-decreasing, which is the realistic concurrent-ingest shape.
	rng := rand.New(rand.NewSource(19))
	var shuffled []temporalTestOp
	for round := 0; round < 6; round++ {
		perm := rng.Perm(300)
		for _, id := range perm {
			shuffled = append(shuffled, temporalTestOp{
				ID:  uint64(id),
				TS:  base + int64(round*300+id)*1000,
				Vec: val(round*11 + id),
			})
		}
	}
	workloads["shuffled-insertion"] = shuffled

	workloads["empty"] = nil
	return workloads
}

func temporalRangeProbes(ops []temporalTestOp, base int64) []struct {
	name       string
	start, end int64
} {
	minTs, maxTs := base, base
	for _, op := range ops {
		if op.TS < minTs {
			minTs = op.TS
		}
		if op.TS > maxTs {
			maxTs = op.TS
		}
	}
	mid := minTs + (maxTs-minTs)/2
	return []struct {
		name       string
		start, end int64
	}{
		{"full", minTs, maxTs},
		{"before", minTs - 10_000, minTs - 1},
		{"after", maxTs + 1, maxTs + 10_000},
		{"single", mid, mid},
		{"narrow", mid, mid + 20_000},
		{"wide", minTs + 500_000, maxTs - 500_000},
		{"inverted", mid, mid - 1},
	}
}

func TestTemporalRangeParity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping temporal range parity in short mode")
	}
	ctx := context.Background()
	base := time.Now().Add(-4 * time.Hour).UnixNano()
	workloads := temporalParityWorkloads(base)

	for name, ops := range workloads {
		ops := ops
		ref := newTemporalRefIndex(ops)
		probes := temporalRangeProbes(ops, base)
		for _, columnar := range []bool{true, false} {
			mode := "chunked"
			if columnar {
				mode = "columnar"
			}
			ti := buildTemporalTestIndex(t, ops, columnar)

			t.Run(name+"/"+mode, func(t *testing.T) {
				for _, probe := range probes {
					for _, k := range []int{1, 10, 100} {
						want := ref.refRange(probe.start, probe.end, k)
						got, err := ti.SearchRange(ctx, probe.start, probe.end, k)
						require.NoError(t, err)
						requireTemporalResultsEqual(t,
							fmt.Sprintf("range %s k=%d", probe.name, k), got, want)
					}
					// k above the candidate count must return every candidate.
					wantAll := ref.refRange(probe.start, probe.end, len(ops)+10)
					gotAll, err := ti.SearchRange(ctx, probe.start, probe.end, len(ops)+10)
					require.NoError(t, err)
					requireTemporalResultsEqual(t, fmt.Sprintf("range %s k=all", probe.name), gotAll, wantAll)
				}

				for _, window := range []int{1, 7, 50, 1000, 5000, len(ops) + 10} {
					want := ref.refSlidingWindow(window, 10)
					got, err := ti.SearchSlidingWindow(ctx, window, 10)
					require.NoError(t, err)
					requireTemporalResultsEqual(t, fmt.Sprintf("window=%d", window), got, want)
				}

				// As-of is the same active-version enumeration over [0, ts].
				for _, probe := range probes {
					for _, ts := range []int64{probe.start, probe.end} {
						want := ref.refRange(0, ts, 10)
						got, err := ti.SearchAsOf(ctx, ts, 10)
						require.NoError(t, err)
						requireTemporalResultsEqual(t, fmt.Sprintf("asof ts=%d", ts), got, want)
					}
				}
			})
		}
	}
}

// TestTemporalSlidingWindowByTimeParity compares the wall-clock window against
// the same exhaustive active-version reference used for SearchRange.
func TestTemporalSlidingWindowByTimeParity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping temporal time-window parity in short mode")
	}
	ctx := context.Background()
	startNow := time.Now().UnixNano()

	// 120 ids, one 10s apart, ending roughly half an hour before the query.
	base := startNow - int64(time.Hour)
	const n = 120
	ops := make([]temporalTestOp, n)
	for i := 0; i < n; i++ {
		ops[i] = temporalTestOp{
			ID:  uint64(i),
			TS:  base + int64(i)*int64(10*time.Second),
			Vec: []float32{float32(i%9) + 1, 0, 0, 0},
		}
	}
	ref := newTemporalRefIndex(ops)

	for _, columnar := range []bool{true, false} {
		mode := "chunked"
		if columnar {
			mode = "columnar"
		}
		ti := buildTemporalTestIndex(t, ops, columnar)
		requireTemporalResultsEqual(t, mode+"/all",
			mustSlidingByTime(t, ti, ctx, time.Duration(time.Hour*2), 20),
			ref.refRange(0, startNow, 20))
		requireTemporalResultsEqual(t, mode+"/none",
			mustSlidingByTime(t, ti, ctx, time.Minute, 20),
			[]lbtypes.SearchResult{})

		// Put the window boundary five seconds before a timestamp grid point so
		// the exact wall-clock value cannot flip an entry in or out.
		probeNow := time.Now().UnixNano()
		d := time.Duration(probeNow-base) - 20*time.Minute + 5*time.Second
		got := mustSlidingByTime(t, ti, ctx, d, 20)
		wantStart := base + int64(20*time.Minute) - int64(5*time.Second)
		requireTemporalResultsEqual(t, mode+"/partial", got, ref.refRange(wantStart, probeNow, 20))
	}
}

func mustSlidingByTime(t *testing.T, ti *TemporalIndex, ctx context.Context, d time.Duration, k int) []lbtypes.SearchResult {
	t.Helper()
	got, err := ti.SearchSlidingWindowByTime(ctx, d, k)
	if err != nil {
		t.Fatalf("SearchSlidingWindowByTime(%v): %v", d, err)
	}
	return got
}

// TestTemporalRangeExactTopKBeyondOldCap pins the specific regression where the
// candidate set was truncated to the newest 4096 ids before scoring. The top-k
// by norm lives in the oldest ids here, so any truncation changes the result.
func TestTemporalRangeExactTopKBeyondOldCap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping temporal top-k regression in short mode")
	}
	base := int64(1_700_000_000_000_000_000)
	const n = 5000
	ops := make([]temporalTestOp, n)
	for i := 0; i < n; i++ {
		// id i has norm 1 for the first 100 ids and larger afterwards; the
		// oldest ids therefore own the entire top-100.
		v := float32(10)
		if i < 100 {
			v = 1
		}
		ops[i] = temporalTestOp{ID: uint64(i), TS: base + int64(i), Vec: []float32{v, 0, 0, 0}}
	}

	ti := buildTemporalTestIndex(t, ops, true)
	got, err := ti.SearchRange(context.Background(), base, base+n, 100)
	require.NoError(t, err)
	require.Len(t, got, 100)
	for i, res := range got {
		require.Equal(t, lbtypes.VectorID(i), res.ID, "result %d", i)
	}
}

// TestTemporalSlidingWindowScansOnlyTheTail proves P2-3: the sliding window is
// served by the newest tail of the sorted index, so the scanned-node count is
// proportional to the window, not the corpus.
func TestTemporalSlidingWindowScansOnlyTheTail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping temporal tail-cost test in short mode")
	}
	const n = 20_000
	ti := buildTemporalScaleIndex(t, n)
	tree := ti.temporalTree.Load()

	before := testutil.ToFloat64(metrics.TemporalQueryScannedNodesTotal)
	ids := tree.GetUniqueLatest(16)
	scanned := testutil.ToFloat64(metrics.TemporalQueryScannedNodesTotal) - before
	require.Len(t, ids, 16)
	require.LessOrEqual(t, scanned, float64(64),
		"GetUniqueLatest(16) over %d entries scanned %.0f nodes; the window must not walk the corpus", n, scanned)

	// The returned ids must be exactly the newest sixteen.
	for i, id := range ids {
		require.Equal(t, uint64(n-1-i), id, "newest id %d", i)
	}
}

// TestTemporalRangeDegenerateInputs pins the zero/empty inputs that previously
// could panic on an empty max-heap.
func TestTemporalRangeDegenerateInputs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping temporal degenerate-input test in short mode")
	}
	ctx := context.Background()
	ti := NewTemporalIndex(4)
	ti.SetAsyncIngestion(false)
	t.Cleanup(func() { _ = ti.Close() })

	for _, k := range []int{0, -1} {
		got, err := ti.SearchRange(ctx, 0, 100, k)
		require.NoError(t, err)
		require.Empty(t, got)
		got, err = ti.SearchSlidingWindow(ctx, 10, k)
		require.NoError(t, err)
		require.Empty(t, got)
		got, err = ti.SearchSlidingWindowByTime(ctx, time.Minute, k)
		require.NoError(t, err)
		require.Empty(t, got)
		got, err = ti.SearchAsOf(ctx, 100, k)
		require.NoError(t, err)
		require.Empty(t, got)
	}

	got, err := ti.SearchRange(ctx, 100, 0, 10)
	require.NoError(t, err)
	require.Empty(t, got)

	for _, window := range []int{0, -5} {
		got, err = ti.SearchSlidingWindow(ctx, window, 10)
		require.NoError(t, err)
		require.Empty(t, got)
	}

	got, err = ti.SearchSlidingWindowByTime(ctx, -time.Second, 10)
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestTemporalTreeChunkBoundaries compares every tree walk against a
// brute-force view across chunk splits, duplicates, and out-of-order insert
// timestamps.
func TestTemporalTreeChunkBoundaries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping temporal chunk-boundary test in short mode")
	}
	base := int64(1_000_000)
	n := 2*nodesPerChunk + 37

	// Out-of-order inserts, with every 17th id repeated at a later timestamp.
	type ins struct {
		ts int64
		id uint64
	}
	var inserts []ins
	for i := 0; i < n; i++ {
		inserts = append(inserts, ins{ts: base + int64(i)*3, id: uint64(i)})
	}
	for i := 0; i < n; i += 17 {
		inserts = append(inserts, ins{ts: base + int64(n)*3 + int64(i), id: uint64(i)})
	}
	rng := rand.New(rand.NewSource(23))
	rng.Shuffle(len(inserts), func(i, j int) { inserts[i], inserts[j] = inserts[j], inserts[i] })

	tt := NewTemporalTree(memory.NewSlabArena(64 * 1024 * 1024))
	for _, in := range inserts {
		tt.Insert(in.ts, in.id, float32(in.id%11))
	}
	tt.rebuildColumnarIndex()
	require.NotNil(t, tt.columnarIndex())

	ops := make([]temporalTestOp, len(inserts))
	for i, in := range inserts {
		ops[i] = temporalTestOp{ID: in.id, TS: in.ts, Vec: []float32{float32(in.id % 11), 0, 0, 0}}
	}

	// Reference walks over the timestamp-ordered entries.
	ordered := append([]temporalTestOp(nil), ops...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].TS < ordered[j].TS })
	newestIDs := func(count int) []uint64 {
		if count <= 0 {
			return nil
		}
		if count > len(ordered) {
			count = len(ordered)
		}
		out := make([]uint64, 0, count)
		for i := len(ordered) - 1; i >= 0 && len(out) < count; i-- {
			out = append(out, ordered[i].ID)
		}
		return out
	}
	earliestIDs := func(count int) []uint64 {
		if count <= 0 {
			return nil
		}
		if count > len(ordered) {
			count = len(ordered)
		}
		out := make([]uint64, count)
		for i := 0; i < count; i++ {
			out[i] = ordered[i].ID
		}
		return out
	}
	rangeIDs := func(start, end int64) []uint64 {
		var out []uint64
		for _, op := range ordered {
			if op.TS >= start && op.TS <= end {
				out = append(out, op.ID)
			}
		}
		return out
	}
	uniqueRangeIDs := func(start, end int64) []uint64 {
		seen := make(map[uint64]struct{})
		var out []uint64
		for i := len(ordered) - 1; i >= 0; i-- {
			op := ordered[i]
			if op.TS < start || op.TS > end {
				continue
			}
			if _, ok := seen[op.ID]; ok {
				continue
			}
			seen[op.ID] = struct{}{}
			out = append(out, op.ID)
		}
		return out
	}

	// Ranges deliberately straddle nodesPerChunk boundaries.
	bounds := []int64{
		base - 1, base, base + 1,
		base + int64(nodesPerChunk-1)*3, base + int64(nodesPerChunk)*3,
		base + int64(2*nodesPerChunk)*3, base + int64(n)*3,
		base + int64(n)*4,
	}
	for i := 0; i < len(bounds); i++ {
		for j := i + 1; j < len(bounds); j++ {
			start, end := bounds[i], bounds[j]
			label := fmt.Sprintf("range [%d,%d]", start, end)
			require.Equal(t, rangeIDs(start, end), tt.GetRange(start, end), "%s ascending", label)
			require.Equal(t, uniqueRangeIDs(start, end), tt.GetUniqueIDsInRange(start, end), "%s unique", label)

			wantRev := make([]uint64, 0, len(ordered))
			for k := len(ordered) - 1; k >= 0; k-- {
				if ordered[k].TS >= start && ordered[k].TS <= end {
					wantRev = append(wantRev, ordered[k].ID)
				}
			}
			require.Equal(t, wantRev, tt.GetRangeReversed(start, end), "%s reversed", label)
		}
	}

	for _, count := range []int{0, 1, nodesPerChunk - 1, nodesPerChunk, nodesPerChunk + 1, len(ordered), len(ordered) + 10} {
		require.Equal(t, earliestIDs(count), tt.GetEarliest(count), "earliest %d", count)
		require.Equal(t, newestIDs(count), tt.GetLatest(count), "latest %d", count)
	}

	// Unique-latest is newest occurrence first and stops after count distincts.
	for _, count := range []int{1, 10, 100} {
		seen := make(map[uint64]struct{})
		want := make([]uint64, 0, count)
		for i := len(ordered) - 1; i >= 0 && len(want) < count; i-- {
			id := ordered[i].ID
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			want = append(want, id)
		}
		require.Equal(t, want, tt.GetUniqueLatest(count), "unique latest %d", count)
	}

	// The chunked walk must agree with the columnar snapshot for every range.
	tt.setColumnarDisabled(true)
	require.Nil(t, tt.columnarSnapshot())
	for i := 0; i < len(bounds); i++ {
		for j := i + 1; j < len(bounds); j++ {
			start, end := bounds[i], bounds[j]
			require.Equal(t, rangeIDs(start, end), tt.GetRange(start, end), "chunked range [%d,%d]", start, end)
			require.Equal(t, uniqueRangeIDs(start, end), tt.GetUniqueIDsInRange(start, end), "chunked unique [%d,%d]", start, end)
		}
	}
}

// buildTemporalScaleIndex builds n entries with one entry per microsecond and
// eight shared vectors. It is shared by the scalability test and benchmark so
// both measure the same shape.
func buildTemporalScaleIndex(tb testing.TB, n int) *TemporalIndex {
	tb.Helper()
	ti := NewTemporalIndex(4)
	ti.SetAsyncIngestion(false)
	tb.Cleanup(func() { _ = ti.Close() })

	base := int64(1_700_000_000_000_000_000)
	vecs := make([][]float32, 8)
	for i := range vecs {
		vecs[i] = []float32{float32(i + 1), 0, 0, 0}
	}

	const chunk = 50_000
	ids := make([]uint64, 0, min(n, chunk))
	timestamps := make([]int64, 0, min(n, chunk))
	vectors := make([][]float32, 0, min(n, chunk))
	for lo := 0; lo < n; lo += chunk {
		hi := min(lo+chunk, n)
		ids = ids[:0]
		timestamps = timestamps[:0]
		vectors = vectors[:0]
		for i := lo; i < hi; i++ {
			ids = append(ids, uint64(i))
			timestamps = append(timestamps, base+int64(i))
			vectors = append(vectors, vecs[i%len(vecs)])
		}
		if err := ti.AddBatch(ids, vectors, timestamps, nil); err != nil {
			tb.Fatalf("AddBatch [%d,%d): %v", lo, hi, err)
		}
	}
	return ti
}

const temporalScaleBase = int64(1_700_000_000_000_000_000)

// bestTemporalQPS runs rounds of queries and returns the best round's QPS, so a
// GC pause or another test's retained heap cannot turn a throughput floor into a
// flaky failure.
func bestTemporalQPS(rounds, queries int, fn func(i int) error) (float64, error) {
	best := 0.0
	for r := 0; r < rounds; r++ {
		runtime.GC()
		start := time.Now()
		for i := 0; i < queries; i++ {
			if err := fn(i); err != nil {
				return 0, err
			}
		}
		qps := float64(queries) / time.Since(start).Seconds()
		if qps > best {
			best = qps
		}
	}
	return best, nil
}

// TestTemporalRangeScalability guards P2-4b: a realistic 1000-entry window over
// a 1M-row index must sustain at least 1000 QPS after warm-up.
func TestTemporalRangeScalability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping temporal scalability test in short mode")
	}
	const (
		n       = 1_000_000
		window  = 1000
		queries = 50
		rounds  = 3
	)
	ctx := context.Background()
	ti := buildTemporalScaleIndex(t, n)

	// Warm the lazy version-history index and columnar snapshot before timing;
	// the first query after ingest pays an O(N) build.
	if _, err := ti.SearchRange(ctx, temporalScaleBase, temporalScaleBase+window, 10); err != nil {
		t.Fatalf("warm-up range: %v", err)
	}
	if _, err := ti.SearchSlidingWindow(ctx, window, 10); err != nil {
		t.Fatalf("warm-up window: %v", err)
	}

	qps, err := bestTemporalQPS(rounds, queries, func(i int) error {
		lo := temporalScaleBase + int64((i*7919)%(n-window))
		res, err := ti.SearchRange(ctx, lo, lo+window, 10)
		if err != nil {
			return err
		}
		if len(res) != 10 {
			return fmt.Errorf("range query %d: got %d results, want 10", i, len(res))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("range query: %v", err)
	}
	t.Logf("TemporalRangeScalability: n=%d window=%d qps=%.1f", n, window, qps)
	if qps < 1000 {
		t.Fatalf("temporal range QPS %.1f below threshold 1000", qps)
	}

	windowQPS, err := bestTemporalQPS(rounds, queries, func(i int) error {
		res, err := ti.SearchSlidingWindow(ctx, window, 10)
		if err != nil {
			return err
		}
		if len(res) != 10 {
			return fmt.Errorf("window query %d: got %d results, want 10", i, len(res))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("window query: %v", err)
	}
	t.Logf("TemporalSlidingWindowScalability: n=%d window=%d qps=%.1f", n, window, windowQPS)
	if windowQPS < 1000 {
		t.Fatalf("temporal sliding-window QPS %.1f below threshold 1000", windowQPS)
	}
}

// BenchmarkTemporalRangeScale measures the sorted-index range walk at the scale
// used for the P2 target. Large cases are skipped in short mode.
func BenchmarkTemporalRangeScale(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{100_000, 500_000, 1_000_000} {
		if testing.Short() && n > 100_000 {
			b.Skip("skipping large temporal benchmark in short mode")
		}
		ti := buildTemporalScaleIndex(b, n)
		for _, window := range []int{1000, 10_000} {
			if window > n {
				continue
			}
			b.Run(fmt.Sprintf("n=%d/window=%d", n, window), func(b *testing.B) {
				// Warm the version-history index and columnar snapshot.
				if _, err := ti.SearchRange(ctx, temporalScaleBase, temporalScaleBase+int64(window), 10); err != nil {
					b.Fatal(err)
				}
				span := n - window
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; b.Loop(); i++ {
					lo := temporalScaleBase + int64((i*7919)%span)
					if _, err := ti.SearchRange(ctx, lo, lo+int64(window), 10); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
					b.ReportMetric(float64(b.N)/elapsed, "qps")
				}
			})
		}
	}
}
