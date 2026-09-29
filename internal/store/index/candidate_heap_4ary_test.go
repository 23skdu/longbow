package index

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
)

func uniqueShuffledCands(rnd *rand.Rand, n int) []types.Candidate {
	c := make([]types.Candidate, n)
	for i := range c {
		c[i] = types.Candidate{ID: uint32(i), Dist: float32(i), Level: i % 8}
	}
	rnd.Shuffle(n, func(i, j int) { c[i], c[j] = c[j], c[i] })
	return c
}

func refSorted(c []types.Candidate, asc bool) []types.Candidate {
	ref := make([]types.Candidate, len(c))
	copy(ref, c)
	sort.Slice(ref, func(i, j int) bool {
		if asc {
			return ref[i].Dist < ref[j].Dist
		}
		return ref[i].Dist > ref[j].Dist
	})
	return ref
}

func checkMin4Ary(t *testing.T, h MinCandidateHeapAdapter) {
	t.Helper()
	n := len(h)
	for i := 0; i < n; i++ {
		for c := 4*i + 1; c <= 4*i+4 && c < n; c++ {
			if h[c].Dist < h[i].Dist {
				t.Fatalf("4-ary min-heap violated: child %d (dist %v) < parent %d (dist %v), n=%d",
					c, h[c].Dist, i, h[i].Dist, n)
			}
		}
	}
	if n > 0 {
		for i := 1; i < n; i++ {
			if h[i].Dist < h[0].Dist {
				t.Fatalf("root %v is not the minimum: %v at %d", h[0].Dist, h[i].Dist, i)
			}
		}
	}
}

func checkMax4Ary(t *testing.T, h MaxCandidateHeapAdapter) {
	t.Helper()
	n := len(h)
	for i := 0; i < n; i++ {
		for c := 4*i + 1; c <= 4*i+4 && c < n; c++ {
			if h[c].Dist > h[i].Dist {
				t.Fatalf("4-ary max-heap violated: child %d (dist %v) > parent %d (dist %v), n=%d",
					c, h[c].Dist, i, h[i].Dist, n)
			}
		}
	}
	if n > 0 {
		for i := 1; i < n; i++ {
			if h[i].Dist > h[0].Dist {
				t.Fatalf("root %v is not the maximum: %v at %d", h[0].Dist, h[i].Dist, i)
			}
		}
	}
}

func candEqual(a, b types.Candidate) bool {
	return a.ID == b.ID && a.Dist == b.Dist && a.Level == b.Level
}

func TestMinCandidateHeapAdapter_DrainMatchesSortedReference(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))
	for n := 0; n <= 1000; n++ {
		cands := uniqueShuffledCands(rnd, n)
		ref := refSorted(cands, true)

		var h MinCandidateHeapAdapter
		var bh binaryMinAdapter
		for _, c := range cands {
			h.PushCandidate(c)
			bh.pushCandidate(c)
		}
		if h.Len() != n {
			t.Fatalf("n=%d: Len() = %d after %d pushes", n, h.Len(), n)
		}
		checkMin4Ary(t, h)

		for k := 0; k < n; k++ {
			got := h.PopCandidate()
			if !candEqual(got, ref[k]) {
				t.Fatalf("n=%d: pop %d = %+v, want %+v", n, k, got, ref[k])
			}
			gotBin := bh.popCandidate()
			if !candEqual(gotBin, got) {
				t.Fatalf("n=%d: pop %d binary=%+v 4ary=%+v", n, k, gotBin, got)
			}
			checkMin4Ary(t, h)
		}
		if h.Len() != 0 {
			t.Fatalf("n=%d: heap not empty after drain, Len()=%d", n, h.Len())
		}
	}
}

func TestMaxCandidateHeapAdapter_DrainMatchesSortedReference(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))
	for n := 0; n <= 1000; n++ {
		cands := uniqueShuffledCands(rnd, n)
		ref := refSorted(cands, false)

		var h MaxCandidateHeapAdapter
		var bh binaryMaxAdapter
		for _, c := range cands {
			h.PushCandidate(c)
			bh.pushCandidate(c)
		}
		if h.Len() != n {
			t.Fatalf("n=%d: Len() = %d after %d pushes", n, h.Len(), n)
		}
		checkMax4Ary(t, h)

		for k := 0; k < n; k++ {
			got := h.PopCandidate()
			if !candEqual(got, ref[k]) {
				t.Fatalf("n=%d: pop %d = %+v, want %+v", n, k, got, ref[k])
			}
			gotBin := bh.popCandidate()
			if !candEqual(gotBin, got) {
				t.Fatalf("n=%d: pop %d binary=%+v 4ary=%+v", n, k, gotBin, got)
			}
			checkMax4Ary(t, h)
		}
		if h.Len() != 0 {
			t.Fatalf("n=%d: heap not empty after drain, Len()=%d", n, h.Len())
		}
	}
}

type refHeapModel struct {
	items []types.Candidate
	min   bool
}

func (m *refHeapModel) push(c types.Candidate) { m.items = append(m.items, c) }

func (m *refHeapModel) pop() (types.Candidate, bool) {
	if len(m.items) == 0 {
		return types.Candidate{}, false
	}
	best := 0
	for i := 1; i < len(m.items); i++ {
		if m.min {
			if m.items[i].Dist < m.items[best].Dist {
				best = i
			}
		} else if m.items[i].Dist > m.items[best].Dist {
			best = i
		}
	}
	c := m.items[best]
	m.items[best] = m.items[len(m.items)-1]
	m.items = m.items[:len(m.items)-1]
	return c, true
}

func (m *refHeapModel) len() int { return len(m.items) }

func TestCandidateHeapAdapter_InterleavedMatchesReference(t *testing.T) {
	for _, seed := range []int64{42, 7, 2026} {
		seed := seed
		t.Run("min", func(t *testing.T) {
			runInterleaved(t, seed, true)
		})
		t.Run("max", func(t *testing.T) {
			runInterleaved(t, seed, false)
		})
	}
}

func runInterleaved(t *testing.T, seed int64, min bool) {
	t.Helper()
	const (
		ops   = 4000
		limit = 1000
	)
	rnd := rand.New(rand.NewSource(seed))
	pool := uniqueShuffledCands(rnd, 3000)
	next := 0
	model := &refHeapModel{min: min}

	var h4 MinCandidateHeapAdapter
	var h4Max MaxCandidateHeapAdapter
	var hbMin binaryMinAdapter
	var hbMax binaryMaxAdapter

	push4 := func(c types.Candidate) {
		if min {
			h4.PushCandidate(c)
			hbMin.pushCandidate(c)
		} else {
			h4Max.PushCandidate(c)
			hbMax.pushCandidate(c)
		}
	}
	pop4 := func() types.Candidate {
		if min {
			return h4.PopCandidate()
		}
		return h4Max.PopCandidate()
	}
	popBin := func() types.Candidate {
		if min {
			return hbMin.popCandidate()
		}
		return hbMax.popCandidate()
	}
	size := func() int {
		if min {
			return h4.Len()
		}
		return h4Max.Len()
	}

	for op := 0; op < ops; op++ {
		wantPush := next < len(pool) && (size() == 0 || size() < limit && rnd.Intn(100) < 60)
		if wantPush {
			c := pool[next]
			next++
			push4(c)
			model.push(c)
		} else if size() > 0 {
			want, ok := model.pop()
			if !ok {
				t.Fatalf("op=%d: reference model unexpectedly empty", op)
			}
			got := pop4()
			if !candEqual(got, want) {
				t.Fatalf("op=%d: 4ary pop = %+v, want %+v", op, got, want)
			}
			gotBin := popBin()
			if !candEqual(gotBin, want) {
				t.Fatalf("op=%d: binary pop = %+v, want %+v", op, gotBin, want)
			}
		}
		if size() != model.len() {
			t.Fatalf("op=%d: heap size %d != model size %d", op, size(), model.len())
		}
		if min {
			checkMin4Ary(t, h4)
		} else {
			checkMax4Ary(t, h4Max)
		}
	}

	for model.len() > 0 {
		want, _ := model.pop()
		got := pop4()
		if !candEqual(got, want) {
			t.Fatalf("final drain: pop = %+v, want %+v", got, want)
		}
	}
	if size() != 0 {
		t.Fatalf("final drain left %d elements", size())
	}
}

func TestCandidateHeapAdapter_4AryInvariantAfterRandomOps(t *testing.T) {
	for _, seed := range []int64{42, 99, 123456} {
		rnd := rand.New(rand.NewSource(seed))
		pool := uniqueShuffledCands(rnd, 2000)
		next := 0

		var hMin MinCandidateHeapAdapter
		var hMax MaxCandidateHeapAdapter
		for op := 0; op < 2000; op++ {
			c := pool[next]
			next++
			hMin.PushCandidate(c)
			hMax.PushCandidate(c)
			checkMin4Ary(t, hMin)
			checkMax4Ary(t, hMax)

			if next%3 == 0 && hMin.Len() > 0 {
				hMin.PopCandidate()
				hMax.PopCandidate()
				checkMin4Ary(t, hMin)
				checkMax4Ary(t, hMax)
			}
		}
		if hMin.Len() == 0 {
			t.Fatalf("seed=%d: expected non-empty heap for final drain", seed)
		}
	}
}

func TestCandidateHeapAdapter_TiedDistsTerminate(t *testing.T) {
	const n = 500

	t.Run("all_ties_drain", func(t *testing.T) {
		var hMin MinCandidateHeapAdapter
		var hMax MaxCandidateHeapAdapter
		for i := 0; i < n; i++ {
			c := types.Candidate{ID: uint32(i), Dist: 1.5}
			hMin.PushCandidate(c)
			hMax.PushCandidate(c)
		}
		checkMin4Ary(t, hMin)
		checkMax4Ary(t, hMax)

		seenMin := make(map[uint32]bool, n)
		for k := 0; hMin.Len() > 0; k++ {
			if k > 4*n {
				t.Fatalf("min drain exceeded budget with %d left", hMin.Len())
			}
			seenMin[hMin.PopCandidate().ID] = true
		}
		seenMax := make(map[uint32]bool, n)
		for k := 0; hMax.Len() > 0; k++ {
			if k > 4*n {
				t.Fatalf("max drain exceeded budget with %d left", hMax.Len())
			}
			seenMax[hMax.PopCandidate().ID] = true
		}
		if len(seenMin) != n || len(seenMax) != n {
			t.Fatalf("drained %d/%d unique IDs, want %d", len(seenMin), len(seenMax), n)
		}
	})

	t.Run("interleaved_ties", func(t *testing.T) {
		rnd := rand.New(rand.NewSource(42))
		var hMin MinCandidateHeapAdapter
		var hMax MaxCandidateHeapAdapter
		var refMin, refMax []types.Candidate
		for op := 0; op < 3000; op++ {
			if hMin.Len() == 0 || rnd.Intn(100) < 60 {
				c := types.Candidate{ID: uint32(op), Dist: float32((op / 4) % 16)}
				hMin.PushCandidate(c)
				hMax.PushCandidate(c)
				refMin = append(refMin, c)
				refMax = append(refMax, c)
			} else {
				wantMin := refMin[0]
				for i := 1; i < len(refMin); i++ {
					if refMin[i].Dist < wantMin.Dist {
						wantMin = refMin[i]
					}
				}
				wantMax := refMax[0]
				for i := 1; i < len(refMax); i++ {
					if refMax[i].Dist > wantMax.Dist {
						wantMax = refMax[i]
					}
				}
				gotMin := hMin.PopCandidate()
				gotMax := hMax.PopCandidate()
				if gotMin.Dist != wantMin.Dist {
					t.Fatalf("op=%d: min pop dist %v, want %v", op, gotMin.Dist, wantMin.Dist)
				}
				if gotMax.Dist != wantMax.Dist {
					t.Fatalf("op=%d: max pop dist %v, want %v", op, gotMax.Dist, wantMax.Dist)
				}
				refMin = removeCand(refMin, wantMin)
				refMax = removeCand(refMax, wantMax)
			}
			if hMin.Len() > 1000 {
				t.Fatalf("op=%d: heap grew beyond limit: %d", op, hMin.Len())
			}
			if hMin.Len() != len(refMin) || hMax.Len() != len(refMax) {
				t.Fatalf("op=%d: heap sizes min=%d max=%d, refs min=%d max=%d",
					op, hMin.Len(), hMax.Len(), len(refMin), len(refMax))
			}
			checkMin4Ary(t, hMin)
			checkMax4Ary(t, hMax)
		}
	})
}

func removeCand(s []types.Candidate, c types.Candidate) []types.Candidate {
	for i := range s {
		if s[i].ID == c.ID {
			s[i] = s[len(s)-1]
			return s[:len(s)-1]
		}
	}
	return s
}

func TestCandidateHeapAdapter_EdgeCases(t *testing.T) {
	t.Run("empty_pop_min_panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("PopCandidate on empty min heap did not panic")
			}
		}()
		var h MinCandidateHeapAdapter
		h.PopCandidate()
	})

	t.Run("empty_pop_max_panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("PopCandidate on empty max heap did not panic")
			}
		}()
		var h MaxCandidateHeapAdapter
		h.PopCandidate()
	})

	t.Run("single_element", func(t *testing.T) {
		c := types.Candidate{ID: 7, Dist: 3.25}
		var hMin MinCandidateHeapAdapter
		hMin.PushCandidate(c)
		checkMin4Ary(t, hMin)
		if hMin[0] != c {
			t.Fatalf("root = %+v, want %+v", hMin[0], c)
		}
		if got := hMin.PopCandidate(); got != c {
			t.Fatalf("pop = %+v, want %+v", got, c)
		}
		if hMin.Len() != 0 {
			t.Fatalf("Len() = %d after popping the only element", hMin.Len())
		}

		var hMax MaxCandidateHeapAdapter
		hMax.PushCandidate(c)
		checkMax4Ary(t, hMax)
		if got := hMax.PopCandidate(); got != c {
			t.Fatalf("pop = %+v, want %+v", got, c)
		}
		if hMax.Len() != 0 {
			t.Fatalf("Len() = %d after popping the only element", hMax.Len())
		}
	})

	for _, n := range []int{4, 5} {
		rnd := rand.New(rand.NewSource(int64(n)))
		cands := uniqueShuffledCands(rnd, n)
		refAsc := refSorted(cands, true)
		refDesc := refSorted(cands, false)

		var hMin MinCandidateHeapAdapter
		var hMax MaxCandidateHeapAdapter
		for _, c := range cands {
			hMin.PushCandidate(c)
			hMax.PushCandidate(c)
			checkMin4Ary(t, hMin)
			checkMax4Ary(t, hMax)
		}
		if hMin[0].Dist != refAsc[0].Dist {
			t.Fatalf("n=%d: min root %v, want %v", n, hMin[0].Dist, refAsc[0].Dist)
		}
		if hMax[0].Dist != refDesc[0].Dist {
			t.Fatalf("n=%d: max root %v, want %v", n, hMax[0].Dist, refDesc[0].Dist)
		}
		for k := 0; k < n; k++ {
			if got := hMin.PopCandidate(); !candEqual(got, refAsc[k]) {
				t.Fatalf("n=%d: min pop %d = %+v, want %+v", n, k, got, refAsc[k])
			}
			if got := hMax.PopCandidate(); !candEqual(got, refDesc[k]) {
				t.Fatalf("n=%d: max pop %d = %+v, want %+v", n, k, got, refDesc[k])
			}
		}
	}
}

func TestCandidateHeapAdapter_UpAtRootAndDownReturn(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))
	cands := uniqueShuffledCands(rnd, 32)

	var h MinCandidateHeapAdapter
	for _, c := range cands {
		h.PushCandidate(c)
	}
	before := append([]types.Candidate(nil), h...)
	h.up(0)
	for i := range before {
		if before[i] != h[i] {
			t.Fatalf("up(0) mutated the heap at index %d", i)
		}
	}

	var ordered MinCandidateHeapAdapter
	for i := 0; i < 8; i++ {
		ordered = append(ordered, types.Candidate{ID: uint32(i), Dist: float32(i)})
	}
	if ordered.down(0, ordered.Len()) {
		t.Fatal("down on an already-ordered heap returned true")
	}

	var disordered MinCandidateHeapAdapter
	disordered = append(disordered, types.Candidate{ID: 0, Dist: 9})
	for i := 1; i < 8; i++ {
		disordered = append(disordered, types.Candidate{ID: uint32(i), Dist: float32(i)})
	}
	if !disordered.down(0, disordered.Len()) {
		t.Fatal("down on a disordered heap returned false")
	}
	checkMin4Ary(t, disordered)

	var orderedMax MaxCandidateHeapAdapter
	for i := 0; i < 8; i++ {
		orderedMax = append(orderedMax, types.Candidate{ID: uint32(i), Dist: float32(8 - i)})
	}
	if orderedMax.down(0, orderedMax.Len()) {
		t.Fatal("max down on an already-ordered heap returned true")
	}

	var disorderedMax MaxCandidateHeapAdapter
	disorderedMax = append(disorderedMax, types.Candidate{ID: 0, Dist: 1})
	for i := 1; i < 8; i++ {
		disorderedMax = append(disorderedMax, types.Candidate{ID: uint32(i), Dist: float32(8 - i)})
	}
	if !disorderedMax.down(0, disorderedMax.Len()) {
		t.Fatal("max down on a disordered heap returned false")
	}
	checkMax4Ary(t, disorderedMax)
}

func TestCandidateHeapAdapter_NoAllocations(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))
	cands := uniqueShuffledCands(rnd, 64)

	hMin := make(MinCandidateHeapAdapter, 0, 64)
	hMax := make(MaxCandidateHeapAdapter, 0, 64)
	for _, c := range cands {
		hMin.PushCandidate(c)
		hMax.PushCandidate(c)
	}

	minAllocs := testing.AllocsPerRun(1000, func() {
		hMin.PopCandidate()
		hMin.PushCandidate(cands[0])
	})
	if minAllocs != 0 {
		t.Fatalf("min adapter allocated %v times per push/pop, want 0", minAllocs)
	}

	maxAllocs := testing.AllocsPerRun(1000, func() {
		hMax.PopCandidate()
		hMax.PushCandidate(cands[1])
	})
	if maxAllocs != 0 {
		t.Fatalf("max adapter allocated %v times per push/pop, want 0", maxAllocs)
	}
}
