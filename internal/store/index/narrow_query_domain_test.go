package index

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The query domain of the narrow integer element types.
//
// A query arrives as []float32 and is compared against vectors that live in the
// dataset's own integer domain. resolveHNSWComputer used to convert with a bare
// truncating cast, which has two failure modes:
//
//   - A query outside the destination range is an implementation-defined
//     conversion in Go; on amd64 and arm64 it yields the integer-indefinite
//     value, so a component of 1e9 against an int8 dataset becomes -128. A
//     sign flip on an arbitrary component, silently.
//   - A query inside [0,1) against a corpus in [0,127) becomes all zeros, so
//     the search returns the same neighbours for every query and nothing else
//     about it is wrong. That is the shape the 100k benchmark matrix had: int8
//     and uint8 are one byte wide and share an arena, a computer and a kernel,
//     and they reported 1162 QPS and 3974 QPS. The gap was not a throughput
//     gap at all - it was two degenerate searches traversing differently.
//
// These tests pin the conversion, and then the observable consequence: a query
// issued in the corpus's domain finds the vector it was drawn from.

// narrowDomain describes the integer domain a narrow element type stores into.
type narrowDomain struct {
	arrowType arrow.DataType
	vectorTyp types.VectorDataType
	lo, hi    int // half-open, matching what the corpus generator draws from
	name      string
}

func narrowDomains() []narrowDomain {
	return []narrowDomain{
		{arrow.PrimitiveTypes.Int8, types.VectorTypeInt8, 0, 127, "int8"},
		{arrow.PrimitiveTypes.Uint8, types.VectorTypeUint8, 0, 255, "uint8"},
		{arrow.PrimitiveTypes.Int16, types.VectorTypeInt16, 0, 1000, "int16"},
		{arrow.PrimitiveTypes.Uint16, types.VectorTypeUint16, 0, 1000, "uint16"},
	}
}

// buildFloat32NarrowCorpus emits an n x dims corpus. When d is nil the values
// are uniform in [0,1); otherwise they are uniform over d.lo..d.hi, i.e. in the
// domain the narrow element type actually stores into.
func buildFloat32NarrowCorpus(tb testing.TB, d *narrowDomain, n, dims int, rng *rand.Rand) (arrow.RecordBatch, [][]float32) {
	tb.Helper()
	dt := arrow.PrimitiveTypes.Float32
	if d != nil {
		dt = d.arrowType
	}
	b := array.NewRecordBuilder(memory.NewGoAllocator(), arrow.NewSchema(
		[]arrow.Field{{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), dt)}}, nil,
	))
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	stored := make([][]float32, n)
	for i := 0; i < n; i++ {
		listB.Append(true)
		stored[i] = make([]float32, dims)
		vals := make([]int, dims)
		for j := 0; j < dims; j++ {
			if d == nil {
				stored[i][j] = rng.Float32()
			} else {
				vals[j] = d.lo + rng.Intn(d.hi-d.lo) // #nosec G404 -- deterministic
				stored[i][j] = float32(vals[j])
			}
		}
		switch dt {
		case arrow.PrimitiveTypes.Int8:
			vb := listB.ValueBuilder().(*array.Int8Builder)
			for _, v := range vals {
				vb.Append(int8(v)) // #nosec G115 -- v is in [0,127)
			}
		case arrow.PrimitiveTypes.Uint8:
			vb := listB.ValueBuilder().(*array.Uint8Builder)
			for _, v := range vals {
				vb.Append(uint8(v)) // #nosec G115 -- v is in [0,255)
			}
		case arrow.PrimitiveTypes.Int16:
			vb := listB.ValueBuilder().(*array.Int16Builder)
			for _, v := range vals {
				vb.Append(int16(v)) // #nosec G115 -- v is in [0,1000)
			}
		case arrow.PrimitiveTypes.Uint16:
			vb := listB.ValueBuilder().(*array.Uint16Builder)
			for _, v := range vals {
				vb.Append(uint16(v)) // #nosec G115 -- v is in [0,1000)
			}
		default:
			vb := listB.ValueBuilder().(*array.Float32Builder)
			vb.AppendValues(stored[i], nil)
		}
	}
	rec := b.NewRecordBatch()
	rec.Retain()
	tb.Cleanup(rec.Release)
	return rec, stored
}

// narrowDomainIndex indexes rec at the given element type.
func narrowDomainIndex(tb testing.TB, rec arrow.RecordBatch, vt types.VectorDataType, dims int) *ArrowHNSW {
	tb.Helper()
	n := int(rec.NumRows())
	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = vt
	cfg.Dims = dims
	cfg.M = 16
	cfg.MMax = 16
	cfg.MMax0 = 16
	cfg.EfConstruction = 200
	cfg.Workers = 1

	ds := NewMockDataset("narrow-domain", rec.Schema())
	ds.Records = append(ds.Records, rec)
	idx := NewArrowHNSW(ds, &cfg, nil)

	rowIdxs := make([]int, n)
	batchIdx := make([]int, n)
	for i := 0; i < n; i++ {
		rowIdxs[i], batchIdx[i] = i, 0
	}
	if _, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdx); err != nil {
		tb.Fatal(err)
	}
	return idx
}

// narrowQueryStats runs two measurements over an index: how often a corpus
// vector, restated as a float32 query, comes back as its own nearest neighbour
// (k=1), and recall@10 against brute force on the same quantized corpus.
type narrowQueryStats struct {
	selfRetrieval float64
	recall        float64
}

func measureNarrowQuery(tb testing.TB, idx *ArrowHNSW, stored [][]float32, k, probes int, rng *rand.Rand) narrowQueryStats {
	tb.Helper()
	n := len(stored)

	self, found := 0, 0
	for p := 0; p < 50; p++ {
		id := p * 97 % n
		res, err := idx.Search(context.Background(), stored[id], 1, nil)
		if err != nil {
			tb.Fatal(err)
		}
		self++
		if len(res) > 0 && int(res[0].ID) == id {
			found++
		}
	}

	type scored struct {
		d float64
		i int
	}
	all := make([]scored, n)
	hits, total := 0, 0
	for p := 0; p < probes; p++ {
		q := stored[rng.Intn(n)]
		res, err := idx.Search(context.Background(), q, k, nil)
		if err != nil {
			tb.Fatal(err)
		}
		got := make(map[int]bool, len(res))
		for _, r := range res {
			got[int(r.ID)] = true
		}
		for i := 0; i < n; i++ {
			var s float64
			for j := range q {
				dd := float64(q[j]) - float64(stored[i][j])
				s += dd * dd
			}
			all[i] = scored{d: s, i: i}
		}
		sort.Slice(all, func(a, b int) bool { return all[a].d < all[b].d })
		for _, w := range all[:k] {
			if got[w.i] {
				hits++
			}
			total++
		}
	}
	return narrowQueryStats{
		selfRetrieval: float64(found) / float64(self),
		recall:        float64(hits) / float64(total),
	}
}

// TestNarrowQueryOutOfRangeIsDefined pins that a float32 query component outside
// the destination type's range is clamped to the range edge instead of hitting
// Go's implementation-defined float-to-integer conversion. Before this was
// defined, a component of 1e9 against an int8 dataset produced -128 - a sign
// flip, not a rounding error.
func TestNarrowQueryOutOfRangeIsDefined(t *testing.T) {
	cases := []struct {
		in   float32
		want float32
	}{
		{0, 0},
		{1e9, narrowInt8Max},
		{-1e9, narrowInt8Min},
		{float32(math.Inf(1)), narrowInt8Max},
		{float32(math.Inf(-1)), narrowInt8Min},
		{float32(math.NaN()), 0}, // NaN takes every comparison false, so it clamps low
		{3.4, 3},
		{-3.4, -3},
		{3.5, 4},
		{-3.5, -4},
	}
	for _, c := range cases {
		if got := narrowInt8(c.in); math.Abs(float64(got)-float64(c.want)) > 1e-6 {
			t.Errorf("narrowInt8(%v) = %v, want %v", c.in, got, c.want)
		}
	}

	// Round-tripping the destination type's own extremes must be exact, because
	// a client that sends queries in the corpus domain sees no change at all.
	if got := narrowInt8(narrowInt8Max); got != 127 {
		t.Errorf("narrowInt8(%v) = %d, want 127", narrowInt8Max, got)
	}
	if got := narrowInt8(narrowInt8Min); got != -127 {
		t.Errorf("narrowInt8(%v) = %d, want -127", narrowInt8Min, got)
	}
	if got := narrowUint8(255); got != 255 {
		t.Errorf("narrowUint8(255) = %d, want 255", got)
	}
	if got := narrowUint16(65535); got != 65535 {
		t.Errorf("narrowUint16(65535) = %d, want 65535", got)
	}
	// A negative must not wrap to MaxUint32.
	if got := narrowUint32(-1); got != 0 {
		t.Errorf("narrowUint32(-1) = %d, want 0", got)
	}
}

// TestNarrowQueryDomainRecall measures recall@10 for float32 and for every
// narrow integer type with the query issued in the corpus's own domain, and
// gates the narrow types against the float32 figure measured in the same run.
//
// It is gated relatively on purpose. The in-process MockDataset harness reaches
// recall@10 of about 0.70 even for unquantized float32 on uniform random 128-d
// vectors - every point is nearly equidistant from every other, so the graph
// itself is the limit, not the storage width. An absolute threshold would
// therefore be a threshold on the harness. What is meaningful is whether
// narrowing the element type costs anything measurable on top of that ceiling.
func TestNarrowQueryDomainRecall(t *testing.T) {
	if testing.Short() {
		t.Skip("builds five 10k indexes and runs brute force")
	}
	const n, dims, k, probes = 10_000, 128, 10, 20

	type subject struct {
		name string
		d    *narrowDomain
		vt   types.VectorDataType
	}
	subjects := []subject{{"float32", nil, types.VectorTypeFloat32}}
	for _, d := range narrowDomains() {
		subjects = append(subjects, subject{d.name, &d, d.vectorTyp})
	}

	stats := make(map[string]narrowQueryStats, len(subjects))
	for _, s := range subjects {
		rng := rand.New(rand.NewSource(7)) // #nosec G404 -- deterministic
		rec, stored := buildFloat32NarrowCorpus(t, s.d, n, dims, rng)
		idx := narrowDomainIndex(t, rec, s.vt, dims)
		st := measureNarrowQuery(t, idx, stored, k, probes, rand.New(rand.NewSource(11))) // #nosec G404 -- deterministic
		stats[s.name] = st
		t.Logf("NARROW_DOMAIN %-8s self-retrieval=%.2f recall@%d=%.4f",
			s.name, st.selfRetrieval, k, st.recall)
	}

	base := stats["float32"]

	// Absolute floor: a query in the corpus's domain must at least be findable.
	// With the truncating cast these were 0.00, because every probe collapsed to
	// the zero vector and the answer no longer depended on the query at all.
	for name, st := range stats {
		if st.selfRetrieval < 0.50 {
			t.Errorf("%s self-retrieval %.2f; a query restating a corpus vector must "+
				"come back as its own nearest neighbour at least half the time, so the "+
				"query conversion is still losing the query direction", name, st.selfRetrieval)
		}
	}

	// Relative gate: quantization must not cost more than a fifth of what the
	// unquantized path costs on this harness.
	if base.recall > 0 {
		for name, st := range stats {
			if name == "float32" {
				continue
			}
			if ratio := st.recall / base.recall; ratio < 0.80 {
				t.Errorf("%s recall@%d %.4f is %.0f%% of the float32 figure %.4f measured "+
					"in the same run; narrowing the element type should cost little once the "+
					"query is in the corpus domain", name, k, st.recall, ratio*100, base.recall)
			}
		}
	}
}
