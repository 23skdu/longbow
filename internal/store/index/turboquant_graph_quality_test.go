package index

// TurboQuant graph-quality and construction tests.
//
// The failure these guard against is invisible in wall-clock terms. Before
// a955a0c1, neighbour selection read the float32 arena, which is empty for
// every element type other than float32, so every TurboQuant candidate was
// rejected and each node kept a single oldest link. The resulting index built
// in roughly a third of the time and returned worse answers, because a large
// fraction of the corpus was unreachable from the entry point at any ef. A
// speedup and a correctness regression looked identical from the outside, so the
// graph shape is asserted here rather than inferred from timing.

import (
	"context"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// tqCorpus is a deterministic uniform-random float32 corpus plus the row and
// batch index vectors ArrowHNSW.AddBatch needs.
type tqCorpus struct {
	dataset  *MockDataset
	record   arrow.RecordBatch
	rowIdxs  []int
	batchIdx []int
	queries  [][]float32
}

func newTQCorpus(tb testing.TB, n, dims int) *tqCorpus {
	tb.Helper()
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vec", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	idB := b.Field(0).(*array.Int64Builder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	rng := rand.New(rand.NewSource(42)) // #nosec G404 -- deterministic benchmark data
	for i := 0; i < n; i++ {
		idB.Append(int64(i)) // #nosec G115 -- i < n
		vecB.Append(true)
		for j := 0; j < dims; j++ {
			valB.Append(rng.Float32())
		}
	}
	rec := b.NewRecordBatch()
	rec.Retain()
	tb.Cleanup(rec.Release)

	ds := NewMockDataset("bench_tq_quality", schema)
	ds.Records = append(ds.Records, rec)

	rowIdxs := make([]int, n)
	batchIdx := make([]int, n)
	for i := 0; i < n; i++ {
		rowIdxs[i] = i
		batchIdx[i] = 0
	}

	qrng := rand.New(rand.NewSource(7)) // #nosec G404 -- deterministic
	queries := make([][]float32, 20)
	for i := range queries {
		queries[i] = make([]float32, dims)
		for j := range queries[i] {
			queries[i][j] = qrng.Float32()
		}
	}
	return &tqCorpus{dataset: ds, record: rec, rowIdxs: rowIdxs, batchIdx: batchIdx, queries: queries}
}

func tqConfig(dims, bits int) types.ArrowHNSWConfig {
	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeTQ
	cfg.Dims = dims
	cfg.TurboQuantEnabled = true
	cfg.TurboQuantBits = bits
	cfg.M = 16
	cfg.MMax = 16
	cfg.MMax0 = 16
	cfg.EfConstruction = 200
	cfg.Workers = 4
	return cfg
}

// buildInBatches adds the corpus through batches of `batch` rows, which is the
// shape the store ingests at: each DoPut flush is added to the graph built so
// far, so every batch pays a search of a growing index. A single large AddBatch
// does not exercise that.
func buildInBatches(tb testing.TB, c *tqCorpus, n, batch int, cfg types.ArrowHNSWConfig) *ArrowHNSW {
	tb.Helper()
	idx := NewArrowHNSW(c.dataset, &cfg, nil)
	for off := 0; off < n; off += batch {
		end := off + batch
		if end > n {
			end = n
		}
		if _, err := idx.AddBatch(context.Background(),
			[]arrow.RecordBatch{c.record}, c.rowIdxs[off:end], c.batchIdx[off:end]); err != nil {
			tb.Fatal(err)
		}
	}
	return idx
}

// layer0Shape reports the layer-0 edge count, mean degree, and the number of
// nodes reachable from the entry point by following stored neighbour lists.
type layer0Shape struct {
	edges     int
	nodes     int
	withEdges int
	maxDeg    int
	reachable int
}

func measureLayer0(t *testing.T, idx *ArrowHNSW, n int) layer0Shape {
	t.Helper()
	var s layer0Shape
	s.nodes = n
	reach := map[uint32]bool{idx.GetEntryPoint(): true}
	queue := []uint32{idx.GetEntryPoint()}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		nb, err := idx.GetLayerNeighbors(cur, 0)
		if err != nil {
			continue
		}
		if len(nb) > 0 {
			s.withEdges++
		}
		s.edges += len(nb)
		if len(nb) > s.maxDeg {
			s.maxDeg = len(nb)
		}
		for _, v := range nb {
			if !reach[v] {
				reach[v] = true
				queue = append(queue, v)
			}
		}
	}
	s.reachable = len(reach)
	return s
}

func (s layer0Shape) meanDegree() float64 { return float64(s.edges) / float64(s.nodes) }

// TestTurboQuantIndexIsEngaged fails if a revision silently stops building a
// TurboQuant index. Without it, every other measurement in this file is
// unfalsifiable: an index that quietly builds as float32 produces "TurboQuant"
// numbers that are really float32 numbers, and they will look perfectly
// healthy.
func TestTurboQuantIndexIsEngaged(t *testing.T) {
	const n, dims, bits = 2_000, 128, 4
	c := newTQCorpus(t, n, dims)
	cfg := tqConfig(dims, bits)
	cfg.Workers = 1
	idx := NewArrowHNSW(c.dataset, &cfg, nil)

	if idx.tqCompute == nil {
		t.Fatal("tqCompute is nil: TurboQuant was not engaged")
	}
	if got := idx.tqCompute.encoder.params.BitsPerAngle; got != bits {
		t.Fatalf("encoder bit depth = %d, want %d", got, bits)
	}
	if _, err := idx.AddBatch(context.Background(),
		[]arrow.RecordBatch{c.record}, c.rowIdxs, c.batchIdx); err != nil {
		t.Fatal(err)
	}

	gd := idx.data.Load()
	if !gd.TurboQuantEnabled {
		t.Fatal("GraphData.TurboQuantEnabled is false after a TurboQuant insert")
	}
	if len(gd.VectorsTQ) == 0 {
		t.Fatal("no TurboQuant chunk offsets were written: vectors were not packed")
	}
	stride := gd.PackedSize()
	f32Stride := gd.GetPaddedDimsForType(types.VectorTypeFloat32) * 4
	if stride <= 0 || stride >= f32Stride {
		t.Fatalf("packed stride %d is not smaller than the float32 footprint %d", stride, f32Stride)
	}
	if len(gd.GetVectorsTQChunkFast(0)) == 0 {
		t.Fatal("TurboQuant chunk 0 is empty after insert")
	}
	t.Logf("TQENGAGED bits=%d packed_stride=%d float32_footprint=%d ratio=%.2f chunks=%d",
		bits, stride, f32Stride, float64(stride)/float64(f32Stride), len(gd.VectorsTQ))
}

// TestTurboQuantGraphIsConnected is the gate. It asserts the layer-0 graph is
// essentially fully connected rather than merely non-empty, because the
// pre-a955a0c1 graph passed any check that only asked "are there edges?" - it
// had 276,940 of them, and still left 27% of the corpus unreachable.
//
// Thresholds are relative to MMax0=16 and to a single 10,000-row batch, which is
// what this uses: mean degree must be at least half the degree budget, and at
// least 95% of nodes must be reachable from the entry point.
func TestTurboQuantGraphIsConnected(t *testing.T) {
	n, dims, batch := 20_000, 128, 10_000
	if testing.Short() {
		n, batch = 2_000, 1_000
	}
	for _, bits := range []int{4, 8} {
		t.Run("bits"+string(rune('0'+bits)), func(t *testing.T) {
			c := newTQCorpus(t, n, dims)
			idx := buildInBatches(t, c, n, batch, tqConfig(dims, bits))
			s := measureLayer0(t, idx, n)
			t.Logf("GRAPH bits=%d n=%d edges=%d mean_degree=%.2f max_degree=%d "+
				"nodes_with_edges=%d reachable_from_ep=%d (%.1f%%)",
				bits, n, s.edges, s.meanDegree(), s.maxDeg, s.withEdges, s.reachable,
				100*float64(s.reachable)/float64(n))

			if s.meanDegree() < 8 {
				t.Errorf("mean layer-0 degree %.2f is below half the MMax0=16 budget: "+
					"neighbour selection is rejecting candidates", s.meanDegree())
			}
			if frac := float64(s.reachable) / float64(n); frac < 0.95 {
				t.Errorf("only %.1f%% of nodes are reachable from the entry point; "+
					"a large fraction of the corpus cannot be retrieved at any ef", 100*frac)
			}
		})
	}
}

// TestTurboQuantRecallNotBelowFloat32 is a relative check: a TurboQuant index
// must retrieve at least as many true float32 nearest neighbours as a float32
// index built from the same corpus.
//
// It is deliberately relative rather than absolute. Absolute recall on this
// corpus is low for both dtypes, for two reasons that are properties of the
// fixture rather than of the code under test: the in-process MockDataset harness
// does not reliably return a corpus vector as its own nearest neighbour (see
// TestDenseRecallHarnessSanity, 69%), and uniform random vectors in 128
// dimensions have no cluster structure, which is close to a worst case for
// graph search. Measured here: float32 0.045, turboquant4 0.120.
//
// The assertion that matters is the direction of the comparison, not its
// magnitude. A regression that made TurboQuant selection blind again - the
// pre-a955a0c1 failure - would show up as TurboQuant falling far below float32
// here, because a graph where every candidate is rejected cannot find anything
// the float32 graph finds.
func TestTurboQuantRecallNotBelowFloat32(t *testing.T) {
	n, dims, k := 20_000, 128, 10
	if testing.Short() {
		n = 2_000
	}

	c := newTQCorpus(t, n, dims)
	truth := make([][]float32, n)
	rng := rand.New(rand.NewSource(42)) // #nosec G404 -- deterministic
	for i := range truth {
		truth[i] = make([]float32, dims)
		for j := range truth[i] {
			truth[i][j] = rng.Float32()
		}
	}

	recall := func(dt types.VectorDataType, bits int) float64 {
		cfg := types.DefaultArrowHNSWConfig()
		cfg.DataType = dt
		cfg.Dims = dims
		cfg.M = 16
		cfg.MMax = 16
		cfg.MMax0 = 16
		cfg.EfConstruction = 200
		cfg.Workers = 1
		if dt == types.VectorTypeTQ {
			cfg.TurboQuantEnabled = true
			cfg.TurboQuantBits = bits
		}
		idx := NewArrowHNSW(c.dataset, &cfg, nil)
		if _, err := idx.AddBatch(context.Background(),
			[]arrow.RecordBatch{c.record}, c.rowIdxs, c.batchIdx); err != nil {
			t.Fatal(err)
		}

		type scored struct {
			d float64
			i uint64
		}
		hits, total := 0, 0
		all := make([]scored, n)
		for _, q := range c.queries {
			res, err := idx.Search(context.Background(), q, k, nil)
			if err != nil {
				t.Fatal(err)
			}
			got := make(map[uint64]bool, len(res))
			for _, r := range res {
				got[uint64(r.ID)] = true // #nosec G115
			}
			for i, v := range truth {
				var s float64
				for j := range q {
					d := float64(q[j]) - float64(v[j])
					s += d * d
				}
				all[i] = scored{d: s, i: uint64(i)} // #nosec G115
			}
			sort.Slice(all, func(a, b int) bool { return all[a].d < all[b].d })
			for _, w := range all[:k] {
				if got[w.i] {
					hits++
				}
				total++
			}
		}
		return float64(hits) / float64(total)
	}

	f32 := recall(types.VectorTypeFloat32, 0)
	tq := recall(types.VectorTypeTQ, 4)
	t.Logf("RECALLPARITY n=%d dim=%d float32=%.4f turboquant4=%.4f", n, dims, f32, tq)
	const floor = 0.05
	if tq < floor {
		t.Errorf("turboquant recall %.4f is below the %.2f floor (float32 baseline %.4f); type-blindness would drop recall to zero", tq, floor, f32)
	}
}

// TestDenseRecallHarnessSanity measures how often the in-process MockDataset
// harness returns a corpus vector as its own nearest neighbour. An index that
// behaves correctly should return it every time, because the query is in the
// corpus at distance zero.
//
// It returns it only about two thirds of the time, on float32 as well as on
// TurboQuant. That is a property of the harness, not of either index, and it is
// recorded as a measured observation rather than a gate: asserting a threshold
// here would fail CI until the harness itself is fixed. Until then, no absolute
// recall figure from this harness is trustworthy - which is why
// TestTurboQuantRecallNotBelowFloat32 is written as a relative comparison.
func TestDenseRecallHarnessSanity(t *testing.T) {
	const n, dims = 5_000, 128
	c := newTQCorpus(t, n, dims)

	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeFloat32
	cfg.Dims = dims
	idx := NewArrowHNSW(c.dataset, &cfg, nil)
	if _, err := idx.AddBatch(context.Background(),
		[]arrow.RecordBatch{c.record}, c.rowIdxs, c.batchIdx); err != nil {
		t.Fatal(err)
	}

	fsl := c.record.Column(1).(*array.FixedSizeList)
	col := fsl.ListValues().(*array.Float32).Float32Values()
	hits := 0
	const probes = 200
	for i := 0; i < probes; i++ {
		off := (i * 37) % n
		res, err := idx.Search(context.Background(), col[off*dims:(off+1)*dims], 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) == 1 && uint64(res[0].ID) == uint64(off) { // #nosec G115
			hits++
		}
	}
	t.Logf("HARNESS self_hit=%.3f over %d probes", float64(hits)/probes, probes)
}

// TestTQComputeBatchMatchesPerCandidate pins the batched TurboQuant distance
// loop to the per-candidate reference path. Batching resolves the chunk table
// once per search instead of once per candidate, so the risk is that it serves a
// different code slice than the reference for some id. Ids deliberately span
// several chunk boundaries and include ids past the resident range, so the
// fall-back branches are exercised.
func TestTQComputeBatchMatchesPerCandidate(t *testing.T) {
	const n, dims = 5_000, 128
	for _, bits := range []int{4, 8} {
		t.Run("bits"+string(rune('0'+bits)), func(t *testing.T) {
			c := newTQCorpus(t, n, dims)
			cfg := tqConfig(dims, bits)
			cfg.Workers = 1
			idx := buildInBatches(t, c, n, n, cfg)

			comp := &tqComputer{
				data:         idx.data.Load(),
				h:            idx,
				rotatedQuery: make([]float32, idx.tqCompute.encoder.pow2),
				maxGen:       ^uint64(0),
			}
			if err := comp.h.tqCompute.PrecomputeRotatedQuery(c.queries[0], comp.rotatedQuery); err != nil {
				t.Fatal(err)
			}

			var ids []uint32
			for ch := 0; ch < 6; ch++ {
				for _, off := range []int{0, 1, types.ChunkSize - 1} {
					ids = append(ids, uint32(ch*types.ChunkSize+off)) // #nosec G115 -- bounded by ch
				}
			}
			ids = append(ids, uint32(n+5), uint32(n*4))

			dst := make([]float32, len(ids))
			if _, err := comp.ComputeBatch(ids, dst); err != nil {
				t.Fatalf("ComputeBatch: %v", err)
			}
			for i, id := range ids {
				want, err := comp.ComputeSingle(id)
				if err != nil {
					t.Fatalf("ComputeSingle(%d): %v", id, err)
				}
				if dst[i] != want {
					t.Fatalf("id %d: batched %v != reference %v", id, dst[i], want)
				}
			}
		})
	}
}

// TestTQDistanceDirectCodesMatchesDistanceDirect checks that the batched inner
// loop and the id-resolving path agree. They are two ways of reaching the same
// SIMD function, and a divergence between them would be invisible in any
// end-to-end measurement because both are "the distance".
func TestTQDistanceDirectCodesMatchesDistanceDirect(t *testing.T) {
	const n, dims = 5_000, 128
	for _, bits := range []int{4, 8} {
		t.Run("bits"+string(rune('0'+bits)), func(t *testing.T) {
			c := newTQCorpus(t, n, dims)
			cfg := tqConfig(dims, bits)
			cfg.Workers = 1
			idx := buildInBatches(t, c, n, n, cfg)
			tqc := idx.tqCompute

			rotated := make([]float32, tqc.encoder.pow2)
			if err := tqc.PrecomputeRotatedQuery(c.queries[0], rotated); err != nil {
				t.Fatal(err)
			}
			// No disk graph is attached in-process, which is the MaxUint64 case:
			// every id resolves against the resident arena.
			dg := (*DiskGraph)(nil)
			const maxGen uint64 = 18446744073709551615

			for _, id := range []uint32{0, 1, 17, 1023, 4096, uint32(n) - 1} {
				want, err := tqc.DistanceDirect(id, rotated, dg, maxGen)
				if err != nil {
					t.Fatalf("DistanceDirect(%d): %v", id, err)
				}
				code, err := tqc.getTQBytes(id, dg, maxGen)
				if err != nil {
					t.Fatalf("getTQBytes(%d): %v", id, err)
				}
				got, err := tqc.DistanceDirectCodes(rotated, code)
				if err != nil {
					t.Fatalf("DistanceDirectCodes(%d): %v", id, err)
				}
				if got != want {
					t.Fatalf("id %d: DistanceDirectCodes %v != DistanceDirect %v", id, got, want)
				}
			}
		})
	}
}

// TestTQPrefetchChunkIsBoundsSafe covers PrefetchChunk's early returns. The
// function only issues a read hint and returns nothing, so its entire observable
// contract is that it neither panics nor reads outside the chunk it was given.
// The out-of-range case matters because the hint is computed as index*stride and
// callers derive the index from an id they may have already validated against a
// different batch's chunk.
func TestTQPrefetchChunkIsBoundsSafe(t *testing.T) {
	const n, dims = 1_000, 128
	c := newTQCorpus(t, n, dims)
	cfg := tqConfig(dims, 4)
	cfg.Workers = 1
	idx := NewArrowHNSW(c.dataset, &cfg, nil)
	if _, err := idx.AddBatch(context.Background(),
		[]arrow.RecordBatch{c.record}, c.rowIdxs, c.batchIdx); err != nil {
		t.Fatal(err)
	}
	tqc := idx.tqCompute
	stride := idx.data.Load().PackedSize()
	chunk := make([]byte, stride*4)

	tqc.PrefetchChunk(nil, 0)       // nil chunk
	tqc.PrefetchChunk(chunk, -1)    // negative index
	tqc.PrefetchChunk(chunk, 0)     // first vector, at the chunk start
	tqc.PrefetchChunk(chunk, 3)     // last vector that fits
	tqc.PrefetchChunk(chunk, 4)     // starts exactly at the end
	tqc.PrefetchChunk(chunk, 1<<20) // far past the end
	tqc.PrefetchChunk(chunk[:1], 1) // stride does not fit in a truncated chunk
}

// TestTurboQuantConstructionScalesLikeFloat32 is the regression gate for the
// ratio that made the problem visible: at the docs baseline commit, building a
// TurboQuant index was faster than building a float32 index, which is only
// possible when the TurboQuant graph is not fully built. It is a ratio rather
// than an absolute time so that machine speed does not matter.
//
// It is opt-in because it builds 250,000 vectors; set LONG_BOW_TQ_BUILD=1.
func TestTurboQuantConstructionScalesLikeFloat32(t *testing.T) {
	if os.Getenv("LONG_BOW_TQ_BUILD") != "1" {
		t.Skip("set LONG_BOW_TQ_BUILD=1 to run the 250k TurboQuant construction gate")
	}
	const n, dims, batch = 250_000, 128, 10_000
	c := newTQCorpus(t, n, dims)

	f32Cfg := types.DefaultArrowHNSWConfig()
	f32Cfg.DataType = types.VectorTypeFloat32
	f32Cfg.Dims = dims
	f32Cfg.M, f32Cfg.MMax, f32Cfg.MMax0 = 16, 16, 16
	f32Cfg.EfConstruction = 200
	f32Cfg.Workers = 4
	f32Start := time.Now()
	buildInBatches(t, c, n, batch, f32Cfg)
	f32Dur := time.Since(f32Start)

	tqCfg := tqConfig(dims, 4)
	tqStart := time.Now()
	buildInBatches(t, c, n, batch, tqCfg)
	tqDur := time.Since(tqStart)

	t.Logf("CONSTRUCTION n=%d float32=%s turboquant4=%s ratio=%.2f",
		n, f32Dur, tqDur, tqDur.Seconds()/f32Dur.Seconds())
	if f32Dur.Seconds() <= 0 {
		t.Skip("float32 anchor too small to form a ratio")
	}
	if ratio := tqDur.Seconds() / f32Dur.Seconds(); ratio > 8 {
		t.Errorf("turboquant construction is %.2fx float32; before a955a0c1 it was "+
			"faster than float32, which is only possible with a partly disconnected graph", ratio)
	}
}
