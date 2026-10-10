package index

// Coverage for the bulk-insert chain link on unordered input.
//
// TestBulkInsert_CollinearGraphStaysConnected in
// arrow_hnsw_bulk_connectivity_test.go covers collinear data, where the
// insertion-order predecessor genuinely is the nearest neighbour. This file
// covers the opposite geometry, and it exists because of a failed attempt to
// "fix" the chain link (docs/roadmap.md section 8.3.1).
//
// The attempted fix was to add the predecessor edge only when it was near, gated
// on the node's own candidate median. It looked correct - on shuffled input it
// removes an arbitrary long-range edge from every layer-0 node - and it was
// reverted because it broke graph connectivity badly:
//
//   reachable at layer 0        unconditional   proximity-gated
//   turboquant 4-bit, n=20000        98.2%            81.8%
//   turboquant 8-bit, n=20000        97.9%            86.8%
//
// On unordered input the predecessor is usually far, so the gate rejects nearly
// every chain edge, and the result shows the edge is not the rare fallback the
// roadmap assumed. It is where most inbound edges come from. Any replacement has
// to guarantee inbound edges first (roadmap R26); gating the edge before that
// strands nodes.
//
// The tests here therefore pin the behaviour that is actually shipped, so that a
// future attempt at the gate fails here rather than in production.

import (
	"context"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// shuffledCorpus builds n vectors in a shuffled order, which is what an embedding
// load looks like once rows arrive out of order. Insertion order then carries no
// information about distance, so the insertion-order predecessor is an arbitrary
// point rather than a near neighbour.
//
// The returned vectors are in *insertion* order, so that node id i corresponds to
// byID[i] - AddBatch assigns ids in record order.
func shuffledCorpus(t *testing.T, n, dims int, seed int64) (arrow.RecordBatch, [][]float32) {
	t.Helper()

	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic test data
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = make([]float32, dims)
		for j := range vecs[i] {
			vecs[i][j] = rng.Float32()
		}
	}
	// Shuffle insertion order independently of the vectors themselves.
	order := rng.Perm(n)

	builder := array.NewRecordBuilder(memory.NewGoAllocator(), arrow.NewSchema(
		[]arrow.Field{{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)}}, nil,
	))
	defer builder.Release()
	listB := builder.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)
	byID := make([][]float32, n)
	for pos, i := range order {
		listB.Append(true)
		for _, v := range vecs[i] {
			valB.Append(v)
		}
		byID[pos] = vecs[i]
	}
	return builder.NewRecordBatch(), byID
}

// randomOrderedCorpus builds n vectors in natural order without permuting. The
// vectors are still mutually unrelated, so insertion order still carries no
// distance information; this is the geometry the TurboQuant graph-quality gate
// uses, and it is where the proximity gate degraded connectivity.
func randomOrderedCorpus(t *testing.T, n, dims int, seed int64) (arrow.RecordBatch, [][]float32) {
	t.Helper()

	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic test data
	byID := make([][]float32, n)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), arrow.NewSchema(
		[]arrow.Field{{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)}}, nil,
	))
	defer builder.Release()
	listB := builder.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)
	for i := 0; i < n; i++ {
		byID[i] = make([]float32, dims)
		for j := range byID[i] {
			byID[i][j] = rng.Float32()
		}
		listB.Append(true)
		for _, v := range byID[i] {
			valB.Append(v)
		}
	}
	return builder.NewRecordBatch(), byID
}

func chainLinkTestConfig(dt types.VectorDataType, dims int) types.ArrowHNSWConfig {
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
		cfg.TurboQuantBits = 4
	}
	return cfg
}

// buildThroughBulk ingests in batches of batch rows, which is the shape the store
// ingests at: each flush is linked into the graph built so far, so the chain edge
// is what keeps a fresh node reachable.
func buildThroughBulk(t *testing.T, rec arrow.RecordBatch, n, batch int, cfg types.ArrowHNSWConfig) *ArrowHNSW {
	t.Helper()
	ds := NewMockDataset("chainlink", rec.Schema())
	ds.Records = append(ds.Records, rec)
	rowIdxs := make([]int, n)
	batchIdx := make([]int, n)
	for i := 0; i < n; i++ {
		rowIdxs[i] = i
		batchIdx[i] = 0
	}
	idx := NewArrowHNSW(ds, &cfg, nil)
	for off := 0; off < n; off += batch {
		end := off + batch
		if end > n {
			end = n
		}
		if _, err := idx.AddBatch(context.Background(),
			[]arrow.RecordBatch{rec}, rowIdxs[off:end], batchIdx[off:end]); err != nil {
			t.Fatal(err)
		}
	}
	return idx
}

func layer0Reachable(t *testing.T, idx *ArrowHNSW) int {
	t.Helper()
	seen := map[uint32]bool{idx.GetEntryPoint(): true}
	queue := []uint32{idx.GetEntryPoint()}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		nb, err := idx.GetLayerNeighbors(cur, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range nb {
			if !seen[v] {
				seen[v] = true
				queue = append(queue, v)
			}
		}
	}
	return len(seen)
}

func bruteForceRecall(t *testing.T, idx *ArrowHNSW, corpus [][]float32, probes [][]float32, k int) float64 {
	t.Helper()
	dist := make([]float64, len(corpus))
	order := make([]int, len(corpus))
	hits, total := 0, 0
	for _, q := range probes {
		for i, v := range corpus {
			var d float64
			for j := range q {
				diff := float64(q[j]) - float64(v[j])
				d += diff * diff
			}
			dist[i] = d
			order[i] = i
		}
		sort.Slice(order, func(a, b int) bool { return dist[order[a]] < dist[order[b]] })

		res, err := idx.Search(context.Background(), q, k, nil)
		if err != nil {
			t.Fatal(err)
		}
		got := make(map[uint64]bool, len(res))
		for _, r := range res {
			got[uint64(r.ID)] = true // #nosec G115 -- ids are node indices
		}
		for _, o := range order[:k] {
			if got[uint64(o)] { // #nosec G115
				hits++
			}
			total++
		}
	}
	return float64(hits) / float64(total)
}

func probeQueries(corpus [][]float32, count int, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic
	out := make([][]float32, count)
	for i := range out {
		out[i] = make([]float32, len(corpus[0]))
		for j := range out[i] {
			out[i][j] = rng.Float32()
		}
	}
	return out
}

func seedFromEnv(def int64) int64 {
	v := os.Getenv("CHAINLINK_SEED")
	if v == "" {
		return def
	}
	parsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return parsed
}

// TestBulkInsert_UnorderedCorpusStaysReachable is the invariant the chain edge
// actually has to hold on input whose insertion order carries no distance
// information. Both geometries are covered because the failed proximity gate
// regressed on the natural-order one more than on the shuffled one.
func TestBulkInsert_UnorderedCorpusStaysReachable(t *testing.T) {
	n, dims, batch := 20_000, 128, 10_000
	if testing.Short() {
		n, batch = 2_000, 1_000
	}

	corpora := map[string]func(*testing.T) (arrow.RecordBatch, [][]float32){
		"shuffled": func(t *testing.T) (arrow.RecordBatch, [][]float32) {
			return shuffledCorpus(t, n, dims, seedFromEnv(99))
		},
		"naturalorder": func(t *testing.T) (arrow.RecordBatch, [][]float32) {
			return randomOrderedCorpus(t, n, dims, seedFromEnv(99))
		},
	}

	for name, build := range corpora {
		t.Run(name, func(t *testing.T) {
			rec, _ := build(t)
			defer rec.Release()

			idx := buildThroughBulk(t, rec, n, batch, chainLinkTestConfig(types.VectorTypeFloat32, dims))
			reach := layer0Reachable(t, idx)
			t.Logf("UNORDERED %s n=%d reachable=%d/%d (%.1f%%)", name, n, reach, n,
				100*float64(reach)/float64(n))

			if reach < n*99/100 {
				t.Errorf("only %d of %d nodes reachable at layer 0 on %s input; "+
					"the chain edge is load-bearing for reachability here and must not "+
					"be gated on proximity", reach, n, name)
			}
		})
	}
}

// TestBulkInsert_UnorderedCorpusRecallFloored records absolute recall on
// unordered input. It is a floor, not a target: the in-process harness returns a
// corpus vector as its own nearest neighbour only about 69% of the time
// (TestDenseRecallHarnessSanity), and uniform random 128-d vectors are close to a
// worst case for graph search, so the absolute number is low by construction.
func TestBulkInsert_UnorderedCorpusRecallFloored(t *testing.T) {
	n, dims, k, probes, batch := 20_000, 128, 10, 20, 10_000
	if testing.Short() {
		n, batch = 2_000, 1_000
	}

	rec, corpus := shuffledCorpus(t, n, dims, seedFromEnv(99))
	defer rec.Release()

	idx := buildThroughBulk(t, rec, n, batch, chainLinkTestConfig(types.VectorTypeFloat32, dims))
	recall := bruteForceRecall(t, idx, corpus, probeQueries(corpus, probes, 7), k)
	t.Logf("UNORDERED_RECALL shuffled n=%d recall@%d=%.4f", n, k, recall)

	const floor = 0.05
	if recall < floor {
		t.Errorf("recall@%d %.4f on shuffled input is below the %.2f floor", k, recall, floor)
	}
}
