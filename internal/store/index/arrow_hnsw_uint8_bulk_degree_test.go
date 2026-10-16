package index

// Roadmap Item 29: uint8 connection-degree invariant under bulk indexing.
//
// The uint8 selectNeighbors branch fixed reachability (see
// arrow_hnsw_uint8_reachability_test.go), but reachability is the weaker of
// the two properties. A graph can reach 100% of its nodes and still be a bad
// graph: greedy descent crosses it in more hops, which is what the measured
// 708-vs-3522 QPS search regression on the float32 bulk path was.
//
// This is the degree half of that gate, and it is deliberately measured on the
// *bulk* path in the shape production ingests: four flushes of 1,000 rows, each
// one linked into the graph built so far. A single AddBatch of all 4,000 rows
// links once against a static corpus, which is the case the bulk linker is
// best at and says nothing about the repeated small-flush ingest the store
// actually performs.
//
// Floor: mean layer-0 degree >= 15.0 with MMax0=16. int8 and float32 hold
// 16.00 on this corpus; uint8 pruned to 1.3-6.3 before its diversity branch
// landed. Anything below 15.0 means the branch has been bypassed again, or
// that KeepPrunedConnections is no longer topping the selection up to M.

import (
	"math"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// uint8RecordBatch renders a float32 corpus into the uint8 Arrow column the
// bulk path ingests, using the same component quantizer buildQuantizedCorpus
// uses (scale 127, bias 128, clamped to [0,255]).
func uint8RecordBatch(tb testing.TB, corpus [][]float32) arrow.RecordBatch {
	tb.Helper()
	dims := len(corpus[0])
	b := array.NewRecordBuilder(memory.NewGoAllocator(), arrow.NewSchema(
		[]arrow.Field{{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Uint8)}}, nil))
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	vb := listB.ValueBuilder().(*array.Uint8Builder)
	scale, bias, lo, hi := quantizerFor(types.VectorTypeUint8)
	for _, v := range corpus {
		listB.Append(true)
		for _, x := range v {
			vb.Append(uint8(clampInt(int(math.Round(float64(x)*float64(scale)))+bias, lo, hi))) // #nosec G115 -- range checked by clampInt
		}
	}
	rec := b.NewRecordBatch()
	rec.Retain()
	tb.Cleanup(rec.Release)
	return rec
}

// uniformCorpus builds n uniform random vectors in [-1,1] - the distribution
// where 8-bit quantization loses the most neighbour ordering, and the one the
// reachability gate uses as its worst case.
func uniformCorpus(tb testing.TB, seed int64, n, dims int) [][]float32 {
	tb.Helper()
	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic
	out := make([][]float32, n)
	for i := range out {
		v := make([]float32, dims)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		out[i] = v
	}
	return out
}

// TestBulkInsert_Uint8MeanDegreeFloor holds the bulk linker to the degree
// invariant for uint8 across the corpus shapes the reachability gate covers.
func TestBulkInsert_Uint8MeanDegreeFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("builds three 4k uint8 indexes through the bulk path")
	}
	const dims, n, batch = 128, 4000, 1000

	corpora := []struct {
		name string
		corp [][]float32
	}{
		{"tight_clusters", clusterCorpus(t, 3, n, dims, 3, 0.04, 0.8, 1.4)},
		{"uniform", uniformCorpus(t, 9, n, dims)},
		{"twelve_clusters", clusterCorpus(t, 17, n, dims, 12, 0.12, 0.6, 1.3)},
	}

	cfg := chainLinkTestConfig(types.VectorTypeUint8, dims)
	for _, c := range corpora {
		c := c
		t.Run(c.name, func(t *testing.T) {
			rec := uint8RecordBatch(t, c.corp)
			idx := buildThroughBulk(t, rec, n, batch, cfg)
			deg, reach := layer0Stats(t, idx)
			t.Logf("UINT8_BULK_DEGREE %-16s mean_degree=%.2f reachable=%d/%d",
				c.name, deg, reach, n)

			if reach < n*95/100 {
				t.Errorf("uint8 bulk build on %q reaches only %d/%d nodes (%.1f%%), below the 95%% floor",
					c.name, reach, n, 100*float64(reach)/float64(n))
			}
			// The invariant under test: 15.0 of a 16.0 degree budget.
			const minMeanDegree = 15.0
			if deg < minMeanDegree {
				t.Errorf("uint8 bulk build on %q has mean layer-0 degree %.2f, below the %.2f floor "+
					"(MMax0=%d); this is the pruning regression Item 29 exists to catch",
					c.name, deg, minMeanDegree, cfg.MMax0)
			}
		})
	}
}
