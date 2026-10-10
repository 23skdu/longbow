package index

// A/B characterisation of the bulk insert path against sequential insertion.
//
// This exists because of a measured regression, not a suspected one. On
// uniform random 128-d vectors the bulk path links a layer 0 with mean degree
// of 1.9-5.5 while MMax0 is 16, and a 100k float32 build on that graph served
// dense search at 708 QPS against 3522 QPS for the same corpus built one insert
// at a time - a 5x search regression bought with a 4x faster index build.
//
// The cause is the diversity heuristic in selectNeighbors. It rejects a
// candidate when d(candidate, already-selected) < d(candidate, node), which is
// the standard HNSW rule, but on uniform random vectors every pairwise distance
// concentrates near the same value, so the rule fires on most candidates and the
// node keeps only the handful that survive. Sequential insertion does not hit
// this as hard because each node searches the growing graph and links against
// M, not M*2, and because the candidate lists it produces are closer.
//
// So the R8 gate has to key on navigability rather than on reachability alone.
// Reachability stayed at 80-100% on the broken graph: every node was reachable,
// it just took many more hops to get there. The measurements below are what the
// floor in BulkGraphQualityMinDegreeRatio is set from.

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
)

// layer0Stats walks layer 0 and reports mean degree and reachability, which are
// the two quantities the R8 gate compares.
func layer0Stats(t *testing.T, idx *ArrowHNSW) (meanDegree float64, reachable int) {
	t.Helper()
	ep := idx.GetEntryPoint()
	seen := map[uint32]bool{ep: true}
	queue := []uint32{ep}
	degreeSum := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		nb, err := idx.GetLayerNeighbors(cur, 0)
		if err != nil {
			t.Fatal(err)
		}
		degreeSum += len(nb)
		for _, v := range nb {
			if !seen[v] {
				seen[v] = true
				queue = append(queue, v)
			}
		}
	}
	if len(seen) > 0 {
		meanDegree = float64(degreeSum) / float64(len(seen))
	}
	return meanDegree, len(seen)
}

// buildSequential ingests one row per AddBatch call, which is the path the R8
// gate falls back to.
func buildSequential(t *testing.T, rec arrow.RecordBatch, n int, cfg types.ArrowHNSWConfig) *ArrowHNSW {
	t.Helper()
	ds := NewMockDataset("seqcmp", rec.Schema())
	ds.Records = append(ds.Records, rec)
	idx := NewArrowHNSW(ds, &cfg, nil)
	for i := 0; i < n; i++ {
		if _, err := idx.AddBatch(context.Background(),
			[]arrow.RecordBatch{rec}, []int{i}, []int{0}); err != nil {
			t.Fatal(err)
		}
	}
	return idx
}

// TestBulkInsert_GraphTopologyVsSequential records mean layer-0 degree and
// reachability for both insert paths on the same corpus, and holds the bulk path
// to the degree floor the R8 gate enforces.
func TestBulkInsert_GraphTopologyVsSequential(t *testing.T) {
	if testing.Short() {
		t.Skip("builds three indexes of 20k vectors")
	}
	const n, dims, batch = 20_000, 128, 10_000

	rec, corpus := shuffledCorpus(t, n, dims, seedFromEnv(99))
	defer rec.Release()
	cfg := chainLinkTestConfig(types.VectorTypeFloat32, dims)

	bulk := buildThroughBulk(t, rec, n, batch, cfg)
	bulkDeg, bulkReach := layer0Stats(t, bulk)
	bulkRecall := bruteForceRecall(t, bulk, corpus, probeQueries(corpus, 20, 7), 10)
	t.Logf("TOPOLOGY bulk       mean_degree=%.2f reachable=%d/%d recall@10=%.4f",
		bulkDeg, bulkReach, n, bulkRecall)

	seq := buildSequential(t, rec, n, cfg)
	seqDeg, seqReach := layer0Stats(t, seq)
	seqRecall := bruteForceRecall(t, seq, corpus, probeQueries(corpus, 20, 7), 10)
	t.Logf("TOPOLOGY sequential mean_degree=%.2f reachable=%d/%d recall@10=%.4f",
		seqDeg, seqReach, n, seqRecall)

	floor := float64(cfg.MMax0) * BulkGraphQualityMinDegreeRatio
	t.Logf("TOPOLOGY floor=%.2f (%.0f%% of MMax0=%d)", floor,
		BulkGraphQualityMinDegreeRatio*100, cfg.MMax0)

	// Reachability alone does not discriminate here - both paths leave the whole
	// corpus reachable - so this asserts the property that actually separates
	// them: enough edges per node for greedy descent to cross the graph without
	// visiting a large fraction of it.
	if bulkDeg < floor {
		t.Errorf("bulk layer-0 mean degree %.2f is below the %.2f floor "+
			"(MMax0=%d); the R8 gate will fall back to sequential insertion and "+
			"pay the index-time cost instead of shipping this graph",
			bulkDeg, floor, cfg.MMax0)
	}
}
