package index

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func buildScaleQualityIndex(t *testing.T, n, dims int) (*ArrowHNSW, [][]float32) {
	t.Helper()
	mem := arrowmemory.NewGoAllocator()
	vecB := arrowarray.NewFixedSizeListBuilder(mem, int32(dims), arrow.PrimitiveTypes.Float32)
	defer vecB.Release()
	valB := vecB.ValueBuilder().(*arrowarray.Float32Builder)

	rng := rand.New(rand.NewSource(202611183)) // #nosec G404 -- deterministic quality test
	vecs := make([][]float32, n)
	valB.Reserve(n * dims)
	for i := range n {
		vecB.Append(true)
		v := make([]float32, dims)
		for j := range dims {
			x := rng.Float32()*2 - 1
			v[j] = x
			valB.Append(x)
		}
		vecs[i] = v
	}
	vecArr := vecB.NewArray()
	defer vecArr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "vector", Type: vecArr.DataType()}}, nil)
	rec := arrowarray.NewRecordBatch(schema, []arrow.Array{vecArr}, int64(n))
	defer rec.Release()

	cfg := types.DefaultArrowHNSWConfig()
	cfg.Dims = dims
	cfg.M = 16
	cfg.MMax = 16
	cfg.MMax0 = 32
	cfg.EfConstruction = 100
	cfg.InitialCapacity = n + 64
	idx := NewArrowHNSW(nil, &cfg, nil)

	const batchSize = 2000
	ctx := context.Background()
	for off := 0; off < n; off += batchSize {
		end := off + batchSize
		if end > n {
			end = n
		}
		count := end - off
		rowIdxs := make([]int, count)
		batchIdxs := make([]int, count)
		for i := range count {
			rowIdxs[i] = off + i
		}
		_, err := idx.AddBatch(ctx, []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
		require.NoError(t, err)
	}
	return idx, vecs
}

// TestHNSWReachabilityAtScale validates that layer-0 reachability from the
// entry point stays >= 95% after multi-batch bulk insertion (roadmap D2;
// runs at 4k in -short, 500k in full mode).
func TestHNSWReachabilityAtScale(t *testing.T) {
	n := 500_000
	if testing.Short() {
		n = 4_000
	}
	idx, _ := buildScaleQualityIndex(t, n, 32)
	defer func() { _ = idx.Close() }()

	ep := idx.GetEntryPoint()
	visited := make([]bool, n)
	queue := make([]uint32, 0, n)
	visited[ep] = true
	queue = append(queue, ep)

	data := idx.data.Load()
	buf := make([]uint32, 0, 64)
	for head := 0; head < len(queue); head++ {
		cur := queue[head]
		nbs := idx.GetNeighborsCombinedManual(data, 0, cur, buf, math.MaxUint64)
		for _, nb := range nbs {
			if int(nb) < n && !visited[nb] {
				visited[nb] = true
				queue = append(queue, nb)
			}
		}
	}

	reachPct := float64(len(queue)) / float64(n) * 100.0
	t.Logf("Reachability: %d/%d (%.2f%%) from entry point %d", len(queue), n, reachPct, ep)
	require.GreaterOrEqual(t, reachPct, 95.0, "reachability must be >= 95%%")
}

// TestHNSWRecallAt10VsBruteForce validates recall@10 against exact L2 brute
// force over sampled queries (roadmap D5; runs at 4k in -short, 1M in full mode).
func TestHNSWRecallAt10VsBruteForce(t *testing.T) {
	n := 1_000_000
	numQueries := 25
	if testing.Short() {
		n = 4_000
		numQueries = 20
	}
	const (
		dims = 32
		k    = 10
	)
	idx, vecs := buildScaleQualityIndex(t, n, dims)
	defer func() { _ = idx.Close() }()

	type pair struct {
		id   uint32
		dist float32
	}
	rng := rand.New(rand.NewSource(9999)) // #nosec G404 -- deterministic queries
	totalHits := 0
	for range numQueries {
		q := make([]float32, dims)
		for j := range dims {
			q[j] = rng.Float32()*2 - 1
		}

		// Brute-force top-k L2 squared.
		all := make([]pair, n)
		for i, v := range vecs {
			var s float32
			for j := range dims {
				d := q[j] - v[j]
				s += d * d
			}
			all[i] = pair{id: uint32(i), dist: s} // #nosec G115
		}
		sort.Slice(all, func(i, j int) bool { return all[i].dist < all[j].dist })
		gt := make(map[uint32]struct{}, k)
		for i := range k {
			gt[all[i].id] = struct{}{}
		}

		res, err := idx.SearchVectors(context.Background(), q, k, nil, types.SearchOptions{Ef: 128})
		require.NoError(t, err)
		for _, r := range res {
			if _, ok := gt[uint32(r.ID)]; ok {
				totalHits++
			}
		}
	}

	recall := float64(totalHits) / float64(numQueries*k)
	t.Logf("Recall@%d over %d queries (n=%d): %.4f", k, numQueries, n, recall)
	require.GreaterOrEqual(t, recall, 0.85, "recall@10 vs brute force must be >= 0.85")
}
