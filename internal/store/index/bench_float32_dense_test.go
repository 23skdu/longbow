package index

import (
	"context"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// float32Corpus is a deterministic in-memory float32 corpus plus the row/batch
// index vectors ArrowHNSW.AddBatch needs, and a fixed query set.
type float32Corpus struct {
	dataset  *MockDataset
	record   arrow.RecordBatch
	rowIdxs  []int
	batchIdx []int
	queries  [][]float32
}

// newFloat32Corpus builds an n x dims uniform-random float32 corpus with a
// fixed seed, together with a fixed query set drawn from a second seed.
func newFloat32Corpus(tb testing.TB, n, dims int) *float32Corpus {
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
		idB.Append(int64(i)) // #nosec G115 -- i < n, an in-memory corpus index
		vecB.Append(true)
		for j := 0; j < dims; j++ {
			valB.Append(rng.Float32())
		}
	}
	rec := b.NewRecordBatch()
	rec.Retain()
	tb.Cleanup(rec.Release)

	ds := NewMockDataset("bench_f32_dense", schema)
	ds.Records = append(ds.Records, rec)

	rowIdxs := make([]int, n)
	batchIdx := make([]int, n)
	for k := 0; k < n; k++ {
		rowIdxs[k] = k
		batchIdx[k] = 0
	}

	qrng := rand.New(rand.NewSource(7)) // #nosec G404 -- deterministic benchmark data
	queries := make([][]float32, 100)
	for i := range queries {
		queries[i] = make([]float32, dims)
		for j := range queries[i] {
			queries[i][j] = qrng.Float32()
		}
	}
	return &float32Corpus{dataset: ds, record: rec, rowIdxs: rowIdxs, batchIdx: batchIdx, queries: queries}
}

// buildFloat32DenseCorpus indexes an n x dims float32 corpus through ArrowHNSW
// with the scale-adaptive parameters scripts/unified_benchmark.py applies at
// n >= 50000 (M0=16, MMax0=16, efConstruction=200). Workers is pinned to 1 so
// the bulk-insert graph is deterministic, which makes ns/op differences between
// revisions attributable to the search path rather than to graph variance.
func buildFloat32DenseCorpus(tb testing.TB, n, dims, mmax0 int) (*ArrowHNSW, [][]float32) {
	tb.Helper()
	c := newFloat32Corpus(tb, n, dims)

	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeFloat32
	cfg.Dims = dims
	cfg.M = 16
	cfg.MMax = 16
	cfg.MMax0 = mmax0
	cfg.EfConstruction = 200
	cfg.Workers = 1
	idx := NewArrowHNSW(c.dataset, &cfg, nil)

	if _, err := idx.AddBatch(context.Background(),
		[]arrow.RecordBatch{c.record}, c.rowIdxs, c.batchIdx); err != nil {
		tb.Fatal(err)
	}
	return idx, c.queries
}

// BenchmarkDenseSearch_Float32_50k is the in-process counterpart of the
// float32 dense point in the unified benchmark matrix, for use as a bisection
// harness: the corpus, the HNSW parameters and the query set are all pinned, so
// a change in ns/op is a change in the search path and not in the data.
//
// Note that this exercises ArrowHNSW.Search directly, not the gRPC/Arrow Flight
// request path, so its absolute ns/op is not comparable with the QPS figures in
// docs/performance.md. Use it for A/B and bisection only.
func BenchmarkDenseSearch_Float32_50k(b *testing.B) {
	idx, queries := buildFloat32DenseCorpus(b, 50_000, 128, 16)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		if _, err := idx.Search(ctx, queries[0], 10, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDenseSearch_Float32_50k_M32 raises the layer-0 degree budget from the
// harness value (MMax0=16) to 32. The bulk-insert chain-link fix reserves two of
// every node's layer-0 slots (chainLinksPerNode), so at MMax0=16 only 14 remain
// for distance-based neighbour selection; comparing the two isolates that
// reservation from the chain edge itself.
func BenchmarkDenseSearch_Float32_50k_M32(b *testing.B) {
	idx, queries := buildFloat32DenseCorpus(b, 50_000, 128, 32)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		if _, err := idx.Search(ctx, queries[0], 10, nil); err != nil {
			b.Fatal(err)
		}
	}
}
