package index

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// buildFilteredScaleIndex builds a HNSW index over n small vectors and a bitmap
// admitting roughly one in four ids, matching a selective equality filter.
func buildFilteredScaleIndex(tb testing.TB, n int) (*ArrowHNSW, []float32, *roaring.Bitmap) {
	tb.Helper()
	mem := memory.NewGoAllocator()
	dims := 8
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	idB := builder.Field(0).(*array.Int64Builder)
	vecB := builder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	rng := rand.New(rand.NewSource(42))
	idB.Reserve(n)
	vecB.Reserve(n)
	valB.Reserve(n * dims)
	for i := 0; i < n; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		for j := 0; j < dims; j++ {
			valB.Append(rng.Float32())
		}
	}
	rec := builder.NewRecordBatch()
	defer rec.Release()
	rec.Retain()

	ds := NewMockDataset("filtered_scale", schema)
	ds.Records = append(ds.Records, rec)

	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeFloat32
	config.Dims = dims
	config.M = 16
	config.MMax = 16
	config.MMax0 = 32
	config.EfConstruction = 100
	config.Workers = 4
	idx := NewArrowHNSW(ds, &config, nil)

	ctx := context.Background()
	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)
	for k := 0; k < n; k++ {
		rowIdxs[k] = k
		batchIdxs[k] = 0
	}
	for off := 0; off < n; off += 10_000 {
		end := off + 10_000
		if end > n {
			end = n
		}
		if _, err := idx.AddBatch(ctx, []arrow.RecordBatch{rec}, rowIdxs[off:end], batchIdxs[off:end]); err != nil {
			tb.Fatalf("AddBatch: %v", err)
		}
	}

	q := make([]float32, dims)
	for j := range q {
		q[j] = rng.Float32()
	}

	bm := roaring.New()
	for i := 0; i < n; i += 4 {
		bm.Add(uint32(i))
	}
	return idx, q, bm
}

// TestFilteredSearchScalability guards P4-6/P10-6 at 100k.
func TestFilteredSearchScalability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping filtered scalability test in short mode")
	}
	const n = 100_000
	idx, q, bm := buildFilteredScaleIndex(t, n)
	defer func() { _ = idx.Close() }()

	ctx := context.Background()
	if _, err := idx.SearchVectorsWithBitmap(ctx, q, 10, bm, nil); err != nil {
		t.Fatal(err)
	}

	const queries = 50
	const rounds = 3
	best := 0.0
	for r := 0; r < rounds; r++ {
		start := time.Now()
		for i := 0; i < queries; i++ {
			res, err := idx.SearchVectorsWithBitmap(ctx, q, 10, bm, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(res) == 0 {
				t.Fatal("no results")
			}
		}
		elapsed := time.Since(start).Seconds()
		if elapsed > 0 {
			if qps := float64(queries) / elapsed; qps > best {
				best = qps
			}
		}
	}
	t.Logf("FilteredSearchScalability: n=%d qps=%.1f", n, best)
	if best < 100 {
		t.Fatalf("filtered search QPS %.1f below threshold 100", best)
	}
}

// BenchmarkFilteredSearchScale measures the filtered HNSW path at 100k.
func BenchmarkFilteredSearchScale(b *testing.B) {
	const n = 100_000
	idx, q, bm := buildFilteredScaleIndex(b, n)
	defer func() { _ = idx.Close() }()

	b.ReportAllocs()
	b.ResetTimer()
	ctx := context.Background()
	for b.Loop() {
		if _, err := idx.SearchVectorsWithBitmap(ctx, q, 10, bm, nil); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "qps")
	}
	_ = bm
}
