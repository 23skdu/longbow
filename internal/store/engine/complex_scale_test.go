package engine

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// buildComplexScaleIndex builds a HNSW index over n complex128 vectors
// (logical dim 8, physical dim 16 floats).
func buildComplexScaleIndex(tb testing.TB, n int) (*ArrowHNSW, []float32) {
	tb.Helper()
	mem := memory.NewGoAllocator()
	logicalDim := 8
	physicalDim := logicalDim * 2
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(physicalDim), arrow.PrimitiveTypes.Float64)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	idB := builder.Field(0).(*array.Int64Builder)
	vecB := builder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float64Builder)

	rng := rand.New(rand.NewSource(42))
	idB.Reserve(n)
	vecB.Reserve(n)
	valB.Reserve(n * physicalDim)
	for i := 0; i < n; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		for j := 0; j < physicalDim; j++ {
			valB.Append(rng.Float64())
		}
	}
	rec := builder.NewRecordBatch()
	defer rec.Release()
	rec.Retain()

	ds := NewDataset("complex_scale", schema)
	ds.Records.UpdateInPlace(append(append([]arrow.RecordBatch{}, ds.Records.Read()...), rec))
	tb.Cleanup(ds.Close)

	cfg := DefaultArrowHNSWConfig()
	cfg.DataType = VectorTypeComplex128
	cfg.Dims = logicalDim
	cfg.M = 16
	cfg.MMax = 16
	cfg.MMax0 = 32
	cfg.EfConstruction = 100
	cfg.Workers = 4
	idx := NewArrowHNSW(ds, &cfg, nil)
	_ = idx.SetDimension(logicalDim)

	ctx := context.Background()
	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)
	for k := 0; k < n; k++ {
		rowIdxs[k] = k
		batchIdxs[k] = 0
	}
	for off := 0; off < n; off += 10000 {
		end := off + 10000
		if end > n {
			end = n
		}
		if _, err := idx.AddBatch(ctx, []arrow.RecordBatch{rec}, rowIdxs[off:end], batchIdxs[off:end]); err != nil {
			tb.Fatalf("AddBatch: %v", err)
		}
	}

	q := make([]float32, physicalDim)
	for j := range q {
		q[j] = rng.Float32()
	}
	return idx, q
}

// TestComplexScalability guards P5-6 at 100k.
func TestComplexScalability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping complex scalability test in short mode")
	}
	const n = 100000
	idx, q := buildComplexScaleIndex(t, n)
	defer func() { _ = idx.Close() }()

	ctx := context.Background()
	if _, err := idx.SearchVectors(ctx, q, 10, nil, SearchOptions{}); err != nil {
		t.Fatal(err)
	}

	const queries = 50
	const rounds = 3
	best := 0.0
	for r := 0; r < rounds; r++ {
		start := time.Now()
		for i := 0; i < queries; i++ {
			res, err := idx.SearchVectors(ctx, q, 10, nil, SearchOptions{})
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
	t.Logf("ComplexScalability: n=%d qps=%.1f", n, best)
	if best < 20 {
		t.Fatalf("complex search QPS %.1f below threshold 20", best)
	}
}

// BenchmarkComplexSearchScale measures the complex HNSW path at 100k.
func BenchmarkComplexSearchScale(b *testing.B) {
	const n = 100000
	idx, q := buildComplexScaleIndex(b, n)
	defer func() { _ = idx.Close() }()

	b.ReportAllocs()
	b.ResetTimer()
	ctx := context.Background()
	for b.Loop() {
		if _, err := idx.SearchVectors(ctx, q, 10, nil, SearchOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "qps")
	}
}
