package store

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func buildUintScaleIndex(tb testing.TB, n int) (*ArrowHNSW, []uint16) {
	tb.Helper()
	mem := memory.NewGoAllocator()
	dims := 8
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Uint16)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	idB := builder.Field(0).(*array.Int64Builder)
	vecB := builder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Uint16Builder)

	rng := rand.New(rand.NewSource(42))
	idB.Reserve(n)
	vecB.Reserve(n)
	valB.Reserve(n * dims)
	for i := 0; i < n; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		for j := 0; j < dims; j++ {
			valB.Append(uint16(rng.Intn(65536)))
		}
	}
	rec := builder.NewRecordBatch()
	defer rec.Release()
	rec.Retain()

	ds := NewDataset("uint_scale", schema)
	ds.Records.UpdateInPlace(append(append([]arrow.RecordBatch{}, ds.Records.Read()...), rec))

	cfg := DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeUint16
	cfg.Dims = dims
	cfg.M = 16
	cfg.MMax = 16
	cfg.MMax0 = 32
	cfg.EfConstruction = 100
	cfg.Workers = 4
	idx := NewArrowHNSW(ds, &cfg, nil)
	_ = idx.SetDimension(dims)

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

	q := make([]uint16, dims)
	for j := range q {
		q[j] = uint16(rng.Intn(65536))
	}
	return idx, q
}

// TestUintDispatchScalability runs the test uint dispatch scalability operation.
func TestUintDispatchScalability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping uint scalability test in short mode")
	}
	const n = 100000
	idx, q := buildUintScaleIndex(t, n)
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
	t.Logf("UintDispatchScalability: n=%d qps=%.1f", n, best)
	if best < 20 {
		t.Fatalf("uint search QPS %.1f below threshold 20", best)
	}
}
