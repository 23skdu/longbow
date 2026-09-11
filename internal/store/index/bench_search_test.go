package index

import (
	"context"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkInt8Search_50k(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 128
	n := 50_000

	rec := makeInt8TestRecordBatch(mem, dims, n)
	defer rec.Release()
	rec.Retain()

	ds := NewMockDataset("bench_int8", rec.Schema())
	ds.Records = append(ds.Records, rec)

	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeInt8
	config.Dims = dims
	idx := NewArrowHNSW(ds, &config, nil)

	ctx := context.Background()
	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)
	for k := 0; k < n; k++ {
		rowIdxs[k] = k
		batchIdxs[k] = 0
	}
	_, err := idx.AddBatch(ctx, []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	if err != nil {
		b.Fatal(err)
	}

	r := rand.New(rand.NewSource(99))
	query := make([]int8, dims)
	for j := range query {
		query[j] = int8(r.Intn(256) - 128)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := idx.Search(ctx, query, 10, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInt8Search_50k_Parallel(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 128
	n := 50_000

	rec := makeInt8TestRecordBatch(mem, dims, n)
	defer rec.Release()
	rec.Retain()

	ds := NewMockDataset("bench_int8_para", rec.Schema())
	ds.Records = append(ds.Records, rec)

	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeInt8
	config.Dims = dims
	idx := NewArrowHNSW(ds, &config, nil)

	ctx := context.Background()
	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)
	for k := 0; k < n; k++ {
		rowIdxs[k] = k
		batchIdxs[k] = 0
	}
	_, err := idx.AddBatch(ctx, []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	if err != nil {
		b.Fatal(err)
	}

	r := rand.New(rand.NewSource(99))
	query := make([]int8, dims)
	for j := range query {
		query[j] = int8(r.Intn(256) - 128)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := idx.Search(ctx, query, 10, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
