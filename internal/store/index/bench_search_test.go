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
	for b.Loop() {
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

func makeUint8TestRecordBatch(mem memory.Allocator, dims, numRows int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vec", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Uint8)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idB := builder.Field(0).(*array.Int64Builder)
	vecB := builder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Uint8Builder)

	idB.Reserve(numRows)
	vecB.Reserve(numRows)
	valB.Reserve(numRows * dims)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numRows; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		for j := 0; j < dims; j++ {
			valB.Append(uint8(rng.Intn(256)))
		}
	}
	return builder.NewRecordBatch()
}

func BenchmarkUint8Search_50k(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 128
	n := 50_000

	rec := makeUint8TestRecordBatch(mem, dims, n)
	defer rec.Release()
	rec.Retain()

	ds := NewMockDataset("bench_uint8", rec.Schema())
	ds.Records = append(ds.Records, rec)

	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeUint8
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
	query := make([]uint8, dims)
	for j := range query {
		query[j] = uint8(r.Intn(256))
	}

	b.ResetTimer()
	for b.Loop() {
		_, err := idx.Search(ctx, query, 10, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInt8Search_50k_Float32Query(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 128
	n := 50_000

	rec := makeInt8TestRecordBatch(mem, dims, n)
	defer rec.Release()
	rec.Retain()

	ds := NewMockDataset("bench_int8_f32q", rec.Schema())
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
	query := make([]float32, dims)
	for j := range query {
		query[j] = float32(r.Intn(256) - 128)
	}

	b.ResetTimer()
	for b.Loop() {
		_, err := idx.Search(ctx, query, 10, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUint8Search_50k_Float32Query(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 128
	n := 50_000

	rec := makeUint8TestRecordBatch(mem, dims, n)
	defer rec.Release()
	rec.Retain()

	ds := NewMockDataset("bench_uint8_f32q", rec.Schema())
	ds.Records = append(ds.Records, rec)

	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeUint8
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
	query := make([]float32, dims)
	for j := range query {
		query[j] = float32(r.Intn(256))
	}

	b.ResetTimer()
	for b.Loop() {
		_, err := idx.Search(ctx, query, 10, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTurboQuantSearch_100k(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 128
	n := 100_000

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vec", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idB := builder.Field(0).(*array.Int64Builder)
	vecB := builder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	idB.Reserve(n)
	vecB.Reserve(n)
	valB.Reserve(n * dims)

	rng := rand.New(rand.NewSource(42))
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

	ds := NewMockDataset("bench_tq_100k", schema)
	ds.Records = append(ds.Records, rec)

	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeTQ
	config.Dims = dims
	config.TurboQuantEnabled = true
	config.TurboQuantBits = 4
	config.M = 16
	config.MMax = 16
	config.MMax0 = 16
	config.EfConstruction = 200
	config.Workers = 4
	idx := NewArrowHNSW(ds, &config, nil)

	ctx := context.Background()
	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)
	for k := 0; k < n; k++ {
		rowIdxs[k] = k
		batchIdxs[k] = 0
	}
	// Ingest in batches of 10,000 to match production Flight streaming ingest
	for off := 0; off < n; off += 10_000 {
		end := off + 10_000
		if end > n {
			end = n
		}
		_, err := idx.AddBatch(ctx, []arrow.RecordBatch{rec}, rowIdxs[off:end], batchIdxs[off:end])
		if err != nil {
			b.Fatal(err)
		}
	}

	qrng := rand.New(rand.NewSource(99))
	query := make([]float32, dims)
	for j := range query {
		query[j] = qrng.Float32()
	}

	b.ResetTimer()
	for b.Loop() {
		_, err := idx.Search(ctx, query, 10, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTurboQuantSearch_250k(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 128
	n := 250_000

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vec", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idB := builder.Field(0).(*array.Int64Builder)
	vecB := builder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	idB.Reserve(n)
	vecB.Reserve(n)
	valB.Reserve(n * dims)

	rng := rand.New(rand.NewSource(42))
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

	ds := NewMockDataset("bench_tq_250k", schema)
	ds.Records = append(ds.Records, rec)

	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeTQ
	config.Dims = dims
	config.TurboQuantEnabled = true
	config.TurboQuantBits = 4
	config.M = 16
	config.MMax = 16
	config.MMax0 = 16
	config.EfConstruction = 200
	config.Workers = 4
	idx := NewArrowHNSW(ds, &config, nil)

	ctx := context.Background()
	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)
	for k := 0; k < n; k++ {
		rowIdxs[k] = k
		batchIdxs[k] = 0
	}
	// Ingest in batches of 10,000
	for off := 0; off < n; off += 10_000 {
		end := off + 10_000
		if end > n {
			end = n
		}
		_, err := idx.AddBatch(ctx, []arrow.RecordBatch{rec}, rowIdxs[off:end], batchIdxs[off:end])
		if err != nil {
			b.Fatal(err)
		}
	}

	qrng := rand.New(rand.NewSource(99))
	query := make([]float32, dims)
	for j := range query {
		query[j] = qrng.Float32()
	}

	b.ResetTimer()
	for b.Loop() {
		_, err := idx.Search(ctx, query, 10, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}


