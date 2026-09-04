package index

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	lbcore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestFloat64_PrefetchMultiLine(t *testing.T) {
	dims := 128
	numVecs := 200
	data := types.NewGraphData(
		10,
		dims,
		false,
		false,
		0,
		false,
		false,
		false,
		types.VectorTypeFloat64,
		false,
		false,
		false,
		8,
		"test_f64",
		nil,
		false,
	)

	// Populate vectors
	for i := 0; i < numVecs; i++ {
		vec := make([]float64, dims)
		for d := 0; d < dims; d++ {
			vec[d] = float64(i*dims + d)
		}
		data.SetVector(uint32(i), vec)
	}

	q := make([]float64, dims)
	for d := 0; d < dims; d++ {
		q[d] = float64(d)
	}

	h := &ArrowHNSW{
		distFuncF64: func(a, b []float64) (float32, error) {
			var sum float64
			for i := range a {
				diff := a[i] - b[i]
				sum += diff * diff
			}
			return float32(math.Sqrt(sum)), nil
		},
	}

	comp := &float64Computer{
		data:   data,
		q:      q,
		dims:   dims,
		h:      h,
		maxGen: math.MaxUint64,
	}

	// Test prefetching across valid IDs
	for i := 0; i < numVecs; i++ {
		comp.Prefetch(uint32(i))
	}

	// Test prefetching with specific maxGen
	compGen := &float64Computer{
		data:   data,
		q:      q,
		dims:   dims,
		h:      h,
		maxGen: 10,
	}
	for i := 0; i < numVecs; i++ {
		compGen.Prefetch(uint32(i))
	}

	// Out of bounds prefetch should be safe
	comp.Prefetch(99999)
}

func TestInt64_Uint64_PrefetchMultiLine(t *testing.T) {
	dims := 128
	numVecs := 100

	dataI64 := types.NewGraphData(
		10, dims, false, false, 0, false, false, false,
		types.VectorTypeInt64, false, false, false, 8, "test_i64", nil, false,
	)
	for i := 0; i < numVecs; i++ {
		vec := make([]int64, dims)
		for d := 0; d < dims; d++ {
			vec[d] = int64(i*dims + d)
		}
		dataI64.SetVector(uint32(i), vec)
	}

	compI64 := &int64Computer{
		data:   dataI64,
		q:      make([]int64, dims),
		dims:   dims,
		maxGen: math.MaxUint64,
	}
	for i := 0; i < numVecs; i++ {
		compI64.Prefetch(uint32(i))
	}
	compI64.Prefetch(88888)

	dataU64 := types.NewGraphData(
		10, dims, false, false, 0, false, false, false,
		types.VectorTypeUint64, false, false, false, 8, "test_u64", nil, false,
	)
	for i := 0; i < numVecs; i++ {
		vec := make([]uint64, dims)
		for d := 0; d < dims; d++ {
			vec[d] = uint64(i*dims + d)
		}
		dataU64.SetVector(uint32(i), vec)
	}

	compU64 := &uint64Computer{
		data:   dataU64,
		q:      make([]uint64, dims),
		dims:   dims,
		maxGen: math.MaxUint64,
	}
	for i := 0; i < numVecs; i++ {
		compU64.Prefetch(uint32(i))
	}
	compU64.Prefetch(88888)
}

func TestFloat64_ComputeBatchCacheBlocked(t *testing.T) {
	dims := 128
	numVecs := 250 // > 64 to exercise multiple blocks
	data := types.NewGraphData(
		10, dims, false, false, 0, false, false, false,
		types.VectorTypeFloat64, false, false, false, 8, "test_f64_batch", nil, false,
	)

	for i := 0; i < numVecs; i++ {
		vec := make([]float64, dims)
		for d := 0; d < dims; d++ {
			vec[d] = float64(i + d)
		}
		data.SetVector(uint32(i), vec)
	}

	q := make([]float64, dims)
	for d := 0; d < dims; d++ {
		q[d] = 1.0
	}

	h := &ArrowHNSW{
		distFuncF64: func(a, b []float64) (float32, error) {
			var sum float64
			for i := range a {
				diff := a[i] - b[i]
				sum += diff * diff
			}
			return float32(math.Sqrt(sum)), nil
		},
	}

	comp := &float64Computer{
		data:   data,
		q:      q,
		dims:   dims,
		h:      h,
		maxGen: math.MaxUint64,
	}

	ids := make([]uint32, numVecs)
	for i := range ids {
		ids[i] = uint32(i)
	}

	batchDists, err := comp.ComputeBatch(ids, nil)
	if err != nil {
		t.Fatalf("ComputeBatch failed: %v", err)
	}

	if len(batchDists) != numVecs {
		t.Fatalf("Expected %d results, got %d", numVecs, len(batchDists))
	}

	// Verify each result against ComputeSingle
	for i, id := range ids {
		singleDist, err := comp.ComputeSingle(id)
		if err != nil {
			t.Fatalf("ComputeSingle(%d) failed: %v", id, err)
		}
		if math.Abs(float64(batchDists[i]-singleDist)) > 1e-5 {
			t.Errorf("Vector %d: batch dist %f != single dist %f", id, batchDists[i], singleDist)
		}
	}
}

func TestFloat64_SearchLayerAndNavigation(t *testing.T) {
	pool := memory.NewGoAllocator()
	dims := 128
	numVecs := 500

	// Build float64 Arrow RecordBatch
	b := array.NewRecordBuilder(pool, arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float64)},
	}, nil))
	defer b.Release()

	fsb := b.Field(0).(*array.FixedSizeListBuilder)
	vb := fsb.ValueBuilder().(*array.Float64Builder)

	rng := rand.New(rand.NewSource(42))
	allVecs := make([][]float64, numVecs)
	for i := 0; i < numVecs; i++ {
		fsb.Append(true)
		vec := make([]float64, dims)
		for d := 0; d < dims; d++ {
			vec[d] = rng.Float64()
			vb.Append(vec[d])
		}
		allVecs[i] = vec
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	ds := &MockDataset{
		Name:    "test_f64_search",
		Records: []arrow.RecordBatch{rec},
	}

	cfg := DefaultArrowHNSWConfig()
	cfg.Dims = dims
	cfg.DataType = types.VectorTypeFloat64
	cfg.M = 16
	cfg.MMax = 32
	cfg.EfConstruction = 64
	cfg.EfSearch = 64
	cfg.Metric = lbcore.MetricEuclidean

	hnsw := NewArrowHNSW(ds, &cfg, nil)
	defer hnsw.Close()

	rowIdxs := make([]int, numVecs)
	batchIdxs := make([]int, numVecs)
	for i := 0; i < numVecs; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}

	// Ingest vectors
	ids, err := hnsw.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	if err != nil {
		t.Fatalf("AddBatch failed: %v", err)
	}
	if len(ids) != numVecs {
		t.Fatalf("Expected %d ids, got %d", numVecs, len(ids))
	}

	// Search using vector 0
	query := allVecs[0]
	res, err := hnsw.SearchVectorsWithBitmap(context.Background(), query, 5, nil, types.SearchOptions{})
	if err != nil {
		t.Fatalf("SearchVectorsWithBitmap failed: %v", err)
	}

	if len(res) == 0 {
		t.Fatalf("Expected results, got 0")
	}

	// Top result should be vector 0 with distance near 0
	if res[0].ID != 0 {
		t.Errorf("Expected top result ID 0, got %d (dist: %f)", res[0].ID, res[0].Distance)
	}
	if res[0].Distance > 1e-4 {
		t.Errorf("Expected top result distance ~0, got %f", res[0].Distance)
	}
}

func BenchmarkFloat64_ComputeBatch_CacheBlocked(b *testing.B) {
	dims := 128
	batchSizes := []int{16, 64, 128, 256}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", size), func(b *testing.B) {
			data := types.NewGraphData(
				10, dims, false, false, 0, false, false, false,
				types.VectorTypeFloat64, false, false, false, 8, "bench_f64", nil, false,
			)
			for i := 0; i < size; i++ {
				vec := make([]float64, dims)
				for d := 0; d < dims; d++ {
					vec[d] = float64(i + d)
				}
				data.SetVector(uint32(i), vec)
			}

			q := make([]float64, dims)
			for d := 0; d < dims; d++ {
				q[d] = float64(d)
			}

			h := &ArrowHNSW{
				distFuncF64: func(a, b []float64) (float32, error) {
					var sum float64
					for i := range a {
						diff := a[i] - b[i]
						sum += diff * diff
					}
					return float32(math.Sqrt(sum)), nil
				},
			}

			comp := &float64Computer{
				data:   data,
				q:      q,
				dims:   dims,
				h:      h,
				maxGen: math.MaxUint64,
			}

			ids := make([]uint32, size)
			for i := range ids {
				ids[i] = uint32(i)
			}
			dst := make([]float32, size)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = comp.ComputeBatch(ids, dst)
			}
		})
	}
}

func BenchmarkFloat64_HNSWSearch(b *testing.B) {
	pool := memory.NewGoAllocator()
	dims := 128
	numVecs := 5000

	bldr := array.NewRecordBuilder(pool, arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float64)},
	}, nil))
	defer bldr.Release()

	fsb := bldr.Field(0).(*array.FixedSizeListBuilder)
	vb := fsb.ValueBuilder().(*array.Float64Builder)

	rng := rand.New(rand.NewSource(42))
	allVecs := make([][]float64, numVecs)
	for i := 0; i < numVecs; i++ {
		fsb.Append(true)
		vec := make([]float64, dims)
		for d := 0; d < dims; d++ {
			vec[d] = rng.Float64()
			vb.Append(vec[d])
		}
		allVecs[i] = vec
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	ds := &MockDataset{
		Name:    "bench_f64_search",
		Records: []arrow.RecordBatch{rec},
	}

	cfg := DefaultArrowHNSWConfig()
	cfg.Dims = dims
	cfg.DataType = types.VectorTypeFloat64
	cfg.M = 16
	cfg.MMax = 32
	cfg.EfConstruction = 64
	cfg.EfSearch = 64
	cfg.Metric = lbcore.MetricEuclidean

	hnsw := NewArrowHNSW(ds, &cfg, nil)
	defer hnsw.Close()

	rowIdxs := make([]int, numVecs)
	batchIdxs := make([]int, numVecs)
	for i := 0; i < numVecs; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}

	_, err := hnsw.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	if err != nil {
		b.Fatalf("AddBatch failed: %v", err)
	}

	query := allVecs[0]
	ctx := context.Background()
	searchOpts := types.SearchOptions{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = hnsw.SearchVectorsWithBitmap(ctx, query, 10, nil, searchOpts)
	}
}

