package index

import (
	"context"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPIDTuner_GetMaxEf verifies that GetMaxEf accurately returns the configured limit.
func TestPIDTuner_GetMaxEf(t *testing.T) {
	t.Parallel()
	tuner := NewPIDTuner(0.95, 64, 1500)
	assert.Equal(t, 1500, tuner.GetMaxEf())

	var nilTuner *PIDTuner
	assert.Equal(t, 2000, nilTuner.GetMaxEf())
}

// TestFlushSearchMetrics_Idempotent verifies that flushSearchMetrics clears distComputeCount
// to prevent multiple flushes (such as explicit call and deferred call) from double-counting.
func TestFlushSearchMetrics_Idempotent(t *testing.T) {
	idx := &ArrowHNSW{
		name: "test_idx",
		config: types.ArrowHNSWConfig{
			SearchLayerSampleRate: 0,
		},
	}
	ctx := &ArrowSearchContext{
		distComputeCount: 42,
	}

	before := testutil.ToFloat64(metrics.HnswDistanceCalculations)
	idx.flushSearchMetrics(ctx)
	assert.Equal(t, 0, ctx.distComputeCount, "distComputeCount should be cleared to 0")
	after1 := testutil.ToFloat64(metrics.HnswDistanceCalculations)
	assert.Equal(t, before+42, after1)

	// Second flush must not add any more calculations
	idx.flushSearchMetrics(ctx)
	after2 := testutil.ToFloat64(metrics.HnswDistanceCalculations)
	assert.Equal(t, after1, after2, "second flush should be idempotent")
}

// TestAdaptiveEfScaling_SelectiveRoaringFilter_Float32 asserts that under selective
// filtering (5% match rate), initial efSearch scales adaptively to retrieve full k results.
func TestAdaptiveEfScaling_SelectiveRoaringFilter_Float32(t *testing.T) {
	const (
		n    = 1000
		dims = 16
		k    = 10
	)

	r := rand.New(rand.NewSource(42)) // #nosec G404
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(r.NormFloat64())
		}
		vecs[i] = v
	}

	idx, _ := buildTypedIndex(t, "adaptive_ef_f32", arrow.PrimitiveTypes.Float32, types.VectorTypeFloat32, vecs, 16)

	// 5% selective filter: admits 50 out of 1000 nodes (every 20th node)
	filter := roaring.NewBitmap()
	for i := uint32(0); i < n; i += 20 {
		filter.Add(i)
	}
	require.Equal(t, uint64(50), filter.GetCardinality())

	query := make([]float32, dims)
	for j := range query {
		query[j] = float32(r.NormFloat64())
	}

	// Request with small Ef=16. Without adaptive scaling, layer 0 would explore
	// ~16 nodes, finding ~0-1 admitted candidates.
	// Adaptive scaling raises efSearch upfront to >= ceil(10 / 0.05 * 1.25) = 250.
	res, err := idx.SearchVectorsWithBitmap(context.Background(), query, k, filter, types.SearchOptions{Ef: 16})
	require.NoError(t, err)
	require.Len(t, res, k, "adaptive scaling must return full k results on attempt 0")

	for _, hit := range res {
		assert.True(t, filter.Contains(uint32(hit.ID)), "result id %d must be in the filter bitmap", hit.ID) // #nosec G115
	}
}

// TestAdaptiveEfScaling_SelectiveFilter_Float64 asserts that non-float32 vectors ([]float64)
// benefit from adaptive ef scaling, retry loop, and correct result conversion.
func TestAdaptiveEfScaling_SelectiveFilter_Float64(t *testing.T) {
	const (
		n    = 800
		dims = 16
		k    = 8
	)

	r := rand.New(rand.NewSource(99)) // #nosec G404
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(r.NormFloat64())
		}
		vecs[i] = v
	}

	idx, _ := buildTypedIndex(t, "adaptive_ef_f64", arrow.PrimitiveTypes.Float64, types.VectorTypeFloat64, vecs, 16)

	// 10% selective filter: admits 80 out of 800 nodes
	filter := roaring.NewBitmap()
	for i := uint32(0); i < n; i += 10 {
		filter.Add(i)
	}

	queryF64 := make([]float64, dims)
	for j := range queryF64 {
		queryF64[j] = r.NormFloat64()
	}

	res, err := idx.SearchVectorsWithBitmap(context.Background(), queryF64, k, filter, types.SearchOptions{Ef: 16})
	require.NoError(t, err)
	require.Len(t, res, k, "float64 search must return full k results")

	for i, hit := range res {
		assert.True(t, filter.Contains(uint32(hit.ID)), "hit id %d must be admitted", hit.ID) // #nosec G115
		if i > 0 {
			assert.GreaterOrEqual(t, hit.Distance, res[i-1].Distance, "results must be ordered by distance ascending")
		}
	}
}

// TestAdaptiveEfScaling_SelectiveFilter_Int8 asserts that integer vector types ([]int8)
// benefit from adaptive ef scaling, unified retry, and extractSearchResults.
func TestAdaptiveEfScaling_SelectiveFilter_Int8(t *testing.T) {
	const (
		numRows = 500
		dims    = 16
		k       = 8
	)

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Int8)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idB := builder.Field(0).(*array.Int64Builder)
	vecB := builder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Int8Builder)

	idB.Reserve(numRows)
	vecB.Reserve(numRows)
	valB.Reserve(numRows * dims)

	rng := rand.New(rand.NewSource(123)) // #nosec G404
	for i := 0; i < numRows; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		for j := 0; j < dims; j++ {
			valB.Append(int8(rng.Intn(256) - 128))
		}
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()
	rec.Retain()

	ds := NewMockDataset("adaptive_ef_int8", schema)
	ds.Records = append(ds.Records, rec)

	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeInt8
	config.Dims = dims
	config.M = 16
	config.MMax = 16
	config.MMax0 = 16
	idx := NewArrowHNSW(ds, &config, nil)
	t.Cleanup(func() { _ = idx.Close() })

	rowIdxs := make([]int, numRows)
	batchIdxs := make([]int, numRows)
	for i := range rowIdxs {
		rowIdxs[i] = i
	}
	_, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	require.NoError(t, err)

	// 5% selective filter: admits 25 out of 500 nodes (every 20th node)
	filter := roaring.NewBitmap()
	for i := uint32(0); i < numRows; i += 20 {
		filter.Add(i)
	}

	queryInt8 := make([]int8, dims)
	for j := range queryInt8 {
		queryInt8[j] = int8(rng.Intn(256) - 128)
	}

	res, err := idx.SearchVectorsWithBitmap(context.Background(), queryInt8, k, filter, types.SearchOptions{Ef: 16})
	require.NoError(t, err)
	require.Len(t, res, k, "int8 search must return full k results under selective filter")

	for i, hit := range res {
		assert.True(t, filter.Contains(uint32(hit.ID)), "hit id %d must be admitted", hit.ID) // #nosec G115
		if i > 0 {
			assert.GreaterOrEqual(t, hit.Distance, res[i-1].Distance, "results must be ordered by distance ascending")
		}
	}
}

// TestAdaptiveEfScaling_MetadataPredicate asserts that predicate sampling accurately
// estimates selectivity and scales efSearch upfront.
func TestAdaptiveEfScaling_MetadataPredicate(t *testing.T) {
	const (
		n    = 1000
		dims = 16
		k    = 10
	)

	r := rand.New(rand.NewSource(777)) // #nosec G404
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(r.NormFloat64())
		}
		vecs[i] = v
	}

	idx, _ := buildTypedIndex(t, "adaptive_ef_pred", arrow.PrimitiveTypes.Float32, types.VectorTypeFloat32, vecs, 16)

	// 5% selectivity predicate: id % 20 == 0
	pred := modPredicate{mod: 20, want: 0}

	query := make([]float32, dims)
	for j := range query {
		query[j] = float32(r.NormFloat64())
	}

	res, err := idx.SearchVectorsWithBitmap(context.Background(), query, k, nil, types.SearchOptions{Predicate: pred, Ef: 16})
	require.NoError(t, err)
	require.Len(t, res, k, "metadata predicate search must return full k results")

	for _, hit := range res {
		assert.True(t, pred.IsMatch(uint32(hit.ID)), "result id %d must satisfy predicate", hit.ID) // #nosec G115
	}
}

// TestAdaptiveEfScaling_NilFilterMaskFallback verifies extractSearchResults correctly filters
// when filterMask is nil and only filterBitmap is provided.
func TestAdaptiveEfScaling_NilFilterMaskFallback(t *testing.T) {
	idx := &ArrowHNSW{
		deleted: roaring.NewBitmap(),
	}

	filter := roaring.NewBitmap()
	filter.Add(10)
	filter.Add(30)

	searchCtx := &ArrowSearchContext{
		filterBitmap: filter,
		filterMask:   nil, // explicitly nil
	}

	candidates := []types.Candidate{
		{ID: 10, Dist: 1.0},
		{ID: 20, Dist: 2.0}, // rejected by filterBitmap
		{ID: 30, Dist: 3.0},
		{ID: 40, Dist: 4.0}, // rejected by filterBitmap
	}

	results := idx.extractSearchResults(candidates, 5, searchCtx)
	require.Len(t, results, 2)
	assert.Equal(t, types.VectorID(10), results[0].ID)
	assert.Equal(t, types.VectorID(30), results[1].ID)
}

// BenchmarkAdaptiveEfScaling_SelectiveFilter benchmarks the throughput of filtered search
// at 5% selectivity on a 2,000-vector index.
func BenchmarkAdaptiveEfScaling_SelectiveFilter(b *testing.B) {
	const (
		n    = 2000
		dims = 32
		k    = 10
	)

	r := rand.New(rand.NewSource(42)) // #nosec G404
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(r.NormFloat64())
		}
		vecs[i] = v
	}

	idx, _ := buildTypedIndex(b, "bench_adaptive_ef", arrow.PrimitiveTypes.Float32, types.VectorTypeFloat32, vecs, 16)

	// 5% selective filter: admits 100 out of 2000 nodes (every 20th node)
	filter := roaring.NewBitmap()
	for i := uint32(0); i < n; i += 20 {
		filter.Add(i)
	}

	query := make([]float32, dims)
	for j := range query {
		query[j] = float32(r.NormFloat64())
	}

	ctx := context.Background()
	opts := types.SearchOptions{Ef: 16}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := idx.SearchVectorsWithBitmap(ctx, query, k, filter, opts)
		if err != nil || len(res) < k {
			b.Fatalf("search failed: err=%v, res=%d", err, len(res))
		}
	}
}
