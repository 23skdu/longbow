package store

import (
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/23skdu/longbow/internal/metrics"
)

// TestHNSW_HighDimensionGrowth tests index growth with 3072-dim vectors
// to verify memory pressure handling and growth patterns for large dimensions.
func TestHNSW_HighDimensionGrowth(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 3072
	numVectors := 10000

	// Create schema with high-dimension vector
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	ds := NewDataset("high_dim_test", schema)

	// Configure HNSW for high dimensions
	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.M = 16
	config.MMax = 16
	config.MMax0 = 32
	config.EfConstruction = 100
	config.InitialCapacity = 1024 // Start small to test growth

	hnsw := NewArrowHNSW(ds, config, nil)

	// Track memory via runtime stats
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	t.Logf("Initial heap alloc: %d MB", m1.HeapAlloc/(1024*1024))

	// Reset growth metric
	metrics.HNSWIndexGrowthDuration.Write(&dto.Metric{})

	// Add vectors in batches to trigger multiple growth operations
	batchSize := 500
	for batch := 0; batch < numVectors/batchSize; batch++ {
		// Build batch
		builder := array.NewRecordBuilder(mem, schema)

		idBuilder := builder.Field(0).(*array.Uint32Builder)
		vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
		vecValueBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

		for i := 0; i < batchSize; i++ {
			vecID := uint32(batch*batchSize + i)
			idBuilder.Append(vecID)

			vecBuilder.Append(true)
			for d := 0; d < dims; d++ {
				// Generate deterministic but varied values
				val := float32(int(vecID)*dims+d) / float32(numVectors*dims)
				vecValueBuilder.Append(val)
			}
		}

		rec := builder.NewRecordBatch()

		// Add to dataset
		rec.Retain()
		ds.dataMu.Lock()
		ds.Records = append(ds.Records, rec)
		batchIdx := len(ds.Records) - 1
		ds.dataMu.Unlock()

		// Add to index
		for i := 0; i < batchSize; i++ {
			_, err := hnsw.AddByLocation(batchIdx, i)
			require.NoError(t, err)
		}

		if (batch+1)%4 == 0 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			t.Logf("After %d vectors: heap=%d MB",
				(batch+1)*batchSize, m.HeapAlloc/(1024*1024))
		}

		// Manual release
		rec.Release()
		builder.Release()
	}

	// Verify final state
	require.Equal(t, numVectors, hnsw.Len(), "Should have indexed all vectors")

	runtime.ReadMemStats(&m2)
	memDelta := int64(m2.HeapAlloc - m1.HeapAlloc)
	memPerVector := memDelta / int64(numVectors)
	t.Logf("Final heap alloc: %d MB", m2.HeapAlloc/(1024*1024))
	t.Logf("Memory per vector: %d bytes", memPerVector)
	t.Logf("Total memory delta: %d bytes (%.2f MB)",
		memDelta, float64(memDelta)/(1024*1024))

	// Verify growth metric was recorded
	var metric dto.Metric
	err := metrics.HNSWIndexGrowthDuration.Write(&metric)
	require.NoError(t, err)

	if metric.Histogram != nil && metric.Histogram.SampleCount != nil {
		t.Logf("Growth operations: %d", *metric.Histogram.SampleCount)
		require.Greater(t, *metric.Histogram.SampleCount, uint64(0),
			"Should have recorded growth operations")
	}

	// Verify search still works with high-dim vectors
	queryVec := make([]float32, dims)
	for i := range queryVec {
		queryVec[i] = float32(i) / float32(dims)
	}

	results, err := hnsw.SearchVectors(queryVec, 10, nil, SearchOptions{})
	require.NoError(t, err)
	require.Len(t, results, 10, "Should return 10 results")

	t.Logf("Search completed successfully with %d results", len(results))
}

// TestHNSW_HighDimensionGrowth_MemoryPressure tests growth under memory constraints
func TestHNSW_HighDimensionGrowth_MemoryPressure(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 3072
	numVectors := 5000

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	ds := NewDataset("mem_pressure_test", schema)

	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.M = 16
	config.InitialCapacity = 512 // Very small initial capacity to force frequent growth

	hnsw := NewArrowHNSW(ds, config, nil)

	// Track memory allocations via runtime
	type memSample struct {
		vectors int
		heapMB  uint64
	}
	samples := make([]memSample, 0, 10)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	samples = append(samples, memSample{0, m.HeapAlloc / (1024 * 1024)})

	batchSize := 1000
	for batch := 0; batch < numVectors/batchSize; batch++ {
		builder := array.NewRecordBuilder(mem, schema)

		idBuilder := builder.Field(0).(*array.Uint32Builder)
		vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
		vecValueBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

		for i := 0; i < batchSize; i++ {
			vecID := uint32(batch*batchSize + i)
			idBuilder.Append(vecID)
			vecBuilder.Append(true)
			for d := 0; d < dims; d++ {
				vecValueBuilder.Append(float32(int(vecID) + d))
			}
		}

		rec := builder.NewRecordBatch()

		rec.Retain()
		ds.dataMu.Lock()
		ds.Records = append(ds.Records, rec)
		batchIdx := len(ds.Records) - 1
		ds.dataMu.Unlock()

		for i := 0; i < batchSize; i++ {
			_, err := hnsw.AddByLocation(batchIdx, i)
			require.NoError(t, err)
		}

		runtime.ReadMemStats(&m)
		samples = append(samples, memSample{(batch + 1) * batchSize, m.HeapAlloc / (1024 * 1024)})

		// Manual release
		rec.Release()
		builder.Release()
	}

	// Analyze memory growth pattern
	t.Logf("Memory growth pattern:")
	for i, sample := range samples {
		if i > 0 {
			delta := int64(sample.heapMB) - int64(samples[i-1].heapMB)
			t.Logf("  %d vectors: %d MB (delta: %+d MB)", sample.vectors, sample.heapMB, delta)
		} else {
			t.Logf("  Initial: %d MB", sample.heapMB)
		}
	}

	// Verify reasonable memory usage
	// Theoretical minimum: numVectors * dims * 4 bytes (float32)
	theoreticalMinMB := float64(numVectors*dims*4) / (1024 * 1024)

	// Use max heap seen to avoid GC-induced negative deltas
	var maxHeap uint64
	for _, sample := range samples {
		if sample.heapMB > maxHeap {
			maxHeap = sample.heapMB
		}
	}

	actualUsedMB := float64(maxHeap - samples[0].heapMB)
	overhead := actualUsedMB / theoreticalMinMB

	t.Logf("Theoretical minimum: %.2f MB", theoreticalMinMB)
	t.Logf("Actual used (max heap): %.2f MB", actualUsedMB)
	t.Logf("Overhead factor: %.2fx", overhead)

	// Allow up to 8x overhead for graph structure, arenas, etc.
	// High-dimension vectors (3072 dims) have significant graph overhead
	require.Less(t, overhead, 8.0, "Memory overhead should be reasonable")
}
