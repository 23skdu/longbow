package store

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObservability_GranularMetrics(t *testing.T) {
	// Setup Float16 Index (Int8 support is partial, use Float16 to verify non-float32 metrics)
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}, {Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.FixedWidthTypes.Float16)}},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{0, 1}, nil)
	vb := b.Field(1).(*array.FixedSizeListBuilder)
	vbVal := vb.ValueBuilder().(*array.Float16Builder)
	f1 := float16.New(1.0)
	f0 := float16.New(0.0)
	vbVal.AppendValues([]float16.Num{f1, f0, f0, f1}, nil) // 2 vectors: [1.0, 0.0], [0.0, 1.0]
	vb.Append(true)
	vb.Append(true)

	rec := b.NewRecordBatch()
	defer rec.Release()

	ds := &Dataset{
		Name:    "granular_test",
		Records: []arrow.RecordBatch{rec},
		Schema:  schema,
	}

	config := DefaultArrowHNSWConfig()
	config.DataType = lbtypes.VectorTypeFloat16
	config.Float16Enabled = true // Ensure flag is synced

	// Initialize Index
	idx := NewArrowHNSW(ds, &config)

	// 1. Verify Insert Metrics (Float16)
	metrics.HNSWInsertLatencyByType.Reset()
	metrics.HNSWInsertLatencyByDim.Reset()

	_, err := idx.AddByLocation(context.Background(), 0, 0)
	require.NoError(t, err)

	// Check histograms recorded 1 event
	assert.Equal(t, 1, testutil.CollectAndCount(metrics.HNSWInsertLatencyByType), "Should record insert latency for float16")
	assert.Equal(t, 1, testutil.CollectAndCount(metrics.HNSWInsertLatencyByDim), "Should record insert latency for dim=2")

	// 2. Verify Search Metrics (Int8)
	metrics.HNSWSearchLatencyByType.Reset()

	// We search with float32 approx since Int8 input support in Search() input validation might be limited,
	// but the *Metric* switch uses config.DataType/data.Type which we set to Int8.
	q := []float32{1.0, 0.0}
	_, err = idx.Search(context.Background(), q, 1, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, testutil.CollectAndCount(metrics.HNSWSearchLatencyByType), "Should record search latency for int8 type")

	// 3. Verify Bulk Insert Metrics (Float32)
	// AddBatchBulk is specialized for Float32 API, so we test that as separate case or override config
	// To test Bulk metrics, we call AddBatchBulk directly.
	metrics.HNSWBulkInsertLatencyByType.Reset()

	// For bulk, we need a separate context or just call it.
	// It relies on h.config.DataType for label.
	// If call AddBatchBulk on this Int8 index, it might work if we pass float32s that get quantized?
	// But AddBatchBulk calls SetVectorFromFloat32. Int8 graph supports SetVectorFromFloat32 (conversion)?
	// GraphData.SetVectorFromFloat32 logic usually checks type.
	// If not supported, it returns error.
	// Let's assume for now we use a float32 index for bulk test to be safe on logic.

	configF32 := DefaultArrowHNSWConfig()
	configF32.DataType = lbtypes.VectorTypeFloat32
	idxF32 := NewArrowHNSW(ds, &configF32)

	vecs := [][]float32{{1.0, 0.0}}
	// We need to ensure graph has capacity/chunks. AddBatchBulk usually does prep.
	// We need to feed it context.
	// Also it calls h.data.Load(), so we need initialized data.
	err = idxF32.Grow(10, 2)
	require.NoError(t, err)

	err = idxF32.AddBatchBulk(context.Background(), 100, 1, vecs)
	require.NoError(t, err)

	assert.Equal(t, 1, testutil.CollectAndCount(metrics.HNSWBulkInsertLatencyByType), "Should record bulk insert latency")
}

func TestHNSW_ObservabilityMetrics(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := [][]float32{{1.0, 0.0}, {0.0, 1.0}}
	dims := 2
	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "observability_test",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	config := DefaultArrowHNSWConfig()
	// useRefinement := (h.config.SQ8Enabled || h.config.PQEnabled || h.config.BQEnabled)
	config.SQ8Enabled = true

	idx := NewArrowHNSW(ds, &config)

	// Add vectors
	_, err := idx.AddByLocation(context.Background(), 0, 0)
	require.NoError(t, err)
	_, err = idx.AddByLocation(context.Background(), 0, 1)
	require.NoError(t, err)

	// Reset metrics before search
	metrics.HNSWSearchLatencyByType.Reset()
	metrics.HNSWSearchLatencyByDim.Reset()
	metrics.HNSWRefineThroughput.Reset()

	// Search
	q := []float32{1.0, 0.0}
	_, err = idx.Search(context.Background(), q, 1, nil)
	require.NoError(t, err)

	// Verify Metrics
	typeLabel := "sq8" // Based on config.SQ8Enabled
	// Latency metrics are histograms, we can check if they were collected
	countByType := testutil.CollectAndCount(metrics.HNSWSearchLatencyByType)
	assert.GreaterOrEqual(t, countByType, 1, "Should record search latency by type")

	countByDim := testutil.CollectAndCount(metrics.HNSWSearchLatencyByDim)
	assert.GreaterOrEqual(t, countByDim, 1, "Should record search latency by dim")

	// Refine throughput
	refineCount := testutil.ToFloat64(metrics.HNSWRefineThroughput.WithLabelValues(typeLabel))
	// targetK = k * RefinementFactor = 1 * 2 = 2. But we only have 2 points total.
	// So results should be at most 2.
	assert.Greater(t, refineCount, 0.0, "Should record refinement throughput")
}
