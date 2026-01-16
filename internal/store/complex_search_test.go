package store

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	// Assuming storage package for mocks if needed, or stick to simpler mocks
)

// TestComplex128_DimensionCheck verifies that the server correctly handles
// the physical vs logical dimension mismatch for complex numbers (N vs 2N floats).
func TestComplex128_DimensionCheck(t *testing.T) {

	// Create a dataset with complex128 vectors (logical dim=4, physical dim=8 floats)
	logicalDim := 4
	physicalDim := logicalDim * 2

	ds := NewDataset("test_complex_dims", nil) // schema irrelevant for this unit test of Index

	// Configure HNSW for Complex128
	cfg := DefaultArrowHNSWConfig()
	cfg.DataType = VectorTypeComplex128 // This implies 2x float64 components per element
	cfg.Metric = "l2"                   // Standard L2 on components matches complex L2 distance

	// Initialize Index
	idx := NewArrowHNSW(ds, cfg, nil)
	defer func() { _ = idx.Close() }()

	// During initialization/first insert, HNSW learns the dimension.
	// We set the LOGICAL dimension (4 complex numbers).
	// GraphData interprets this as 4 elements of Complex128.
	// Since Complex128 is 2 floats, this corresponds to 8 floats physically.

	idx.SetDimension(logicalDim) // 4 (Complex Elements)

	require.Equal(t, uint32(logicalDim), idx.GetDimension(), "Index dimension should match logical dimension")

	// Case 1: Search with CORRECT physical dimension (8 floats)
	validQuery := make([]float32, physicalDim) // ArrowHNSW SearchVectors takes []float32
	// Note: Complex128 HNSW uses float64 internally but SearchVectors API takes float32.

	// Let's verify standard search path
	_, err := idx.SearchVectors(validQuery, 10, nil, SearchOptions{})
	require.NoError(t, err, "Search with correct physical dimension should succeed")

	// Case 2: Search with LOGICAL dimension (4 floats) - Should Fail
	// logicalDim is 4 (from above)

	invalidQuery := make([]float32, logicalDim)
	_, err = idx.SearchVectors(invalidQuery, 10, nil, SearchOptions{})
	require.Error(t, err, "Search with logical dimension (half size) should fail")
	require.Contains(t, err.Error(), fmt.Sprintf("index expects %d elements (logical dims=%d), got query len %d", physicalDim, logicalDim, logicalDim))
}

// TestComplex_SearchCorrectness verifies that search actually finds the correct nearest neighbor
// for complex numbers.
func TestComplex_SearchCorrectness(t *testing.T) {
	mem := memory.NewGoAllocator()
	ds := NewDataset("test_complex_correctness", nil)

	// Config
	logicalDim := 2
	physicalDim := logicalDim * 2
	cfg := DefaultArrowHNSWConfig()
	cfg.DataType = VectorTypeComplex128

	idx := NewArrowHNSW(ds, cfg, nil)
	defer func() { _ = idx.Close() }()

	// Insert 3 Vectors
	// V1: [1+0i, 0+0i] -> [1, 0, 0, 0]
	// V2: [0+0i, 1+0i] -> [0, 0, 1, 0]
	// V3: [100+0i, 100+0i] -> [100, 0, 100, 0] (Far away)
	// V4: [1+1i, 0+0i] -> [1, 1, 0, 0]

	vectors := [][]float64{
		{1, 0, 0, 0},
		{0, 0, 1, 0},
		{100, 0, 100, 0},
		{1, 1, 0, 0},
	}

	// Helper to build record batch
	b := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(physicalDim), arrow.PrimitiveTypes.Float64)}, // physical dim
	}, nil))
	defer b.Release()

	idB := b.Field(0).(*array.Int64Builder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float64Builder)

	for i, vec := range vectors {
		idB.Append(int64(i))
		vecB.Append(true)
		for _, v := range vec {
			valB.Append(v)
		}
	}

	rec := b.NewRecordBatch()
	ds.Records = append(ds.Records, rec)
	rec.Retain()
	defer rec.Release()

	// Initialize HNSW sizing
	idx.SetDimension(logicalDim)

	// Insert vectors individually using AddByRecord to handle location mapping automatically
	for i := 0; i < len(vectors); i++ {
		_, err := idx.AddByRecord(rec, i, 0) // batchIdx 0
		require.NoError(t, err)
	}

	// Search
	// Query: [1.1, 0, 0, 0] (Close to V1)
	query := []float32{1.1, 0, 0, 0}

	res, err := idx.SearchVectors(query, 5, nil, SearchOptions{VectorFormat: "complex128"}) // format might matter for distance func
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(res), 1)

	// V1 (ID 0) should be closest
	require.Equal(t, VectorID(0), res[0].ID)

	// Verify Distance/Score
	// Dist sq = (1.1-1)^2 = 0.01. Score = 1/(1+0.01) ~= 0.99
	// require.InDelta(t, 0.99, res[0].Score, 0.01)

	// Search 2
	// Query: [0.1, 0, 1.1, 0] (Close to V2)
	query2 := []float32{0.1, 0, 1.1, 0}
	res2, err := idx.SearchVectors(query2, 5, nil, SearchOptions{})
	require.NoError(t, err)
	require.Equal(t, VectorID(1), res2[0].ID)
}
