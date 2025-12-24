package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHNSWIndex_EstimateMemory verifies that the HNSW index correctly reports
// its estimated memory usage and that it grows as items are added.
func TestHNSWIndex_EstimateMemory(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Create a dataset to back the index
	ds := NewDataset("test_memory_estimation", schema)

	// Initialize HNSW Index
	// Note: NewHNSWIndex takes only *Dataset in this version
	index := NewHNSWIndex(ds)

	// Initial memory should be small but non-zero (struct overhead)
	initialMem := index.EstimateMemory()
	t.Logf("Initial memory estimate: %d bytes", initialMem)
	assert.True(t, initialMem >= 0, "Initial memory should be non-negative")

	// Create a record batch with 100 vectors
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	numRows := 1000
	for i := 0; i < numRows; i++ {
		b.Field(0).(*array.Uint64Builder).Append(uint64(i))
		vecBuilder := b.Field(1).(*array.FixedSizeListBuilder)
		valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)
		vecBuilder.Append(true)
		valBuilder.Append(float32(i)) // x
		valBuilder.Append(float32(i)) // y
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	// Add batch to index
	// Note: We need to use AddByLocation or similar if AddBatch isn't directly usable independently of Store/Dataset integration
	// But AddBatch is on VectorIndex interface, so we can use it.
	// We need 3 input slices for AddBatch: records, rowIdxs, batchIdxs
	// But wait, HNSWIndex.AddBatch signature in `hnsw.go` (checked previously) matches.
	// However, HNSW depends on vectors being in the Dataset to lookup if we use AddByLocation.
	// Let's rely on AddBatch if it handles vector extraction.
	// Looking at `hnsw.go` (from previous context), AddBatch typically iterates and calls AddByLocation.
	// So we must put the record into the dataset first.
	
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	rec.Retain() // Dataset owns one ref
	ds.dataMu.Unlock()
    
    // Construct inputs for AddBatch
    recs := make([]arrow.RecordBatch, numRows)
    rowIdxs := make([]int, numRows)
    batchIdxs := make([]int, numRows)
    for i := 0; i < numRows; i++ {
        recs[i] = rec
        rowIdxs[i] = i
        batchIdxs[i] = 0 // The first batch in dataset
    }

	_, err := index.AddBatch(recs, rowIdxs, batchIdxs)
	require.NoError(t, err)

	// Check memory after adding
	afterMem := index.EstimateMemory()
	t.Logf("Memory estimate after adding %d vectors: %d bytes", numRows, afterMem)

	// It should have grown
	assert.Greater(t, afterMem, initialMem, "Memory estimate should increase after indexing vectors")
    
    // Roughly verify the size. 
    // 1000 vectors * (2 dims * 4 bytes + overhead).
    // Overhead per node approx 100-200 bytes.
    // 1000 * (8 + 200) ~= 208,000 bytes.
    // Let's just assert it is reasonably larger.
    assert.True(t, afterMem > initialMem + int64(numRows)*10, "Memory growth should be significant per vector")
}
