package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func makeHybridTestBatch(mem memory.Allocator, numRows int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "tag", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	idB := b.Field(0).(*array.Int64Builder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	vecValB := vecB.ValueBuilder().(*array.Float32Builder)
	catB := b.Field(2).(*array.StringBuilder)
	tagB := b.Field(3).(*array.StringBuilder)

	for i := 0; i < numRows; i++ {
		idB.Append(int64(i))

		vecB.Append(true)
		// Vector: [i, i, i, i]
		val := float32(i)
		vecValB.AppendValues([]float32{val, val, val, val}, nil)

		// Category: "odd" or "even"
		if i%2 == 0 {
			catB.Append("even")
		} else {
			catB.Append("odd")
		}

		// Tag: "A" for first half, "B" for second half
		if i < numRows/2 {
			tagB.Append("A")
		} else {
			tagB.Append("B")
		}
	}

	return b.NewRecordBatch()
}

func TestHybridSearch_Integration(t *testing.T) {
	// 1. Setup Store
	mem := memory.NewGoAllocator()
	logger, _ := zap.NewDevelopment()
	st := NewVectorStore(mem, logger, 1024*1024*1024, 1024*1024*1024, time.Hour)
	defer func() {
		if st.indexQueue != nil {
			st.indexQueue.Stop()
		}
	}()

	datasetName := "hybrid_test_ds"

	// 2. Ingest Data
	rec := makeHybridTestBatch(mem, 20)
	defer rec.Release()

	// Manual Dataset Creation
	ds := NewDataset(datasetName, rec.Schema())
	ds.Index = NewHNSWIndex(ds) // Initialize Index
	ds.Records = append(ds.Records, rec)
	rec.Retain() // Ownership by dataset

	st.mu.Lock()
	st.datasets[datasetName] = ds
	st.mu.Unlock()

	for i := 0; i < 20; i++ {
		rec.Retain()
		job := IndexJob{
			DatasetName: datasetName,
			Record:      rec,
			RowIdx:      i,
			BatchIdx:    0,
			CreatedAt:   time.Now(),
		}
		st.indexQueue.Send(job)
	}

	// 3. Wait for Indexing
	// 3. Wait for Indexing
	require.Eventually(t, func() bool {
		st.mu.RLock()
		defer st.mu.RUnlock()
		d, ok := st.datasets[datasetName]
		if !ok || d.Index == nil {
			return false
		}
		if d.Index.Len() != 20 {
			return false
		}

		// Check Inverted Index
		d.dataMu.RLock()
		defer d.dataMu.RUnlock()
		if d.InvertedIndexes == nil {
			return false
		}
		catIdx, ok := d.InvertedIndexes["category"]
		if !ok {
			return false
		}
		bm := catIdx.Get("even")
		if bm == nil {
			return false
		}
		return bm.GetCardinality() == 10
	}, 10*time.Second, 100*time.Millisecond, "Index should reach 20 items and Inverted Index populated")

	// 4. Perform Hybrid Search
	ctx := context.Background()

	// Case A: Filter "category"="even". Should match 0,2,4,6,8,10,12,14,16,18. (10 items)
	// Query: Vector 0 ({0,0,0,0}).
	// Expect: 0, 2, 4...
	filters := map[string]string{
		"category": "even",
	}
	qVec := []float32{0, 0, 0, 0}

	results, err := st.HybridSearch(ctx, datasetName, qVec, 5, filters)
	require.NoError(t, err)
	assert.Len(t, results, 5)

	for _, res := range results {
		// Verify ID is even.
		// Result has Score and ID (internal).
		// We know for HNSWIndex (and this is default), ID correlates to insertion order 0..N.
		// So ID should be even.
		id := int(res.ID)
		assert.Equal(t, 0, id%2, "Expected even ID, got %d", id)
	}

	// Case B: Filter "category"="odd" AND "tag"="B".
	// Odd: 1,3,5...19.
	// Tag B: 10..19.
	// Intersection: 11, 13, 15, 17, 19. (5 items)
	filters2 := map[string]string{
		"category": "odd",
		"tag":      "B",
	}

	results2, err := st.HybridSearch(ctx, datasetName, qVec, 10, filters2)
	require.NoError(t, err)
	assert.Len(t, results2, 5)

	expectedIDs := []int{11, 13, 15, 17, 19}
	// Sort results by ID to compare? They are sorted by score (distance to 0).
	// Distance to 0 increases with ID. So should be sorted 11,13,15,17,19.
	for i, res := range results2 {
		assert.Equal(t, uint32(expectedIDs[i]), uint32(res.ID))
	}

	// Case C: No results
	filters3 := map[string]string{
		"category": "nonexistent",
	}
	results3, err := st.HybridSearch(ctx, datasetName, qVec, 5, filters3)
	require.NoError(t, err)
	assert.Len(t, results3, 0)
}
