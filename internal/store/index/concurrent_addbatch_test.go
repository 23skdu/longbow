package index

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func makeInt8TestRecordBatch(mem memory.Allocator, dims, numRows int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vec", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Int8)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idB := builder.Field(0).(*array.Int64Builder)
	vecB := builder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Int8Builder)

	idB.Reserve(numRows)
	vecB.Reserve(numRows)
	valB.Reserve(numRows * dims)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numRows; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		for j := 0; j < dims; j++ {
			valB.Append(int8(rng.Intn(256) - 128))
		}
	}

	return builder.NewRecordBatch()
}

// TestArrowHNSW_ConcurrentAddBatch_Int8_2Batches is a regression test for
// the inBulkInsert reference counter fix and the GraphData/FlatAdjacency
// race fix. It runs two concurrent AddBatch calls with int8 data and
// verifies that both complete without deadlock and that all vectors are
// committed. Exercises both:
//   - the inBulkInsert spin-wait reference counter (prev commit), and
//   - the FlatAdjacency refs pin in GetNeighborsWithGen + removal of
//     PackedNeighbors nil-out in GraphData.Release() (this commit).
func TestArrowHNSW_ConcurrentAddBatch_Int8_2Batches(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 128
	batchSize := 5_000

	rec1 := makeInt8TestRecordBatch(mem, dims, batchSize)
	defer rec1.Release()
	rec1.Retain()
	ds := NewMockDataset("concurrent_int8", rec1.Schema())
	ds.Records = append(ds.Records, rec1)

	rec2 := makeInt8TestRecordBatch(mem, dims, batchSize)
	defer rec2.Release()
	rec2.Retain()
	ds.Records = append(ds.Records, rec2)

	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeInt8
	config.Dims = dims
	idx := NewArrowHNSW(ds, &config, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	start := time.Now()
	for batch := 0; batch < 2; batch++ {
		wg.Add(1)
		rec := rec1
		if batch == 1 {
			rec = rec2
		}
		go func(b int, r arrow.RecordBatch) {
			defer wg.Done()
			rowIdxs := make([]int, batchSize)
			batchIdxs := make([]int, batchSize)
			for k := 0; k < batchSize; k++ {
				rowIdxs[k] = k
				batchIdxs[k] = 0
			}
			_, err := idx.AddBatch(ctx, []arrow.RecordBatch{r}, rowIdxs, batchIdxs)
			if err != nil {
				t.Errorf("batch %d AddBatch failed: %v", b, err)
			}
		}(batch, rec)
	}
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("2x5k int8 concurrent AddBatch completed in %v", elapsed)

	meta := idx.GetMetadataSnapshot()
	if meta.NodeCount < int64(2*batchSize) {
		t.Errorf("expected nodeCount >= %d, got %d", 2*batchSize, meta.NodeCount)
	}
}
