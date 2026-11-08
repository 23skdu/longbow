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
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func makeFloat16TestRecordBatch(mem memory.Allocator, dims, numRows int, seed int64) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vec", Type: arrow.FixedSizeListOf(int32(dims), arrow.FixedWidthTypes.Float16)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idB := builder.Field(0).(*array.Int64Builder)
	vecB := builder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float16Builder)

	idB.Reserve(numRows)
	vecB.Reserve(numRows)
	valB.Reserve(numRows * dims)

	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < numRows; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		for j := 0; j < dims; j++ {
			valB.Append(float16.New(rng.Float32()*2 - 1))
		}
	}

	return builder.NewRecordBatch()
}

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
	if testing.Short() {
		batchSize = 1_000
	}

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

	// This deadline is a deadlock guard, not a performance budget. Measured on
	// the AVX2 client (2026-10-13): 4.7s natively, 61s under -race. It was
	// never enforced while AddBatch ignored ctx, and now that cancellation is
	// honoured it has to leave headroom for race instrumentation so it does
	// not flake.
	//
	// It used to fire here: the R8 hop gate computed its budget from
	// log(N)/log(MMax0), which at 2,000 nodes and MMax0=64 is 1.8 hops, so a
	// perfectly navigable 3.0-hop graph was rejected and rebuilt sequentially -
	// O(m^2) diversity pruning per node, at a 700x cost. bulkHopFloor now puts
	// a floor under that budget. See arrow_hnsw_bulk.go.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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

func TestArrowHNSW_ConcurrentAddBatch_Float16_Search(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 128
	batchSize := 2_500
	if testing.Short() {
		batchSize = 600
	}
	numBatches := 5

	recs := make([]arrow.RecordBatch, numBatches)
	var ds *MockDataset
	for i := 0; i < numBatches; i++ {
		rec := makeFloat16TestRecordBatch(mem, dims, batchSize, int64(100+i))
		defer rec.Release()
		rec.Retain()
		recs[i] = rec
		if ds == nil {
			ds = NewMockDataset("concurrent_f16", rec.Schema())
		}
		ds.Records = append(ds.Records, rec)
	}

	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeFloat16
	// Intentionally leave config.Dims = 0 to exercise store-style lazy dimension binding.
	idx := NewArrowHNSW(ds, &config, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	for b := 0; b < numBatches; b++ {
		wg.Add(1)
		go func(batchIdx int, r arrow.RecordBatch) {
			defer wg.Done()
			rowIdxs := make([]int, batchSize)
			batchIdxs := make([]int, batchSize)
			for k := 0; k < batchSize; k++ {
				rowIdxs[k] = k
				batchIdxs[k] = 0
			}
			_, err := idx.AddBatch(ctx, []arrow.RecordBatch{r}, rowIdxs, batchIdxs)
			if err != nil {
				t.Errorf("batch %d AddBatch failed: %v", batchIdx, err)
			}
		}(b, recs[b])
	}
	wg.Wait()

	totalNodes := numBatches * batchSize
	meta := idx.GetMetadataSnapshot()
	if meta.NodeCount != int64(totalNodes) {
		t.Fatalf("expected nodeCount %d, got %d", totalNodes, meta.NodeCount)
	}

	epVec, err := idx.GetVector(meta.EntryPoint)
	if err != nil || epVec == nil {
		t.Fatalf("entry point %d vector missing after concurrent Float16 AddBatch: vec=%v err=%v", meta.EntryPoint, epVec, err)
	}

	for id := uint32(0); id < uint32(totalNodes); id += uint32(types.ChunkSize / 2) {
		v, err := idx.GetVector(id)
		if err != nil || v == nil {
			t.Fatalf("vector id %d missing after concurrent Float16 AddBatch: vec=%v err=%v", id, v, err)
		}
	}

	query := make([]float16.Num, dims)
	for i := range query {
		query[i] = float16.New(0.25)
	}
	results, err := idx.SearchVectors(ctx, query, 10, nil, types.SearchOptions{})
	if err != nil {
		t.Fatalf("SearchVectors failed after concurrent Float16 AddBatch: %v", err)
	}
	if len(results) == 0 {
		t.Fatalf("expected non-empty search results after concurrent Float16 AddBatch")
	}
}
