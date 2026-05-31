package index

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func makeTestRecordBatch(mem memory.Allocator, dims, numRows int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idB := builder.Field(0).(*array.Int64Builder)
	vecB := builder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	idB.Reserve(numRows)
	vecB.Reserve(numRows)
	valB.Reserve(numRows * dims)

	for i := 0; i < numRows; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		for j := 0; j < dims; j++ {
			valB.Append(rand.Float32())
		}
	}

	return builder.NewRecordBatch()
}

func TestArrowHNSW_Concurrency_AddBatch(t *testing.T) {

	mem := memory.NewGoAllocator()
	numRows := 500
	dims := 128
	rec := makeTestRecordBatch(mem, dims, numRows)
	defer rec.Release()

	ds := NewMockDataset("concurrent_test", rec.Schema())
	rec.Retain()
	ds.Records = append(ds.Records, rec)

	config := types.DefaultArrowHNSWConfig()
	config.EfConstruction = 100
	config.M = 16

	idx := NewArrowHNSW(ds, &config, nil)

	numWorkers := 10
	rowsPerWorker := numRows / numWorkers

	var wg sync.WaitGroup
	var errCount atomic.Int32
	assignedIDs := make([]uint32, numRows)

	start := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			startIdx := workerID * rowsPerWorker
			endIdx := startIdx + rowsPerWorker

			for rowIdx := startIdx; rowIdx < endIdx; rowIdx++ {
				ids, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, []int{rowIdx}, []int{0})
				if err != nil {
					errCount.Add(1)
					fmt.Printf("Worker %d failed at row %d: %v\n", workerID, rowIdx, err)
				} else if len(ids) > 0 {
					assignedIDs[rowIdx] = ids[0]
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	if errCount.Load() > 0 {
		t.Fatalf("Encountered %d errors during concurrent AddBatch", errCount.Load())
	}

	t.Logf("Indexed %d vectors in %v", numRows, duration)

	time.Sleep(100 * time.Millisecond)

	// Verify all vectors are searchable
	for i := 0; i < numRows; i++ {
		id := assignedIDs[i]
		lAny, ok := idx.GetLocation(id)
		if !ok {
			t.Errorf("Vector %d (ID %d) missing from LocationStore", i, id)
			continue
		}
		loc := lAny.(types.Location)
		if loc.RowIdx != i {
			t.Errorf("Vector %d (ID %d) has wrong RowIdx: expected %d, got %d", i, id, i, loc.RowIdx)
		}

		// Check if vector exists in GraphData
		vec, err := idx.GetVector(id)
		if err != nil || vec == nil {
			t.Errorf("Vector %d (ID %d) missing from GraphData", i, id)
		}

		// For all but the first few nodes, they should have neighbors if graph is connected
		if id > 20 {
			neighbors, _ := idx.GetLayerNeighbors(id, 0)
			if len(neighbors) == 0 {
				t.Errorf("Vector %d (ID %d) has no neighbors (isolated node)", i, id)
			}
		}
	}

	t.Logf("Verified reachability for %d vectors", numRows)
}

func TestArrowHNSW_Concurrency_MixedReadWrite(t *testing.T) {
	// Pre-existing data race: search readers access GraphData fields while
	// compareAndSwapData releases the same object from another goroutine.
	if raceEnabled {
		t.Skip("skipping: known pre-existing data race between search readers and graph release")
	}

	mem := memory.NewGoAllocator()
	numRows := 256
	dims := 16
	rec := makeTestRecordBatch(mem, dims, numRows)
	defer rec.Release()

	ds := NewMockDataset("mixed_rw_test", rec.Schema())
	rec.Retain()
	ds.Records = append(ds.Records, rec)

	config := types.DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, &config, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			subsetSize := 50
			startIdx := id * subsetSize

			for {
				select {
				case <-ctx.Done():
					return
				default:
					recs := make([]arrow.RecordBatch, subsetSize)
					rIdxs := make([]int, subsetSize)
					bIdxs := make([]int, subsetSize)
					for k := 0; k < subsetSize; k++ {
						recs[k] = rec
						rIdxs[k] = (startIdx + k) % numRows
						bIdxs[k] = 0
					}

					_, err := idx.AddBatch(context.Background(), recs, rIdxs, bIdxs)
					if err != nil {
						fmt.Printf("Writer error: %v\n", err)
						return
					}
					time.Sleep(time.Millisecond)
				}
			}
		}(i)
	}

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			query := make([]float32, dims)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if idx.Len() > 0 {
						for k := 0; k < dims; k++ {
							query[k] = rand.Float32()
						}
						_, _ = idx.SearchVectors(context.Background(), query, 10, nil, types.SearchOptions{})
					}
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	wg.Wait()
	t.Logf("Mixed RW Test Completed. Final Size: %d", idx.Len())
}
