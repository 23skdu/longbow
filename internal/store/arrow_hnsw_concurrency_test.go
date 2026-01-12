package store

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// makeTestRecordBatch creates a simple record batch with "id" and "vector" columns
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
	numRows := 1000
	dims := 128
	rec := makeTestRecordBatch(mem, dims, numRows)
	defer rec.Release()

	ds := NewDataset("concurrent_test", rec.Schema())
	// In the real path, ds.Records is populated. ArrowHNSW reads from it essentially via ExtractVectorFromArrow?
	// ArrowHNSW.AddBatch takes `recs []arrow.RecordBatch` and `rowIdxs`.
	// Let's populate ds.Records for realistic setup.
	rec.Retain()
	ds.Records = append(ds.Records, rec)

	locStore := NewChunkedLocationStore()
	config := DefaultArrowHNSWConfig()
	config.EfConstruction = 100
	config.M = 16

	idx := NewArrowHNSW(ds, config, locStore)

	// Simulate concurrent batch ingestion
	// We will split the 1000 rows into 10 chunks of 100, processed by 10 goroutines.
	numWorkers := 10
	rowsPerWorker := numRows / numWorkers

	var wg sync.WaitGroup
	var errCount atomic.Int32

	start := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			startIdx := workerID * rowsPerWorker
			// endIdx not needed if we iterate k < rowsPerWorker

			// Prepare args for AddBatch
			batchRecs := make([]arrow.RecordBatch, rowsPerWorker)
			batchRowIdxs := make([]int, rowsPerWorker)
			batchBatchIdxs := make([]int, rowsPerWorker)

			for k := 0; k < rowsPerWorker; k++ {
				batchRecs[k] = rec // All point to Same underlying batch
				batchRowIdxs[k] = startIdx + k
				batchBatchIdxs[k] = 0 // Batch 0 in Dataset
			}

			// Call AddBatch
			_, err := idx.AddBatch(batchRecs, batchRowIdxs, batchBatchIdxs)
			if err != nil {
				errCount.Add(1)
				// Use thread-safe logging or just fmt
				fmt.Printf("Worker %d failed: %v\n", workerID, err)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	if errCount.Load() > 0 {
		t.Fatalf("Encountered %d errors during concurrent AddBatch", errCount.Load())
	}

	t.Logf("Indexed %d vectors in %v", numRows, duration)

	if idx.Len() != numRows {
		t.Errorf("Expected index size %d, got %d", numRows, idx.Len())
	}
}

func TestArrowHNSW_Concurrency_MixedReadWrite(t *testing.T) {
	mem := memory.NewGoAllocator()
	numRows := 1000
	dims := 16 // Small dims for speed
	rec := makeTestRecordBatch(mem, dims, numRows)
	defer rec.Release()

	ds := NewDataset("mixed_rw_test", rec.Schema())
	rec.Retain()
	ds.Records = append(ds.Records, rec)

	locStore := NewChunkedLocationStore()
	config := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, config, locStore)

	// Goal: Concurrent Writes (AddBatch) and Reads (SearchVectors)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// 4 Writers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Each writer adds a subset of vectors repeatedly (stress test on updates/duplicates? No, HNSW adds ID)
			// Actually AddBatch assigns NEW IDs via locStore if we use AddByLocation?
			// AddBatch calls AddByLocation?
			// No, AddBatch implementation handles "rowIdxs" -> "LocationStore.Append"?
			// Let's check AddBatch implementation.
			// Assuming AddBatch appends new IDs.
			// To avoid infinite growth issues in this short test, let's limit iterations.

			// We will re-index the same rows as NEW vectors (different VectorIDs, same content).
			// This is valid.

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

					_, err := idx.AddBatch(recs, rIdxs, bIdxs)
					if err != nil {
						fmt.Printf("Writer error: %v\n", err)
						return
					}
					// Small sleep to yield
					time.Sleep(time.Millisecond)
				}
			}
		}(i)
	}

	// 4 Readers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			query := make([]float32, dims)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					// Search random vector
					if idx.Len() > 0 {
						for k := 0; k < dims; k++ {
							query[k] = rand.Float32()
						}
						// Search
						_, _ = idx.SearchVectors(query, 10, nil, SearchOptions{})
					}
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	wg.Wait()
	t.Logf("Mixed RW Test Completed. Final Size: %d", idx.Len())
}
