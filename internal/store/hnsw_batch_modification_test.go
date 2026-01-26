package store

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// TestArrowHNSW_AddBatch_Sequential verifies adding multiple batches sequentially.
func TestArrowHNSW_AddBatch_Sequential(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 16
	numBatches := 5
	batchSize := 100

	// 1. Create Dataset and Index
	ds := &Dataset{Name: "batch_mod_seq"}
	cfg := DefaultArrowHNSWConfig()
	cfg.M = 16
	cfg.EfConstruction = 100
	idx := NewArrowHNSW(ds, cfg)
	// Manually set dims since we bypass full initialization
	idx.dims.Store(int32(dims))

	// 2. Generate and Insert Batches
	for b := 0; b < numBatches; b++ {
		vecs := make([][]float32, batchSize)
		for i := range vecs {
			vecs[i] = makeTestVector(dims, b*batchSize+i)
		}
		rec := makeBatchTestRecord(mem, dims, vecs)

		// Append to Dataset
		// Note: Dataset owns the record, so we should Retain if we plan to keep using `rec` locally or if Dataset closes it.
		// For simplicity in test, assume Dataset takes ownership (or shares).
		rec.Retain()
		ds.Records = append(ds.Records, rec)

		rowIdxs := make([]int, batchSize)
		batchIdxs := make([]int, batchSize)
		for i := 0; i < batchSize; i++ {
			rowIdxs[i] = i
			batchIdxs[i] = b
		}

		ids, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
		require.NoError(t, err)
		require.Len(t, ids, batchSize)
		rec.Release()
	}

	// 3. Verify Count
	require.Equal(t, numBatches*batchSize, idx.Len())

	// 4. Verify Search (Basic Recall)
	// Search for a known vector
	queryVec := makeTestVector(dims, 50) // From 1st batch
	results, err := idx.SearchVectors(context.Background(), queryVec, 10, nil, SearchOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, results)
	found := false
	for _, r := range results {
		// We can't easily map back to b/i without checking vector content or using ID if predictable
		// But AddBatch returns IDs. We assumed monotonic.
		// Let's just check score/distance is close to 0 (assuming Euclidean distance behavior)
		if r.Score < 0.0001 {
			found = true
			break
		}
	}
	require.True(t, found, "Should find exact match for inserted vector")
}

// TestArrowHNSW_AddBatch_Concurrent verify concurrent calls to AddBatch.
func TestArrowHNSW_AddBatch_Concurrent(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 16
	numWorkers := 4
	batchSize := 50
	batchesPerWorker := 5

	ds := &Dataset{Name: "batch_mod_conc"}

	cfg := DefaultArrowHNSWConfig()
	cfg.M = 16
	cfg.EfConstruction = 100
	idx := NewArrowHNSW(ds, cfg)
	idx.dims.Store(int32(dims))

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for b := 0; b < batchesPerWorker; b++ {
				baseSeed := workerID*10000 + b*100
				vecs := make([][]float32, batchSize)
				for i := range vecs {
					vecs[i] = makeTestVector(dims, baseSeed+i)
				}
				rec := makeBatchTestRecord(mem, dims, vecs)

				// Safely append to dataset
				rec.Retain()
				ds.dataMu.Lock()
				ds.Records = append(ds.Records, rec)
				currentBatchIdx := len(ds.Records) - 1
				ds.dataMu.Unlock()

				rowIdxs := make([]int, batchSize)
				batchIdxs := make([]int, batchSize)
				for i := 0; i < batchSize; i++ {
					rowIdxs[i] = i
					batchIdxs[i] = currentBatchIdx
				}

				ids, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
				if err != nil {
					t.Errorf("Worker %d failed: %v", workerID, err)
					rec.Release()
					// Need to release the retained one too if we fail? Dataset cleanup usually handles it.
					return
				}
				if len(ids) != batchSize {
					t.Errorf("Worker %d got wrong ID count: %d", workerID, len(ids))
				}
				rec.Release()
			}
		}(w)
	}

	wg.Wait()

	totalVecs := numWorkers * batchesPerWorker * batchSize
	require.Equal(t, totalVecs, idx.Len())
}

// Helper to create Arrow record (duplicated from hnsw_batch_test.go if not visible)
// It seems visible since same package.
