package store

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// FuzzIngestionIntegrity_Concurrent verifies that concurrent ingestion
// results in a consistent index state where all vectors are retrievable.
func FuzzIngestionIntegrity_Concurrent(f *testing.F) {
	// Seed corpus: (writers, batches, rowsPerBatch)
	f.Add(4, 5, 20)
	f.Add(2, 10, 50)

	f.Fuzz(func(t *testing.T, numWriters int, batchesPerWriter int, rowsPerBatch int) {
		// Constraints to keep fuzz test fast
		if numWriters <= 0 || numWriters > 10 {
			return
		}
		if batchesPerWriter <= 0 || batchesPerWriter > 20 {
			return
		}
		if rowsPerBatch <= 0 || rowsPerBatch > 100 {
			return
		}

		mem := memory.NewGoAllocator()
		// Use a unique dataset name per fuzz iteration
		dsName := fmt.Sprintf("integrity_ds_%d_%d_%d", numWriters, batchesPerWriter, rowsPerBatch)

		// Setup store with fast indexing
		store := NewVectorStore(mem, zerolog.Nop(), 1024*1024*100, 1024*1024*100, 1*time.Hour)
		defer func() { _ = store.Close() }()

		// Force HNSW2 (ArrowHNSW) usage and fast Indexing
		// store.DatasetInitHook or similar?
		// Actually defaults are now HNSW2.
		// We rely on defaults.

		var wg sync.WaitGroup
		wg.Add(numWriters)

		// Verification Map: ID -> Vector
		// We need to track what we inserted to verify later.
		// Since Fuzz runs in parallel, we need a thread-safe map or just specific ID ranges.
		// Let's use deterministic ID generation based on writer/batch/row.
		// ID = writer * 100000 + batch * 1000 + row

		// Pre-generate vectors to verify later?
		// Or just verify count and random sample search.
		// Verification of specific vectors is keys.

		for w := 0; w < numWriters; w++ {
			go func(writerID int) {
				defer wg.Done()
				for b := 0; b < batchesPerWriter; b++ {
					// Create batch
					rec := createIntegrityTestBatch(t, mem, rowsPerBatch, writerID, b)
					err := store.StoreRecordBatch(context.Background(), dsName, rec)
					rec.Release()
					if err != nil {
						t.Errorf("StoreRecordBatch failed: %v", err)
					}
					// Small yield
					if b%5 == 0 {
						time.Sleep(time.Millisecond)
					}
				}
			}(w)
		}

		wg.Wait()

		// Wait for ingestion pipeline to drain
		// We can check IngestionQueueDepth or wait for expected count.
		totalExpected := numWriters * batchesPerWriter * rowsPerBatch

		require.Eventually(t, func() bool {
			ds, ok := store.getDataset(dsName)
			if !ok {
				return false
			}
			ds.dataMu.RLock()
			defer ds.dataMu.RUnlock()

			// We check Records length (memory applied)
			// total rows calculation:
			count := 0
			for _, r := range ds.Records {
				count += int(r.NumRows())
			}
			return count == totalExpected
		}, 10*time.Second, 100*time.Millisecond, "Dataset failed to reach expected record count")

		// Retrieve dataset
		ds, ok := store.getDataset(dsName)
		require.True(t, ok)

		// Wait for Indexing (Background)
		// We don't have a direct "Index Drained" signal in public API easily without metrics.
		// But we can check ds.Index.Len()
		require.Eventually(t, func() bool {
			if ds.Index == nil {
				return false
			}
			return ds.Index.Len() == totalExpected
		}, 10*time.Second, 100*time.Millisecond, "Index failed to reach expected size")

		// Verify Search Integrity (Sample)
		// Pick a random vector from what we inserted (re-generate)
		// and ensure we find it as top-1 (integrity check)

		// Pick random target
		randWriter := rand.Intn(numWriters)
		randBatch := rand.Intn(batchesPerWriter)
		// We re-create that batch
		targetRec := createIntegrityTestBatch(t, mem, rowsPerBatch, randWriter, randBatch)
		defer targetRec.Release()

		// Pick random row in batch
		randRow := rand.Intn(rowsPerBatch)
		vecCol := targetRec.Column(1).(*array.FixedSizeList)
		valCol := vecCol.ListValues().(*array.Float32)

		queryVec := make([]float32, 128)
		startIdx := randRow * 128
		for k := 0; k < 128; k++ {
			queryVec[k] = valCol.Value(startIdx + k)
		}

		// Search
		results, err := ds.Index.SearchVectors(queryVec, 1, nil, SearchOptions{})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 1, "Should find at least 1 result")

		// Distance/Score should be very close to 0 (exact match)
		require.InDelta(t, 0.0, results[0].Score, 0.0001, "Top match should be exact vector")
	})
}

func createIntegrityTestBatch(t *testing.T, mem memory.Allocator, rows int, wID, bID int) arrow.RecordBatch {
	// Schema: id (string), vector (float32[128])
	dims := 128
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idB := builder.Field(0).(*array.StringBuilder)
	vecB := builder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	idB.Reserve(rows)
	vecB.Reserve(rows)
	valB.Reserve(rows * dims)

	// Deterministic Seed for this batch
	// Seed = wID * 10000 + bID
	rng := rand.New(rand.NewSource(int64(wID*10000 + bID)))

	for i := 0; i < rows; i++ {
		idStr := fmt.Sprintf("w%d_b%d_r%d", wID, bID, i)
		idB.Append(idStr)
		vecB.Append(true)
		for j := 0; j < dims; j++ {
			valB.Append(rng.Float32())
		}
	}

	return builder.NewRecordBatch()
}
