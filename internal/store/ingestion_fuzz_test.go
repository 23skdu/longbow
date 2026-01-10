package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// FuzzIngestionPipeline_ConcurrentWrites simulates multiple concurrent writers
// to verify data integrity with the async pipeline.
func FuzzIngestionPipeline_ConcurrentWrites(f *testing.F) {
	f.Add(10, 5) // 10 writers, 5 batches each

	f.Fuzz(func(t *testing.T, numWriters int, batchesPerWriter int) {
		if numWriters <= 0 || numWriters > 50 {
			return
		}
		if batchesPerWriter <= 0 || batchesPerWriter > 50 {
			return
		}

		mem := memory.NewGoAllocator()
		store := NewVectorStore(mem, zerolog.Nop(), 1024*1024*1024, 1024*1024*100, 1*time.Hour)
		dsName := fmt.Sprintf("fuzz_ds_%d_%d", numWriters, batchesPerWriter)

		var wg sync.WaitGroup
		wg.Add(numWriters)

		start := time.Now()

		for i := 0; i < numWriters; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < batchesPerWriter; j++ {
					rec := createTestRecordBatch(t, 10)
					err := store.StoreRecordBatch(context.Background(), dsName, rec)
					rec.Release()
					// We just log error, f.Fuzz might fail on Fatal, but erratic errors in Fuzz are noisy.
					// Ideally we Assert types.
					if err != nil {
						t.Errorf("Writer %d failed: %v", id, err)
					}
				}
			}(i)
		}

		wg.Wait()
		duration := time.Since(start)
		t.Logf("Fuzz: %d writers, %d batches each took %v", numWriters, batchesPerWriter, duration)

		// Verification: Total records should match
		expected := numWriters * batchesPerWriter // * 10 rows

		ds, ok := store.getDataset(dsName)
		require.True(t, ok, "Dataset must exist")

		// Wait for async processing
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			ds.dataMu.RLock()
			count := len(ds.Records)
			ds.dataMu.RUnlock()
			if count == expected {
				return // Success
			}
			time.Sleep(10 * time.Millisecond)
		}

		ds.dataMu.RLock()
		finalCount := len(ds.Records)
		ds.dataMu.RUnlock()
		require.Equal(t, expected, finalCount, "Data loss detected in async pipeline")
	})
}
