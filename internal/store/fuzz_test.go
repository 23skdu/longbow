package store

import (
	"math/rand"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

func FuzzIngestion(f *testing.F) {
	// Seed corpus
	f.Add(int64(12345))
	f.Add(int64(time.Now().UnixNano()))

	f.Fuzz(func(t *testing.T, seed int64) {
		rng := rand.New(rand.NewSource(seed))
		mem := memory.NewGoAllocator()

		// Use Nop logger to avoid clutter
		store := NewVectorStore(mem, zerolog.Nop(), 50*1024*1024, 0, 0)
		defer func() { _ = store.Close() }()

		schema := GenerateRandomSchema(rng)
		numRows := rng.Intn(100) + 1

		batch := GenerateRandomRecordBatch(mem, rng, schema, numRows)
		defer batch.Release()

		// Attempt ingestion
		// Currently using IndexRecordColumns logic or new DoPut logic if exposed.
		// Since DoPut is gRPC, we'll simulate internal add.
		// Dataset creation:
		dsName := "fuzz_dataset"

		// We'll use a direct internal method or public API wrapper if available.
		// Taking logic from DoPut roughly:
		ds, ok := store.getDataset(dsName)
		if !ok {
			// Create it
			ds, _ = store.getOrCreateDataset(dsName, func() *Dataset {
				return &Dataset{Name: dsName}
			})
		}

		// Just append to records directly for this test scope to verify it doesn't panic on weird data
		// But better to use AddToIndex if we had it exposed.
		// Let's use IndexRecordColumns as it parses things.
		store.IndexRecordColumns(dsName, batch, 0)

		// Also basic append safety
		ds.dataMu.Lock()
		batch.Retain()
		ds.Records = append(ds.Records, batch)
		ds.dataMu.Unlock()
	})
}

func FuzzCompaction(f *testing.F) {
	f.Add(int64(98765))

	f.Fuzz(func(t *testing.T, seed int64) {
		rng := rand.New(rand.NewSource(seed))
		mem := memory.NewGoAllocator()

		store := NewVectorStore(mem, zerolog.Nop(), 50*1024*1024, 0, 0)
		defer func() { _ = store.Close() }()

		// Ensure compaction worker is set up
		store.compactionConfig = DefaultCompactionConfig()
		store.compactionConfig.Enabled = true
		store.compactionWorker = NewCompactionWorker(store, store.compactionConfig)
		store.compactionWorker.Start()

		// Create dataset
		dsName := "fuzz_compaction"
		ds, _ := store.getOrCreateDataset(dsName, func() *Dataset {
			return &Dataset{Name: dsName}
		})

		// Add multiple small batches
		numBatches := rng.Intn(5) + 2
		schema := GenerateRandomSchema(rng) // Consistent schema for one run

		for i := 0; i < numBatches; i++ {
			rows := rng.Intn(10) + 1
			batch := GenerateRandomRecordBatch(mem, rng, schema, rows)
			ds.dataMu.Lock()
			ds.Records = append(ds.Records, batch)
			ds.dataMu.Unlock()
		}

		// Trigger compaction manually
		// note: CompactDataset is atomic swap of records.
		// It might fail if types mismatch in our random schema if we are not careful?
		// No, we use same schema for all batches in a run.

		// We need to access CompactDataset. Since it's in store package (same package), we can call it?
		// CompactDataset is likely unexported or attached to worker.
		// looking at compaction_store.go...
		// store.CompactDataset(ctx, dsName)

		_ = store.CompactDataset(dsName)

		// Assert assumption: No panic.
	})
}
