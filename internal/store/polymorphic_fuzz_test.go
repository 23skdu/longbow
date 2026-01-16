package store

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func FuzzPolymorphicIngestion(f *testing.F) {
	// Seed corpus
	f.Add(int64(12345), uint8(FuzzTypeFloat32), int(128))
	f.Add(int64(time.Now().UnixNano()), uint8(FuzzTypeFloat16), int(128))
	// Add other types
	f.Add(int64(11111), uint8(FuzzTypeComplex64), int(64))

	f.Fuzz(func(t *testing.T, seed int64, typeByte uint8, dims int) {
		rng := rand.New(rand.NewSource(seed))
		vecType := int(typeByte) % 4 // 4 types defined
		// sanitize dims
		if dims < 1 {
			dims = 1
		}
		if dims > 512 {
			dims = 512
		}
		// Ensure even for complex?
		// Logic handles strict dims * 2, so input dims is "vector logic dim".

		mem := memory.NewGoAllocator()

		// Setup temp dir for persistence
		tmpDir, err := os.MkdirTemp("", "fuzz_poly_store_*")
		if err != nil {
			t.Skip("failed to create temp dir")
		}
		defer func() { _ = os.RemoveAll(tmpDir) }()

		// 1. Initialize Store
		logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()
		store := NewVectorStore(mem, logger, 50*1024*1024, 0, 0)
		defer func() { _ = store.Close() }()

		// Init persistence to ensure we can persist
		cfg := storage.StorageConfig{
			DataPath: tmpDir,
			// WalEnabled Removed as it is not in config struct
			SnapshotInterval: 0, // Manual snapshots
		}
		err = store.InitPersistence(cfg)
		if err != nil {
			t.Skipf("failed to init persistence: %v", err)
		}

		// 2. Generate and Ingest Batch
		numRows := rng.Intn(50) + 1
		batch := GenerateRandomPolymorphicBatch(mem, rng, vecType, dims, numRows)
		defer batch.Release()

		dsName := "fuzz_ds"

		// Create dataset
		ds, _ := store.getOrCreateDataset(dsName, func() *Dataset {
			return NewDataset(dsName, batch.Schema())
		})

		// Ingest
		// 1. Storage
		batch.Retain()
		ds.dataMu.Lock()
		ds.Records = append(ds.Records, batch)
		batchIdx := len(ds.Records) - 1
		ds.dataMu.Unlock()

		// 2. Index (HNSW)
		if ds.Index != nil {
			// AddByRecord
			// Assume rowIdx starts at 0 for this batch
			for i := 0; i < int(batch.NumRows()); i++ {
				_, err := ds.Index.AddByLocation(batchIdx, i)
				require.NoError(t, err)
			}
		}

		// 3. Search
		// Construct query
		qLen := dims
		if vecType == FuzzTypeComplex64 || vecType == FuzzTypeComplex128 {
			qLen = dims * 2 // flattened
		}
		q := make([]float32, qLen)
		for i := range q {
			q[i] = rng.Float32()
		}

		// Search
		if ds.Index != nil {
			_, err := ds.Index.SearchVectors(q, 10, nil, SearchOptions{})
			if err != nil {
				t.Logf("Search failed: %v", err)
			}
		}

		// 4. Persistence Roundtrip
		// Snapshot
		err = store.Snapshot()
		require.NoError(t, err)

		_ = store.Close()

		// Re-open
		store2 := NewVectorStore(mem, logger, 50*1024*1024, 0, 0)
		defer func() { _ = store2.Close() }()

		err = store2.InitPersistence(cfg)
		require.NoError(t, err)

		// Verify
		ds2, ok := store2.getDataset(dsName)
		if !ok {
			// It's possible dataset wasn't fully persisted if snapshot failed or timing.
			// But we required NoError on Snapshot.
			// loadSnapshotItem restores it.
			t.Fatalf("Dataset not restored")
		}

		if ds2.Index != nil {
			metrics.HNSWPolymorphicFuzzCrashRecovered.Inc()
			// Search again
			_, err = ds2.Index.SearchVectors(q, 10, nil, SearchOptions{})
			require.NoError(t, err)
		}
	})
}
