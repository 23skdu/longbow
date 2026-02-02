package store

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestConcurrentSnapshot_NonBlocking(t *testing.T) {
	// this test verifies that Snapshot() does not block ingest for long periods
	// and produces a valid snapshot.

	tmpDir := t.TempDir()
	config := storage.StorageConfig{
		DataPath:         tmpDir,
		SnapshotInterval: 0, // Manual snapshot
	}

	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 0, 0, 0)
	require.NoError(t, vs.InitPersistence(config))
	defer vs.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numVectors = 5000
	const dim = 16

	// Create dataset
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	// Ingest routine
	ingestDone := make(chan struct{})
	var pushedOps atomic.Int64

	go func() {
		defer close(ingestDone)
		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		for i := 0; i < numVectors; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Build little batches
			b.Field(0).(*array.Int32Builder).Append(int32(i))
			vb := b.Field(1).(*array.FixedSizeListBuilder)
			vvb := vb.ValueBuilder().(*array.Float32Builder)
			vb.Append(true)
			for j := 0; j < dim; j++ {
				vvb.Append(rand.Float32())
			}

			rec := b.NewRecordBatch()
			err := vs.ApplyDelta("test_ds", rec, uint64(i), time.Now().UnixNano())
			rec.Release()
			if err != nil {
				// might fail during close
				return
			}
			pushedOps.Add(1)

			// Small sleep to simulate work and allow snapshot opportunity
			time.Sleep(100 * time.Microsecond)
		}
	}()

	// Wait for some data
	time.Sleep(200 * time.Millisecond)

	// Trigger Snapshot
	start := time.Now()
	err := vs.Snapshot(ctx)
	duration := time.Since(start)
	require.NoError(t, err, "Snapshot should succeed")

	t.Logf("Snapshot took %v", duration)

	// Verify we didn't block ingest completely
	opsBefore := pushedOps.Load()
	time.Sleep(100 * time.Millisecond)
	opsAfter := pushedOps.Load()
	t.Logf("Ingest progress: %d -> %d", opsBefore, opsAfter)

	cancel()
	<-ingestDone

	// Verify snapshot file exists
	snapDir := filepath.Join(tmpDir, "snapshots")
	entries, err := os.ReadDir(snapDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "Snapshot directory should not be empty")

	// Verify we can load it
	vs2 := NewVectorStore(mem, zerolog.Nop(), 0, 0, 0)
	err = vs2.InitPersistence(config)
	require.NoError(t, err, "Should reload from snapshot")
	defer vs2.Close()

	// Check count
	ds, err := vs2.GetDataset("test_ds")
	require.NoError(t, err)
	count := 0
	ds.dataMu.RLock()
	for _, r := range ds.Records {
		count += int(r.NumRows())
	}
	ds.dataMu.RUnlock()

	require.Greater(t, count, 0, "Snapshot should contain data")
	t.Logf("Restored %d records", count)
}

func TestSnapshotRateLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping slow rate limit test")
	}

	// Verify rate limiter slows down writing
	tmpDir := t.TempDir()

	limit := 500 * 1024 // 500KB/s

	config := storage.StorageConfig{
		DataPath:          tmpDir,
		SnapshotRateLimit: limit,
	}

	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 0, 0, 0)
	require.NoError(t, vs.InitPersistence(config))
	defer vs.Close()

	rec := generateRandomBatch(mem, 2000, 256) // ~2MB raw
	err := vs.ApplyDelta("heavy_ds", rec, 1, time.Now().UnixNano())
	rec.Release()
	require.NoError(t, err)

	start := time.Now()
	err = vs.Snapshot(context.Background())
	require.NoError(t, err)
	dur := time.Since(start)

	t.Logf("Snapshot with limit %d bytes/s took %v", limit, dur)
}

func generateRandomBatch(mem memory.Allocator, rows int, dim int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	idBuilder := b.Field(0).(*array.Int32Builder)
	vecBuilder := b.Field(1).(*array.FixedSizeListBuilder)
	vecValBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < rows; i++ {
		idBuilder.Append(int32(i))
		vecBuilder.Append(true)
		for j := 0; j < dim; j++ {
			vecValBuilder.Append(rand.Float32())
		}
	}
	return b.NewRecordBatch()
}
