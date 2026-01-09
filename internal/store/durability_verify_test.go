package store

import (
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Helper to create schema with vector
func createDurabilityTestSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "val", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
	}, nil)
}

// Helper to create batch
func createDurabilityTestBatch(mem memory.Allocator, startID, count int) arrow.RecordBatch {
	schema := createDurabilityTestSchema()
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	for i := 0; i < count; i++ {
		b.Field(0).(*array.Uint32Builder).Append(uint32(startID + i))
		b.Field(1).(*array.Int64Builder).Append(int64((startID + i) * 100))

		listB := b.Field(2).(*array.FixedSizeListBuilder)
		valB := listB.ValueBuilder().(*array.Float32Builder)
		listB.Append(true)
		valB.AppendValues([]float32{float32(startID + i), float32(startID + i)}, nil)
	}
	return b.NewRecordBatch()
}

// TestDurability_EndToEnd verifies that data persists across store restarts via WAL replay.
func TestDurability_EndToEnd(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()

	// 1. Initialize Store 1
	store1 := NewVectorStore(mem, logger, 1<<30, 1<<30, 0)
	cfg1 := storage.StorageConfig{
		DataPath:         tmpDir,
		SnapshotInterval: 1 * time.Hour,
	}
	require.NoError(t, store1.InitPersistence(cfg1))

	// 2. Insert Data
	rec1 := createDurabilityTestBatch(mem, 0, 10)
	defer rec1.Release()

	require.NoError(t, store1.writeToWAL(rec1, "test_ds", time.Now().UnixNano()))
	require.NoError(t, store1.ApplyDelta("test_ds", rec1, 1, time.Now().UnixNano()))

	// Force WAL Sync
	require.NoError(t, store1.FlushWAL())
	require.NoError(t, store1.ClosePersistence())

	// 3. Recover Store 2
	store2 := NewVectorStore(mem, logger, 1<<30, 1<<30, 0)
	require.NoError(t, store2.InitPersistence(cfg1))
	defer func() { _ = store2.ClosePersistence() }()

	// 4. Verify
	ds, err := store2.GetDataset("test_ds")
	require.NoError(t, err)
	require.NotNil(t, ds)
	require.Equal(t, 10, ds.IndexLen())
	require.Len(t, ds.Records, 1)
	require.Equal(t, int64(10), ds.Records[0].NumRows())

	idArr := ds.Records[0].Column(0).(*array.Uint32)
	require.Equal(t, uint32(0), idArr.Value(0))
	require.Equal(t, uint32(9), idArr.Value(9))
}

// TestDurability_SnapshotAndWAL verifies recovery from a Snapshot + Subsequent WAL entries.
func TestDurability_SnapshotAndWAL(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()

	store1 := NewVectorStore(mem, logger, 1<<30, 1<<30, 0)
	cfg1 := storage.StorageConfig{
		DataPath:         tmpDir,
		SnapshotInterval: 1 * time.Hour,
	}
	require.NoError(t, store1.InitPersistence(cfg1))

	// 2. Insert Batch A
	recA := createDurabilityTestBatch(mem, 0, 5)
	defer recA.Release()
	require.NoError(t, store1.writeToWAL(recA, "ds_mixed", time.Now().UnixNano()))
	require.NoError(t, store1.ApplyDelta("ds_mixed", recA, 1, time.Now().UnixNano()))

	// 3. Trigger Snapshot
	require.NoError(t, store1.Snapshot())

	// 4. Insert Batch B
	recB := createDurabilityTestBatch(mem, 5, 5)
	defer recB.Release()
	require.NoError(t, store1.writeToWAL(recB, "ds_mixed", time.Now().UnixNano()))
	require.NoError(t, store1.ApplyDelta("ds_mixed", recB, 2, time.Now().UnixNano()))

	require.NoError(t, store1.FlushWAL())
	_ = store1.ClosePersistence()

	// 5. Recover Store 2
	store2 := NewVectorStore(mem, logger, 1<<30, 1<<30, 0)
	require.NoError(t, store2.InitPersistence(cfg1))
	defer func() { _ = store2.ClosePersistence() }()

	// 6. Verify
	ds, err := store2.GetDataset("ds_mixed")
	require.NoError(t, err)

	totalRows := 0
	for _, r := range ds.Records {
		totalRows += int(r.NumRows())
	}
	require.Equal(t, 10, totalRows)
	require.Equal(t, 10, ds.IndexLen())
}

// TestDurability_IndexRebuild verifies that the vector index is searchable after recovery.
func TestDurability_IndexRebuild(t *testing.T) {
	// Re-uses logic from above but specifically runs a search
	tmpDir := t.TempDir()
	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()

	store1 := NewVectorStore(mem, logger, 1<<30, 1<<30, 0)
	cfg1 := storage.StorageConfig{DataPath: tmpDir}
	require.NoError(t, store1.InitPersistence(cfg1))

	// Vector Schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	// Insert [1.0, 0.0] -> ID 0
	// Insert [0.0, 1.0] -> ID 1
	listB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	b.Field(0).(*array.Uint32Builder).Append(0)
	listB.Append(true)
	valB.AppendValues([]float32{1.0, 0.0}, nil)

	b.Field(0).(*array.Uint32Builder).Append(1)
	listB.Append(true)
	valB.AppendValues([]float32{0.0, 1.0}, nil)

	rec := b.NewRecordBatch()
	defer rec.Release()

	require.NoError(t, store1.writeToWAL(rec, "ds_vec", time.Now().UnixNano()))
	require.NoError(t, store1.ApplyDelta("ds_vec", rec, 1, time.Now().UnixNano()))
	require.NoError(t, store1.FlushWAL())
	_ = store1.ClosePersistence()

	// Recover
	store2 := NewVectorStore(mem, logger, 1<<30, 1<<30, 0)
	require.NoError(t, store2.InitPersistence(cfg1))
	defer func() { _ = store2.ClosePersistence() }()

	// Search
	// Query [1.0, 0.0] should confirm ID 0 is top match
	// We need a way to invoke search. Store doesn't expose Search directly easily without Coordinator?
	// We can get the dataset's index.
	ds, _ := store2.GetDataset("ds_vec")
	require.NotNil(t, ds)
	require.NotNil(t, ds.Index)

	// Use internal index search for verification
	// We need the query vector wrapper/interface.
	// But `VectorIndex` interface has `Search`.
	// We need `query.VectorSearchRequest` or similar? No, HNSW Search takes `[]float32`.
	// Let's assume ArrowHNSW has Search(query []float32, k int).
	// Actually VectorIndex interface: Search(query []float32, k int, ef int, bitset *query.Bitset)

	// Check store/index_build.go or similar for signature.
	// VectorIndex interface: Search(query []float32, k int, ef int, filter *query.Bitset) ([]Candidate, error)

	qVec := []float32{1.0, 0.0}
	results, err := ds.Index.SearchVectors(qVec, 1, nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, uint32(0), uint32(results[0].ID))
	// Dist should be close to 0 (Squared Euclidean: (1-1)^2 + (0-0)^2 = 0)
	require.InDelta(t, 0.0, results[0].Score, 0.0001)
}

// Verify cleaning up of WAL files after snapshot? (Optional advanced)
func TestDurability_WALTruncation(t *testing.T) {
	// If we snapshot, the WAL *should* be technically truncatable or archived.
	// StorageEngine handles this.
	// We verify that after snapshot, we don't replay the old WAL entries duplicatively
	// (or if we do, idempotency handles it).
	// Actually, InitPersistence calls `ReplayWAL`. If WAL isn't truncated, we replay everything.
	// `ApplyDelta` is idempotent for Graph, but for Records?
	// `ds.Records = append(...)`. It's NOT idempotent!
	// If we replay WAL entries that are already in Snapshot, we get DUPLICATES!
	// This test verifies that StorageEngine correctly handles cutover/truncation.

	tmpDir := t.TempDir()
	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()

	store1 := NewVectorStore(mem, logger, 1<<30, 1<<30, 0)
	cfg1 := storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: 100 * time.Hour} // Manual snapshot
	require.NoError(t, store1.InitPersistence(cfg1))

	rec := createDurabilityTestBatch(mem, 100, 1) // 1 record
	defer rec.Release()

	require.NoError(t, store1.writeToWAL(rec, "ds_dup", time.Now().UnixNano()))
	require.NoError(t, store1.ApplyDelta("ds_dup", rec, 1, 0))

	// Snapshot
	require.NoError(t, store1.Snapshot())

	// Wait for any async truncate if it exists (StorageEngine might do it inline in Snapshot though)

	_ = store1.ClosePersistence()

	// Recover
	store2 := NewVectorStore(mem, logger, 1<<30, 1<<30, 0)
	require.NoError(t, store2.InitPersistence(cfg1))
	defer func() { _ = store2.ClosePersistence() }()

	ds, _ := store2.GetDataset("ds_dup")
	// If WAL wasn't truncated/checkpointed, we'd have 1 (Snapshot) + 1 (WAL) = 2 records?
	// Or StorageEngine ReplayWAL is smart enough to start after snapshot seq?

	// Current implementation expectation:
	// Snapshotting should advance the "start sequence" for WAL replay or truncate the WAL.
	// If this fails, we have a duplication bug.

	count := 0
	for _, r := range ds.Records {
		count += int(r.NumRows())
	}
	require.Equal(t, 1, count, "Should not duplicate records on recovery after snapshot")
}
