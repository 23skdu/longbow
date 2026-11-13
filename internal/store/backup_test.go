package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func makeBackupTestBatch(mem memory.Allocator, startID int64, count int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	idB := b.Field(0).(*array.Int64Builder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	for i := 0; i < count; i++ {
		idB.Append(startID + int64(i))
		vecB.Append(true)
		for d := 0; d < 4; d++ {
			valB.Append(float32(i+d) * 0.1)
		}
	}
	return b.NewRecordBatch()
}

func TestOnlineBackupVerifyRestoreAndPITR(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewGoAllocator()
	s := NewVectorStore(mem, zerolog.Nop(), 1<<28, 0, 0)
	defer func() { _ = s.Close() }()

	backupDir := t.TempDir()
	_, err := s.CreateBackup(BackupConfig{
		BackupDir:      backupDir,
		RetentionDays:  7,
		RetentionCount: 3,
	})
	require.NoError(t, err)

	// Populate baseline dataset with 3 rows
	batch1 := makeBackupTestBatch(mem, 1, 3)
	defer batch1.Release()
	require.NoError(t, s.applyReplayBatch("docs", batch1, 10, time.Now().UnixNano()))
	s.sequence.Store(10)

	// 1. Trigger online backup to local directory destination
	meta, err := s.CreateOnlineBackup(ctx, "docs", backupDir)
	require.NoError(t, err)
	require.Equal(t, int64(3), meta.RowCount)
	require.Equal(t, uint64(10), meta.WALSequence)
	require.FileExists(t, filepath.Join(backupDir, meta.BackupID+".tar.gz"))

	// 2. Crash consistency / corruption detection test
	s.backupManager.mu.Lock()
	corruptCopy := append([]byte(nil), s.backupManager.data[meta.BackupID]...)
	corruptCopy[len(corruptCopy)/2] ^= 0xff
	s.backupManager.data["corrupt-id"] = corruptCopy
	s.backupManager.backups["corrupt-id"] = meta
	s.backupManager.mu.Unlock()

	_, err = s.RestoreWithPITR(ctx, PITRConfig{
		BackupID:    "corrupt-id",
		DatasetName: "docs_corrupt",
	})
	require.Error(t, err)
	_, exists := s.getDataset("docs_corrupt")
	require.False(t, exists, "failed corrupt restore must not leave partial dataset")

	// 3. Write WAL entries after snapshot seq=10 with different timestamps for PITR cutoff
	walDir := t.TempDir()
	wal := storage.NewWAL(walDir)
	t1 := time.Now().Add(10 * time.Second)
	t2 := time.Now().Add(20 * time.Second)
	bSeq11 := makeBackupTestBatch(mem, 100, 2)
	bSeq12 := makeBackupTestBatch(mem, 200, 2)
	defer bSeq11.Release()
	defer bSeq12.Release()
	require.NoError(t, wal.Write("docs_pitr", 11, t1.UnixNano(), bSeq11))
	require.NoError(t, wal.Write("docs_pitr", 12, t2.UnixNano(), bSeq12))
	require.NoError(t, wal.Close())

	// Restore baseline into docs_pitr up to t1 (seq 11 included, seq 12 excluded!)
	report, err := s.RestoreWithPITR(ctx, PITRConfig{
		BackupID:    meta.BackupID,
		DatasetName: "docs_pitr",
		WALDir:      walDir,
		TargetTime:  t1,
	})
	require.NoError(t, err)
	require.True(t, report.ChecksumsVerified)
	require.Equal(t, int64(3), report.RestoredRows)
	require.Equal(t, 1, report.WALEntriesApplied)
	require.Equal(t, uint64(11), report.FinalWALSequence)

	restoredDS, ok := s.getDataset("docs_pitr")
	require.True(t, ok)
	require.Equal(t, int64(5), restoredDS.TotalIngestedRows.Load())

	// 4. DoAction TriggerBackup / ListBackups / VerifyBackup dispatch
	stream := &captureActionStream{ctx: ctx}
	reqBody, _ := json.Marshal(map[string]string{"dataset": "docs"})
	require.NoError(t, s.DoAction(&flight.Action{Type: "TriggerBackup", Body: reqBody}, stream))
	require.Len(t, stream.results, 1)

	verifyStream := &captureActionStream{ctx: ctx}
	vBody, _ := json.Marshal(map[string]string{"backup_id": meta.BackupID, "dataset": "docs"})
	require.NoError(t, s.DoAction(&flight.Action{Type: "VerifyBackup", Body: vBody}, verifyStream))
	require.Contains(t, string(verifyStream.results[0].Body), `"checksums_verified":true`)
}

func FuzzSnapshotArchiveParse(f *testing.F) {
	f.Add([]byte("not-a-valid-tar-gz"))
	f.Add([]byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = ParseAndVerifySnapshotArchive(data)
	})
}
