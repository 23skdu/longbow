package store

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SnapshotManifest describes the components and integrity checksums of an archive snapshot (Item 4.1).
type SnapshotManifest struct {
	FormatVersion int               `json:"format_version"`
	BackupID      string            `json:"backup_id"`
	DatasetName   string            `json:"dataset_name"`
	CreatedAt     time.Time         `json:"created_at"`
	WALSequence   uint64            `json:"wal_sequence"`
	RowCount      int64             `json:"row_count"`
	Dimension     uint32            `json:"dimension"`
	Metric        string            `json:"metric"`
	FileChecksums map[string]string `json:"file_checksums"`
}

// SnapshotDatasetMetadata holds dataset-level state persisted alongside Arrow batches.
type SnapshotDatasetMetadata struct {
	Name          string   `json:"name"`
	Version       int64    `json:"version"`
	IndexedCols   []string `json:"indexed_columns,omitempty"`
	TombstoneRows [][2]int `json:"tombstone_rows,omitempty"` // [batchIdx, rowIdx]
	IndexType     string   `json:"index_type"`
}

// PITRConfig configures Point-in-Time Recovery using a baseline snapshot + WAL replay (Item 4.4).
type PITRConfig struct {
	BackupID     string
	DatasetName  string
	WALDir       string
	TargetTime   time.Time // Replay entries up to TargetTime (inclusive); zero means unlimited
	TargetMaxSeq uint64    // Replay entries up to TargetMaxSeq (inclusive); 0 means unlimited
	VerifyOnly   bool
}

// RestoreVerificationReport summarizes integrity and consistency checks performed during restore.
type RestoreVerificationReport struct {
	BackupID          string `json:"backup_id"`
	DatasetName       string `json:"dataset_name"`
	ChecksumsVerified bool   `json:"checksums_verified"`
	RestoredRows      int64  `json:"restored_rows"`
	WALEntriesApplied int    `json:"wal_entries_applied"`
	FinalWALSequence  uint64 `json:"final_wal_sequence"`
}

var backupSeq atomic.Uint64

func nextUniqueBackupID(datasetName string) string {
	n := backupSeq.Add(1)
	safeDS := strings.ReplaceAll(strings.ReplaceAll(datasetName, "/", "_"), "..", "_")
	return fmt.Sprintf("%s_%s_%06d", safeDS, time.Now().Format("20060102150405"), n)
}

// NewSnapshotBackendForURI resolves a storage.SnapshotBackend from local directory, file://, s3://, or gs:// URI (Item 4.2).
func NewSnapshotBackendForURI(ctx context.Context, dest string) (storage.SnapshotBackend, error) {
	if dest == "" {
		return nil, errors.New("backup destination is empty")
	}
	if strings.HasPrefix(dest, "s3://") {
		u, err := url.Parse(dest)
		if err != nil {
			return nil, fmt.Errorf("invalid s3 URI: %w", err)
		}
		return storage.NewS3Backend(&storage.S3BackendConfig{
			Bucket:          u.Host,
			Prefix:          strings.TrimPrefix(u.Path, "/"),
			AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			Region:          os.Getenv("AWS_REGION"),
			Endpoint:        os.Getenv("AWS_ENDPOINT_URL"),
			UsePathStyle:    os.Getenv("AWS_ENDPOINT_URL") != "",
		})
	}
	if strings.HasPrefix(dest, "gs://") {
		u, err := url.Parse(dest)
		if err != nil {
			return nil, fmt.Errorf("invalid gs URI: %w", err)
		}
		return storage.NewGCSBackend(ctx, &storage.GCSBackendConfig{
			Bucket: u.Host,
			Prefix: strings.TrimPrefix(u.Path, "/"),
		})
	}
	dir := strings.TrimPrefix(dest, "file://")
	return storage.NewFileSnapshotBackend(dir)
}

// BuildSnapshotArchive serializes dataset records, index metadata, and dataset metadata into a checksummed tar.gz archive.
func BuildSnapshotArchive(ds *Dataset, backupID string, walSeq uint64) ([]byte, SnapshotManifest, error) {
	if ds == nil {
		return nil, SnapshotManifest{}, errors.New("dataset is required")
	}

	// Capture consistent read snapshot of batches cleanly without holding write locks across serialization.
	ds.dataMu.RLock()
	origRecs := ds.Records.Read()
	retained := make([]arrow.RecordBatch, len(origRecs))
	var totalRows int64
	for i, r := range origRecs {
		r.Retain()
		retained[i] = r
		totalRows += r.NumRows()
	}
	schema := ds.Schema
	version := ds.Version
	metric := fmt.Sprintf("%v", ds.Metric)
	var dim uint32
	if ds.Index != nil {
		dim = ds.Index.GetDimension()
	}
	ds.metadataMu.Lock()
	var tombstones [][2]int
	for batchIdx, bs := range ds.Tombstones {
		if bs == nil {
			continue
		}
		// Bounded row iteration using batch row count if available
		maxRows := 0
		if batchIdx >= 0 && batchIdx < len(retained) {
			maxRows = int(retained[batchIdx].NumRows())
		}
		for rowIdx := 0; rowIdx < maxRows; rowIdx++ {
			if bs.Contains(rowIdx) {
				tombstones = append(tombstones, [2]int{batchIdx, rowIdx})
			}
		}
	}
	ds.metadataMu.Unlock()
	ds.dataMu.RUnlock()

	defer func() {
		for _, r := range retained {
			r.Release()
		}
	}()

	var arrowBuf bytes.Buffer
	if len(retained) > 0 && schema != nil {
		w := ipc.NewWriter(&arrowBuf, ipc.WithSchema(schema))
		for _, rec := range retained {
			if err := w.Write(rec); err != nil {
				_ = w.Close()
				return nil, SnapshotManifest{}, fmt.Errorf("backup arrow encode failed: %w", err)
			}
		}
		if err := w.Close(); err != nil {
			return nil, SnapshotManifest{}, fmt.Errorf("backup arrow close failed: %w", err)
		}
	}

	metaObj := SnapshotDatasetMetadata{
		Name:          ds.Name,
		Version:       version,
		TombstoneRows: tombstones,
		IndexType:     "hnsw",
	}
	metaBytes, err := json.Marshal(metaObj)
	if err != nil {
		return nil, SnapshotManifest{}, err
	}

	idxConfigBytes, err := json.Marshal(map[string]any{
		"dimension": dim,
		"metric":    metric,
		"rows":      totalRows,
	})
	if err != nil {
		return nil, SnapshotManifest{}, err
	}

	files := map[string][]byte{
		"records.arrow": arrowBuf.Bytes(),
		"metadata.json": metaBytes,
		"index.json":    idxConfigBytes,
	}

	checksums := make(map[string]string, len(files))
	for name, content := range files {
		sum := sha256.Sum256(content)
		checksums[name] = hex.EncodeToString(sum[:])
	}

	manifest := SnapshotManifest{
		FormatVersion: 1,
		BackupID:      backupID,
		DatasetName:   ds.Name,
		CreatedAt:     time.Now().UTC(),
		WALSequence:   walSeq,
		RowCount:      totalRows,
		Dimension:     dim,
		Metric:        metric,
		FileChecksums: checksums,
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, SnapshotManifest{}, err
	}

	var out bytes.Buffer
	gzw := gzip.NewWriter(&out)
	tw := tar.NewWriter(gzw)

	order := []string{"manifest.json", "records.arrow", "metadata.json", "index.json"}
	allEntries := map[string][]byte{
		"manifest.json": manifestBytes,
		"records.arrow": files["records.arrow"],
		"metadata.json": files["metadata.json"],
		"index.json":    files["index.json"],
	}
	for _, name := range order {
		payload := allEntries[name]
		hdr := &tar.Header{
			Name: name,
			Mode: 0o600,
			Size: int64(len(payload)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			_ = tw.Close()
			_ = gzw.Close()
			return nil, SnapshotManifest{}, err
		}
		if _, err := tw.Write(payload); err != nil {
			_ = tw.Close()
			_ = gzw.Close()
			return nil, SnapshotManifest{}, err
		}
	}
	if err := tw.Close(); err != nil {
		_ = gzw.Close()
		return nil, SnapshotManifest{}, err
	}
	if err := gzw.Close(); err != nil {
		return nil, SnapshotManifest{}, err
	}
	return out.Bytes(), manifest, nil
}

// ParseAndVerifySnapshotArchive extracts a tar.gz snapshot archive and verifies every component SHA-256 checksum.
func ParseAndVerifySnapshotArchive(archiveBytes []byte) (SnapshotManifest, map[string][]byte, error) {
	if len(archiveBytes) == 0 {
		return SnapshotManifest{}, nil, errors.New("empty snapshot archive")
	}
	gzr, err := gzip.NewReader(bytes.NewReader(archiveBytes))
	if err != nil {
		return SnapshotManifest{}, nil, fmt.Errorf("invalid gzip snapshot header: %w", err)
	}
	defer func() { _ = gzr.Close() }()

	tr := tar.NewReader(gzr)
	files := make(map[string][]byte)
	const maxComponentSize = 512 << 20
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return SnapshotManifest{}, nil, fmt.Errorf("corrupt snapshot tar entry: %w", err)
		}
		if hdr.Size < 0 || hdr.Size > maxComponentSize {
			return SnapshotManifest{}, nil, fmt.Errorf("snapshot component %s exceeds limit", hdr.Name)
		}
		cleanName := filepath.Base(hdr.Name)
		data, err := io.ReadAll(io.LimitReader(tr, maxComponentSize))
		if err != nil {
			return SnapshotManifest{}, nil, fmt.Errorf("failed reading component %s: %w", cleanName, err)
		}
		files[cleanName] = data
	}

	manifestRaw, ok := files["manifest.json"]
	if !ok {
		return SnapshotManifest{}, nil, errors.New("snapshot missing manifest.json")
	}
	var manifest SnapshotManifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		return SnapshotManifest{}, nil, fmt.Errorf("invalid snapshot manifest: %w", err)
	}
	if manifest.FormatVersion != 1 {
		return SnapshotManifest{}, nil, fmt.Errorf("unsupported snapshot format version: %d", manifest.FormatVersion)
	}
	for fname, expectedSum := range manifest.FileChecksums {
		content, exists := files[fname]
		if !exists {
			return SnapshotManifest{}, nil, fmt.Errorf("snapshot missing component %s", fname)
		}
		sum := sha256.Sum256(content)
		actual := hex.EncodeToString(sum[:])
		if actual != expectedSum {
			return SnapshotManifest{}, nil, fmt.Errorf("checksum mismatch for %s: expected %s got %s", fname, expectedSum, actual)
		}
	}
	return manifest, files, nil
}

// CreateOnlineBackup captures an online archive snapshot and persists it both in BackupManager and optional backend (local/S3/GCS).
func (vs *VectorStore) CreateOnlineBackup(ctx context.Context, datasetName, destination string) (BackupMetadata, error) {
	if vs.backupManager == nil {
		dir := destination
		if dir == "" || strings.HasPrefix(dir, "s3://") || strings.HasPrefix(dir, "gs://") {
			dir = filepath.Join(os.TempDir(), "longbow-backups")
		}
		if _, err := vs.CreateBackup(BackupConfig{BackupDir: dir, Destination: destination, RetentionDays: 30}); err != nil {
			return BackupMetadata{}, err
		}
	}

	ds, ok := vs.getDataset(datasetName)
	if !ok {
		return BackupMetadata{}, fmt.Errorf("dataset %s not found", datasetName)
	}

	backupID := nextUniqueBackupID(datasetName)
	walSeq := vs.sequence.Load()
	archiveBytes, manifest, err := BuildSnapshotArchive(ds, backupID, walSeq)
	if err != nil {
		return BackupMetadata{}, err
	}

	hasher := sha256.Sum256(archiveBytes)
	checksum := hex.EncodeToString(hasher[:])

	meta := BackupMetadata{
		BackupID:    backupID,
		DatasetName: datasetName,
		Timestamp:   manifest.CreatedAt,
		SizeBytes:   int64(len(archiveBytes)),
		Type:        "full",
		Checksum:    checksum,
		Version:     manifest.FormatVersion,
		WALSequence: walSeq,
		RowCount:    manifest.RowCount,
		Destination: destination,
	}

	vs.backupManager.mu.Lock()
	vs.backupManager.backups[backupID] = meta
	vs.backupManager.data[backupID] = archiveBytes
	vs.backupManager.mu.Unlock()

	if destination != "" {
		backend, err := NewSnapshotBackendForURI(ctx, destination)
		if err != nil {
			return BackupMetadata{}, err
		}
		if err := backend.WriteSnapshotFile(ctx, backupID, ".tar.gz", bytes.NewReader(archiveBytes)); err != nil {
			return BackupMetadata{}, fmt.Errorf("failed writing backup to %s: %w", destination, err)
		}
	} else if vs.backupManager.backupDir != "" {
		backend, err := storage.NewFileSnapshotBackend(vs.backupManager.backupDir)
		if err == nil {
			_ = backend.WriteSnapshotFile(ctx, backupID, ".tar.gz", bytes.NewReader(archiveBytes))
		}
	}

	vs.applyRetentionCountLocked()
	return meta, nil
}

func (vs *VectorStore) applyRetentionCountLocked() {
	if vs.backupManager == nil || vs.backupManager.config.RetentionCount <= 0 {
		return
	}
	limit := vs.backupManager.config.RetentionCount
	vs.backupManager.mu.Lock()
	defer vs.backupManager.mu.Unlock()
	if len(vs.backupManager.backups) <= limit {
		return
	}
	list := make([]BackupMetadata, 0, len(vs.backupManager.backups))
	for _, m := range vs.backupManager.backups {
		list = append(list, m)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Timestamp.After(list[j].Timestamp)
	})
	for _, old := range list[limit:] {
		delete(vs.backupManager.backups, old.BackupID)
		delete(vs.backupManager.data, old.BackupID)
	}
}

// RestoreWithPITR restores a dataset from an online snapshot archive and optionally replays WAL entries up to target time/sequence (Item 4.3 & 4.4).
func (vs *VectorStore) RestoreWithPITR(ctx context.Context, cfg PITRConfig) (RestoreVerificationReport, error) {
	var report RestoreVerificationReport
	report.BackupID = cfg.BackupID
	report.DatasetName = cfg.DatasetName

	if vs.backupManager == nil {
		return report, errors.New("backup manager not configured")
	}

	archiveBytes, err := vs.backupManager.Restore(RestoreConfig{
		BackupID:    cfg.BackupID,
		DatasetName: cfg.DatasetName,
	})
	if err != nil {
		return report, err
	}

	// Verify whether this backup uses the tar.gz SnapshotManifest format or legacy raw IPC bytes.
	if len(archiveBytes) >= 2 && archiveBytes[0] == 0x1f && archiveBytes[1] == 0x8b {
		manifest, files, err := ParseAndVerifySnapshotArchive(archiveBytes)
		if err != nil {
			return report, err
		}
		report.ChecksumsVerified = true
		report.FinalWALSequence = manifest.WALSequence

		// Validate Arrow IPC stream BEFORE mutating store state (atomic consistency check)
		arrowData := files["records.arrow"]
		var restoredBatches []arrow.RecordBatch
		var restoredSchema *arrow.Schema
		if len(arrowData) > 0 {
			r, err := ipc.NewReader(bytes.NewReader(arrowData), ipc.WithAllocator(vs.mem))
			if err != nil {
				return report, fmt.Errorf("snapshot arrow reader error: %w", err)
			}
			restoredSchema = r.Schema()
			for r.Next() {
				rec := r.RecordBatch()
				rec.Retain()
				restoredBatches = append(restoredBatches, rec)
				report.RestoredRows += rec.NumRows()
			}
			if err := r.Err(); err != nil {
				for _, b := range restoredBatches {
					b.Release()
				}
				r.Release()
				return report, fmt.Errorf("corrupt arrow batch in snapshot: %w", err)
			}
			r.Release()
		}

		var dsMeta SnapshotDatasetMetadata
		if metaRaw, ok := files["metadata.json"]; ok {
			_ = json.Unmarshal(metaRaw, &dsMeta)
		}

		if cfg.VerifyOnly {
			for _, b := range restoredBatches {
				b.Release()
			}
			return report, nil
		}

		dsName := cfg.DatasetName
		if dsName == "" {
			dsName = manifest.DatasetName
		}
		report.DatasetName = dsName

		ds, _ := vs.getOrCreateDataset(dsName, func() *Dataset {
			if restoredSchema != nil {
				return NewDataset(dsName, restoredSchema)
			}
			return NewDataset(dsName, arrow.NewSchema(nil, nil))
		})
		for _, rec := range restoredBatches {
			if err := vs.applyBatchToMemory(ds, rec, manifest.CreatedAt.UnixNano()); err != nil {
				for _, rem := range restoredBatches {
					rem.Release()
				}
				return report, err
			}
			rec.Release()
		}

		// Optional WAL replay for Point-in-Time Recovery after snapshot sequence
		if cfg.WALDir != "" {
			applied, maxSeq, err := vs.replayWALRange(ctx, cfg.WALDir, dsName, manifest.WALSequence, cfg.TargetTime, cfg.TargetMaxSeq)
			if err != nil {
				return report, fmt.Errorf("PITR WAL replay failed: %w", err)
			}
			report.WALEntriesApplied = applied
			if maxSeq > report.FinalWALSequence {
				report.FinalWALSequence = maxSeq
			}
		}
		return report, nil
	}

	// Legacy raw Arrow IPC restore path
	if err := vs.RestoreFromBackup(cfg.BackupID, cfg.DatasetName); err != nil {
		return report, err
	}
	report.ChecksumsVerified = true
	return report, nil
}

func (vs *VectorStore) replayWALRange(ctx context.Context, walDir, targetDataset string, minSeqExclusive uint64, targetTime time.Time, maxSeq uint64) (int, uint64, error) {
	it, err := storage.NewWALIterator(walDir, vs.mem)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, minSeqExclusive, nil
		}
		return 0, minSeqExclusive, err
	}
	defer func() { _ = it.Close() }()

	if minSeqExclusive > 0 {
		if err := it.Seek(minSeqExclusive); err != nil && !errors.Is(err, io.EOF) {
			return 0, minSeqExclusive, err
		}
	}

	applied := 0
	highestSeq := minSeqExclusive
	for {
		select {
		case <-ctx.Done():
			return applied, highestSeq, ctx.Err()
		default:
		}
		seq, ts, name, rec, err := it.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return applied, highestSeq, err
		}
		if seq <= minSeqExclusive {
			if rec != nil {
				rec.Release()
			}
			continue
		}
		if maxSeq > 0 && seq > maxSeq {
			if rec != nil {
				rec.Release()
			}
			break
		}
		if !targetTime.IsZero() && ts > targetTime.UnixNano() {
			if rec != nil {
				rec.Release()
			}
			continue
		}
		if targetDataset != "" && name != targetDataset {
			if rec != nil {
				rec.Release()
			}
			continue
		}
		if rec != nil {
			if err := vs.applyReplayBatch(name, rec, seq, ts); err != nil {
				rec.Release()
				return applied, highestSeq, err
			}
			rec.Release()
			applied++
			if seq > highestSeq {
				highestSeq = seq
			}
		}
	}
	return applied, highestSeq, nil
}

// StartBackupScheduler starts periodic automated backups and retention pruning for the configured interval (Item 4.5).
func (vs *VectorStore) StartBackupScheduler(ctx context.Context, destination string) {
	interval := vs.backupScheduleInterval
	if interval <= 0 {
		return
	}
	vs.workerWg.Add(1)
	go func() {
		defer vs.workerWg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-vs.stopChan:
				return
			case <-ticker.C:
				for _, dsName := range vs.GetActiveDatasets() {
					_, _ = vs.CreateOnlineBackup(ctx, dsName, destination)
				}
				if vs.backupManager != nil {
					_, _ = vs.backupManager.ApplyRetentionPolicy()
				}
			}
		}
	}()
}

func (vs *VectorStore) handleBackupAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	ctx := stream.Context()
	switch action.Type {
	case "TriggerBackup":
		var req struct {
			Dataset     string `json:"dataset"`
			Destination string `json:"destination"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		meta, err := vs.CreateOnlineBackup(ctx, req.Dataset, req.Destination)
		if err != nil {
			return true, status.Errorf(codes.Internal, "backup failed: %v", err)
		}
		body, _ := json.Marshal(meta)
		return true, stream.Send(&flight.Result{Body: body})

	case "RestoreBackup":
		var req struct {
			BackupID      string `json:"backup_id"`
			Dataset       string `json:"dataset"`
			WALDir        string `json:"wal_dir"`
			PITRTimestamp int64  `json:"pitr_timestamp"`
			MaxSeq        uint64 `json:"max_seq"`
			VerifyOnly    bool   `json:"verify_only"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		var targetTime time.Time
		if req.PITRTimestamp > 0 {
			targetTime = time.Unix(0, req.PITRTimestamp)
		}
		report, err := vs.RestoreWithPITR(ctx, PITRConfig{
			BackupID:     req.BackupID,
			DatasetName:  req.Dataset,
			WALDir:       req.WALDir,
			TargetTime:   targetTime,
			TargetMaxSeq: req.MaxSeq,
			VerifyOnly:   req.VerifyOnly,
		})
		if err != nil {
			return true, status.Errorf(codes.Internal, "restore failed: %v", err)
		}
		body, _ := json.Marshal(report)
		return true, stream.Send(&flight.Result{Body: body})

	case "ListBackups":
		var req struct {
			Dataset string `json:"dataset"`
		}
		_ = json.Unmarshal(action.Body, &req)
		if vs.backupManager == nil {
			return true, stream.Send(&flight.Result{Body: []byte(`{"backups":[]}`)})
		}
		backups := vs.backupManager.ListBackups(req.Dataset)
		body, _ := json.Marshal(map[string]any{"backups": backups})
		return true, stream.Send(&flight.Result{Body: body})

	case "VerifyBackup":
		var req struct {
			BackupID string `json:"backup_id"`
			Dataset  string `json:"dataset"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		report, err := vs.RestoreWithPITR(ctx, PITRConfig{
			BackupID:    req.BackupID,
			DatasetName: req.Dataset,
			VerifyOnly:  true,
		})
		if err != nil {
			return true, status.Errorf(codes.DataLoss, "backup verification failed: %v", err)
		}
		body, _ := json.Marshal(report)
		return true, stream.Send(&flight.Result{Body: body})
	}
	return false, nil
}

// Unused import safety helper for memory.NewGoAllocator if needed in tests
var _ = memory.NewGoAllocator
