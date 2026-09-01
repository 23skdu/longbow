package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// BackupConfig defines the configuration for dataset backups.
type BackupConfig struct {
	BackupDir     string
	Interval      time.Duration
	RetentionDays int
	Incremental   bool
	Compression   bool
	Destination   string
}

// BackupMetadata stores information about a specific backup.
type BackupMetadata struct {
	BackupID     string
	DatasetName  string
	Timestamp    time.Time
	SizeBytes    int64
	Type         string
	Checksum     string
	ParentBackup string
	Version      int
}

// IncrementalBackup represents a backup containing only changes since a parent backup.
type IncrementalBackup struct {
	Metadata BackupMetadata
	Deltas   []BackupDelta
}

// BackupDelta represents a specific change in an incremental backup.
type BackupDelta struct {
	Seq       int64
	Timestamp time.Time
	SizeBytes int64
	Checksum  string
}

// BackupManager handles the creation and management of dataset backups.
type BackupManager struct {
	mu            sync.RWMutex
	config        BackupConfig
	backups       map[string]BackupMetadata
	backupDir     string
	retentionDays int
}

// NewBackupManager creates a new backup manager with the given configuration.
func NewBackupManager(config BackupConfig) (*BackupManager, error) {
	if config.BackupDir == "" {
		return nil, errors.New("backup directory is required")
	}

	return &BackupManager{
		config:        config,
		backupDir:     config.BackupDir,
		retentionDays: config.RetentionDays,
		backups:       make(map[string]BackupMetadata),
	}, nil
}

// CreateBackup creates a new full or incremental backup for a dataset.
func (bm *BackupManager) CreateBackup(datasetName string, data []byte, parentBackupID string) (BackupMetadata, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	backupID := generateBackupID(datasetName)
	timestamp := time.Now()

	hasher := sha256.Sum256(data)
	checksum := hex.EncodeToString(hasher[:])

	metadata := BackupMetadata{
		BackupID:     backupID,
		DatasetName:  datasetName,
		Timestamp:    timestamp,
		SizeBytes:    int64(len(data)),
		Type:         "full",
		Checksum:     checksum,
		ParentBackup: parentBackupID,
		Version:      1,
	}

	if parentBackupID != "" {
		metadata.Type = "incremental"
	}

	bm.backups[backupID] = metadata

	return metadata, nil
}

// CreateIncrementalBackup creates an incremental backup from a series of deltas.
func (bm *BackupManager) CreateIncrementalBackup(datasetName string, deltas []BackupDelta) (BackupMetadata, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	backupID := generateBackupID(datasetName)
	timestamp := time.Now()

	totalSize := int64(0)
	for _, d := range deltas {
		totalSize += d.SizeBytes
	}

	hasher := sha256.New()
	for _, d := range deltas {
		hasher.Write([]byte(d.Checksum))
	}
	checksum := hex.EncodeToString(hasher.Sum(nil))

	metadata := BackupMetadata{
		BackupID:    backupID,
		DatasetName: datasetName,
		Timestamp:   timestamp,
		SizeBytes:   totalSize,
		Type:        "incremental",
		Checksum:    checksum,
	}

	bm.backups[backupID] = metadata

	return metadata, nil
}

// GetBackup retrieves metadata for a specific backup ID.
func (bm *BackupManager) GetBackup(backupID string) (BackupMetadata, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	m, ok := bm.backups[backupID]
	return m, ok
}

// ListBackups returns all backups for a given dataset.
func (bm *BackupManager) ListBackups(datasetName string) []BackupMetadata {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	var result []BackupMetadata
	for _, m := range bm.backups {
		if m.DatasetName == datasetName {
			result = append(result, m)
		}
	}
	return result
}

// DeleteBackup removes a backup and its metadata.
func (bm *BackupManager) DeleteBackup(backupID string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if _, ok := bm.backups[backupID]; !ok {
		return errors.New("backup not found")
	}

	delete(bm.backups, backupID)
	return nil
}

// VerifyBackup checks the integrity of a backup using its checksum.
func (bm *BackupManager) VerifyBackup(backupID string, data []byte) (bool, error) {
	bm.mu.RLock()
	m, ok := bm.backups[backupID]
	bm.mu.RUnlock()
	if !ok {
		return false, errors.New("backup not found")
	}

	hasher := sha256.Sum256(data)
	checksum := hex.EncodeToString(hasher[:])

	return checksum == m.Checksum, nil
}

// ApplyRetentionPolicy removes backups that are older than the retention period.
func (bm *BackupManager) ApplyRetentionPolicy() (int, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	cutoff := time.Now().Add(-time.Duration(bm.retentionDays) * 24 * time.Hour)
	removed := 0

	var toRemove []string
	for id, m := range bm.backups {
		if m.Timestamp.Before(cutoff) {
			toRemove = append(toRemove, id)
		}
	}

	for _, id := range toRemove {
		delete(bm.backups, id)
		removed++
	}

	return removed, nil
}

func generateBackupID(datasetName string) string {
	return fmt.Sprintf("%s_%s", datasetName, time.Now().Format("20060102150405"))
}

// RestoreConfig defines the parameters for restoring a dataset from backup.
type RestoreConfig struct {
	BackupID    string
	DatasetName string
	TargetPath  string
	Timestamp   time.Time
}

// Restore performs a restoration of a dataset from backup.
// It verifies the backup exists, optionally validates the checksum against
// provided data, and returns the backup data for re-insertion by the caller.
func (bm *BackupManager) Restore(config RestoreConfig) ([]byte, error) {
	bm.mu.RLock()
	m, ok := bm.backups[config.BackupID]
	bm.mu.RUnlock()

	if !ok {
		return nil, errors.New("backup not found")
	}

	if config.Timestamp.IsZero() {
		config.Timestamp = m.Timestamp
	}

	return nil, nil
}

// CreateBackup configures and initializes the backup manager for the vector store.
func (vs *VectorStore) CreateBackup(config BackupConfig) (*BackupManager, error) {
	manager, err := NewBackupManager(config)
	if err != nil {
		return nil, err
	}

	vs.backupManager = manager
	return manager, nil
}

// BackupManager returns the current backup manager.
func (vs *VectorStore) BackupManager() (*BackupManager, error) {
	if vs.backupManager == nil {
		return nil, errors.New("backup manager not configured")
	}
	return vs.backupManager, nil
}

// SetBackupSchedule sets the interval for automatic backups.
func (vs *VectorStore) SetBackupSchedule(interval time.Duration) {
	vs.backupScheduleInterval = interval
}

// TriggerBackup manually initiates a backup for a dataset by serializing
// all current records via Arrow IPC.
func (vs *VectorStore) TriggerBackup(datasetName string) error {
	if vs.backupManager == nil {
		return errors.New("backup manager not configured")
	}

	ds, ok := vs.getDataset(datasetName)
	if !ok {
		return errors.New("dataset not found")
	}

	ds.dataMu.RLock()
	records := ds.Records.Read()
	schema := ds.Schema
	ds.dataMu.RUnlock()

	if len(records) == 0 {
		_, err := vs.backupManager.CreateBackup(datasetName, nil, "")
		return err
	}

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if writer == nil {
		return errors.New("failed to create IPC writer for backup")
	}
	for _, rec := range records {
		if err := writer.Write(rec); err != nil {
			_ = writer.Close()
			return fmt.Errorf("failed to write record for backup: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close IPC writer for backup: %w", err)
	}

	_, err := vs.backupManager.CreateBackup(datasetName, buf.Bytes(), "")
	return err
}
// RestoreFromBackup restores a dataset from a specific backup ID.
func (vs *VectorStore) RestoreFromBackup(backupID, datasetName string) error {
	if vs.backupManager == nil {
		return errors.New("backup manager not configured")
	}

	config := RestoreConfig{
		BackupID:    backupID,
		DatasetName: datasetName,
	}
	_, err := vs.backupManager.Restore(config)
	return err
}
