package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

type BackupConfig struct {
	BackupDir     string
	Interval      time.Duration
	RetentionDays int
	Incremental   bool
	Compression   bool
	Destination   string
}

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

type IncrementalBackup struct {
	Metadata BackupMetadata
	Deltas   []BackupDelta
}

type BackupDelta struct {
	Seq       int64
	Timestamp time.Time
	SizeBytes int64
	Checksum  string
}

type BackupManager struct {
	mu            sync.RWMutex
	config        BackupConfig
	backups       map[string]BackupMetadata
	backupDir     string
	retentionDays int
}

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

func (bm *BackupManager) GetBackup(backupID string) (BackupMetadata, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	m, ok := bm.backups[backupID]
	return m, ok
}

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

func (bm *BackupManager) DeleteBackup(backupID string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if _, ok := bm.backups[backupID]; !ok {
		return errors.New("backup not found")
	}

	delete(bm.backups, backupID)
	return nil
}

func (bm *BackupManager) VerifyBackup(backupID string, data []byte) (bool, error) {
	m, ok := bm.backups[backupID]
	if !ok {
		return false, errors.New("backup not found")
	}

	hasher := sha256.Sum256(data)
	checksum := hex.EncodeToString(hasher[:])

	return checksum == m.Checksum, nil
}

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

type RestoreConfig struct {
	BackupID    string
	DatasetName string
	TargetPath  string
	Timestamp   time.Time
}

func (bm *BackupManager) Restore(config RestoreConfig) error {
	bm.mu.RLock()
	m, ok := bm.backups[config.BackupID]
	bm.mu.RUnlock()

	if !ok {
		return errors.New("backup not found")
	}

	if config.Timestamp.IsZero() {
		config.Timestamp = m.Timestamp
	}

	return nil
}

func (vs *VectorStore) CreateBackup(config BackupConfig) (*BackupManager, error) {
	manager, err := NewBackupManager(config)
	if err != nil {
		return nil, err
	}

	vs.backupManager = manager
	return manager, nil
}

func (vs *VectorStore) BackupManager() (*BackupManager, error) {
	if vs.backupManager == nil {
		return nil, errors.New("backup manager not configured")
	}
	return vs.backupManager, nil
}

func (vs *VectorStore) SetBackupSchedule(interval time.Duration) {
	vs.backupScheduleInterval = interval
}

func (vs *VectorStore) TriggerBackup(datasetName string) error {
	if vs.backupManager == nil {
		return errors.New("backup manager not configured")
	}

	_, ok := vs.getDataset(datasetName)
	if !ok {
		return errors.New("dataset not found")
	}

	data := []byte("placeholder")
	_, err := vs.backupManager.CreateBackup(datasetName, data, "")
	return err
}

func (vs *VectorStore) RestoreFromBackup(backupID, datasetName string) error {
	if vs.backupManager == nil {
		return errors.New("backup manager not configured")
	}

	config := RestoreConfig{
		BackupID:    backupID,
		DatasetName: datasetName,
	}
	return vs.backupManager.Restore(config)
}
