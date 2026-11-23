package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog/log"
)

const (
	walFileName     = "wal.log"
	snapshotDirName = "snapshots"
)

// StorageConfig holds configuration for persistence
type StorageConfig struct {
	DataPath            string
	SnapshotInterval    time.Duration
	AsyncFsync          bool
	DoPutBatchSize      int
	UseIOUring          bool
	UseDirectIO         bool
	WALCompression      bool
	WALCompressionType  string // "snappy", "zstd", "lz4". Default "snappy".
	SnapshotRateLimit   int    // Bytes per second. 0 = unlimited.
	SnapshotCompression string // "zstd", "lz4", "uncompressed". Default "zstd".
	Encryption          EncryptionConfig
}

// StorageEngine manages the persistence layer (WAL and Snapshots).
type StorageEngine struct {
	config          StorageConfig
	dataPath        string
	mem             memory.Allocator
	cipher          *CipherEngine
	wal             WAL
	walBatcher      *WALBatcher
	mu              sync.RWMutex // Protects reconfiguration
	stopChan        chan struct{}
	snapshotBackend SnapshotBackend
}

// RLockSnapshot acquires a read lock that prevents concurrent snapshots.
// All write operations that modify both memory and WAL must hold this lock
// to ensure consistency between snapshots and WAL replay.
func (e *StorageEngine) RLockSnapshot() {
	e.mu.RLock()
}

// RUnlockSnapshot releases the snapshot read lock.
func (e *StorageEngine) RUnlockSnapshot() {
	e.mu.RUnlock()
}

// NewStorageEngine creates and initializes a new storage engine.
func NewStorageEngine(cfg StorageConfig, mem memory.Allocator) (*StorageEngine, error) {
	if cfg.DataPath == "" {
		return nil, fmt.Errorf("storage: data path required")
	}

	if err := os.MkdirAll(cfg.DataPath, 0750); err != nil {
		return nil, fmt.Errorf("storage: failed to create data directory: %w", err)
	}

	cipher, err := NewCipherEngineFromConfig(context.Background(), cfg.Encryption)
	if err != nil {
		return nil, err
	}

	e := &StorageEngine{
		config:   cfg,
		dataPath: cfg.DataPath,
		mem:      mem,
		cipher:   cipher,
		stopChan: make(chan struct{}),
	}
	// Note: WAL and Batcher are initialized after Replay in typical flow.
	// But we can initialize WAL struct early if it doesn't truncate.
	// We'll leave explicit initialization to InitWAL() to match previous logic logic.

	return e, nil
}

// CipherEngine returns the engine's encryption engine, or nil when encryption
// at rest is disabled.
func (e *StorageEngine) CipherEngine() *CipherEngine {
	return e.cipher
}

// newWAL returns a WAL writer that encrypts at rest when configured.
func (e *StorageEngine) newWAL() WAL {
	if e.cipher != nil {
		return NewEncryptedWAL(e.dataPath, e.cipher)
	}
	return NewWAL(e.dataPath)
}

// newWALBatcherConfig carries the encryption setting into the batched WAL writer.
func (e *StorageEngine) newWALBatcherConfig() WALBatcherConfig {
	batcherCfg := DefaultWALBatcherConfig()
	if e.config.DoPutBatchSize > 0 {
		batcherCfg.MaxBatchSize = e.config.DoPutBatchSize
	}
	batcherCfg.AsyncFsync.Enabled = e.config.AsyncFsync
	batcherCfg.UseIOUring = e.config.UseIOUring
	batcherCfg.UseDirectIO = e.config.UseDirectIO
	batcherCfg.WALCompression = e.config.WALCompression
	batcherCfg.WALCompressionType = e.config.WALCompressionType
	batcherCfg.Cipher = e.cipher
	return batcherCfg
}

// InitWAL initializes the WAL and Batcher.
func (e *StorageEngine) InitWAL() error {
	e.wal = e.newWAL()

	batcherCfg := e.newWALBatcherConfig()
	e.walBatcher = NewWALBatcher(e.dataPath, &batcherCfg)
	if err := e.walBatcher.Start(); err != nil {
		return fmt.Errorf("failed to start WAL batcher: %w", err)
	}

	return nil
}

// WriteToWAL writes a record to the WAL.
func (e *StorageEngine) WriteToWAL(name string, rec arrow.RecordBatch, seq uint64, ts int64) error {
	if e.walBatcher != nil {
		return e.walBatcher.Write(rec, name, seq, ts)
	}
	if e.wal == nil {
		return fmt.Errorf("storage: WAL not initialized")
	}
	return e.wal.Write(name, seq, ts, rec)
}

// SyncWAL forces WAL to flush to disk
func (e *StorageEngine) SyncWAL() error {
	if e.walBatcher != nil {
		return e.walBatcher.Flush()
	}
	if e.wal != nil {
		return e.wal.Sync()
	}
	return nil
}

// ErrCh returns a channel that signals background WAL errors.
// Returns nil if the current WAL configuration is synchronous.
func (e *StorageEngine) ErrCh() <-chan error {
	if e.walBatcher != nil {
		return e.walBatcher.ErrCh
	}
	return nil
}

// GetWALQueueDepth returns the current pending and capacity of the WAL queue.
func (e *StorageEngine) GetWALQueueDepth() (pending, capacity int) {
	if e.walBatcher == nil {
		return 0, 0
	}
	return e.walBatcher.QueueStatus()
}

// ApplierFunc is a callback for applying WAL entries during replay.
type ApplierFunc func(name string, rec arrow.RecordBatch, seq uint64, ts int64) error

// ReplayWAL reads the WAL and calls the applier for each entry.
// Returns the maximum sequence number encountered.

// SnapshotItem represents a dataset snapshot.
type SnapshotItem struct {
	Name             string
	Records          []arrow.RecordBatch
	GraphRecords     []arrow.RecordBatch
	PQCodebook       []byte
	IndexConfigIndex int // Deprecated or internal? No, IndexConfig is []byte.
	IndexConfig      []byte

	// IndexConfigWriter is a callback to stream index config directly to the output.
	// If set, recursive buffering is avoided.
	IndexConfigWriter func(io.Writer) error

	// OnSnapshot is a callback to handle custom snapshot logic (e.g. large file streaming)
	// It is called with the active snapshot backend if one is configured.
	OnSnapshot func(backend SnapshotBackend) error

	// Cleanup is called after the item has been processed (either successfully or with error).
	Cleanup func()
}

// SnapshotSource represents a snapshot source value.
type SnapshotSource interface {
	Iterate(func(SnapshotItem) error) error
}

// Snapshot runs the snapshot operation.
func (e *StorageEngine) Snapshot(source SnapshotSource) error {
	start := time.Now()

	snapshotDir := filepath.Join(e.dataPath, snapshotDirName)
	tempDir := filepath.Join(e.dataPath, snapshotDirName+"_tmp")

	if err := os.RemoveAll(tempDir); err != nil {
		return fmt.Errorf("failed to clean temp snapshot dir: %w", err)
	}
	if err := os.MkdirAll(tempDir, 0750); err != nil {
		return fmt.Errorf("failed to create temp snapshot dir: %w", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1024)

	err := source.Iterate(func(item SnapshotItem) error {
		if e.snapshotBackend != nil && item.OnSnapshot != nil {
			if err := item.OnSnapshot(e.snapshotBackend); err != nil {
				return err
			}
		}

		wg.Add(1)
		go func(it SnapshotItem) {
			defer wg.Done()
			if it.Cleanup != nil {
				defer it.Cleanup()
			}
			if err := e.writeSnapshotItem(&it, tempDir); err != nil {
				errCh <- err
			}
		}(item)
		return nil
	})

	wg.Wait()
	close(errCh)

	if err != nil {
		return err
	}

	// Check for errors from parallel workers
	for e2 := range errCh {
		if e2 != nil {
			return e2
		}
	}

	// Hold the write lock during atomic swap and WAL reset to prevent writes
	// while switching snapshot directories and resetting sequence numbers.
	e.mu.Lock()

	// Atomic snapshot swap: rename old -> backup, new -> target, then cleanup backup.
	// This ensures a snapshot directory always exists even if the process crashes mid-swap.
	backupDir := snapshotDir + "_bak"
	_ = os.RemoveAll(backupDir) // best-effort cleanup of stale backup

	if _, err := os.Stat(snapshotDir); err == nil {
		// Old snapshot exists — move it out of the way atomically
		if err := os.Rename(snapshotDir, backupDir); err != nil {
			e.mu.Unlock()
			return fmt.Errorf("failed to rename old snapshot dir: %w", err)
		}
	}

	if err := os.Rename(tempDir, snapshotDir); err != nil {
		// Try to recover by renaming backup back
		_ = os.Rename(backupDir, snapshotDir)
		e.mu.Unlock()
		return fmt.Errorf("failed to rename new snapshot dir: %w", err)
	}

	// Cleanup old snapshot after successful swap
	_ = os.RemoveAll(backupDir)

	// Truncate WAL and Restart Batcher
	hadBatcher := e.walBatcher != nil

	if hadBatcher {
		if err := e.walBatcher.Stop(); err != nil {
			e.mu.Unlock()
			return fmt.Errorf("failed to stop wal batcher for snapshot: %w", err)
		}
		e.walBatcher = nil
	}

	if e.wal != nil {
		_ = e.wal.Close()
		walPath := filepath.Join(e.dataPath, walFileName)
		if err := os.Truncate(walPath, 0); err != nil {
			if os.IsNotExist(err) {
				f, createErr := os.Create(walPath) // #nosec G304
				if createErr != nil {
					log.Error().Err(createErr).Msg("Snapshot: failed to create WAL file")
				} else {
					_ = f.Close()
				}
			} else {
				log.Error().Err(err).Msg("Snapshot: failed to truncate WAL")
			}
		}
		e.wal = e.newWAL()
	}

	if hadBatcher {
		batcherCfg := e.newWALBatcherConfig()
		e.walBatcher = NewWALBatcher(e.dataPath, &batcherCfg)
		if err := e.walBatcher.Start(); err != nil {
			e.mu.Unlock()
			return fmt.Errorf("failed to restart wal batcher: %w", err)
		}
	}

	e.mu.Unlock()

	metrics.SnapshotDurationSeconds.Observe(time.Since(start).Seconds())
	return nil
}

// writeProtectedFile writes a snapshot file, sealing it when encryption at
// rest is enabled. The file name is used as AAD to prevent swap attacks.
func (e *StorageEngine) writeProtectedFile(path string, data []byte) error {
	payload := data
	if e.cipher != nil {
		sealed, err := e.cipher.Seal(data, []byte(filepath.Base(path)))
		if err != nil {
			return fmt.Errorf("failed to encrypt snapshot file %q: %w", filepath.Base(path), err)
		}
		payload = sealed
	}
	f, err := os.OpenFile(filepath.Clean(path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) // #nosec G304
	if err != nil {
		return err
	}
	w := NewRateLimitedWriter(f, e.config.SnapshotRateLimit, context.Background())
	if _, err := w.Write(payload); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// readProtectedFile reverses writeProtectedFile.
func (e *StorageEngine) readProtectedFile(path string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Clean(path)) // #nosec G304
	if err != nil {
		return nil, err
	}
	if e.cipher == nil {
		return data, nil
	}
	return e.cipher.Open(context.Background(), data, []byte(filepath.Base(path)))
}

func (e *StorageEngine) writeSnapshotItem(item *SnapshotItem, tempDir string) error {
	compression := e.config.SnapshotCompression
	if compression == "" {
		compression = "zstd"
	}

	// Write Data Records
	if len(item.Records) > 0 {
		path := filepath.Join(tempDir, item.Name+".parquet")
		var buf bytes.Buffer
		if err := writeParquet(&buf, compression, item.Records...); err != nil {
			return fmt.Errorf("failed to write record parquet: %w", err)
		}
		if err := e.writeProtectedFile(path, buf.Bytes()); err != nil {
			return fmt.Errorf("failed to write record parquet: %w", err)
		}
	}

	// Write Graph Records
	if len(item.GraphRecords) > 0 {
		path := filepath.Join(tempDir, item.Name+".graph.parquet")
		var buf bytes.Buffer
		w := &buf
		for _, rec := range item.GraphRecords {
			if err := writeGraphParquet(w, compression, rec); err != nil {
				return fmt.Errorf("failed to write graph parquet: %w", err)
			}
		}
		if err := e.writeProtectedFile(path, buf.Bytes()); err != nil {
			return fmt.Errorf("failed to write graph parquet: %w", err)
		}
	}

	// Write PQ
	if len(item.PQCodebook) > 0 {
		path := filepath.Join(tempDir, item.Name+".pq")
		if err := e.writeProtectedFile(path, item.PQCodebook); err != nil {
			return fmt.Errorf("failed to write PQ codebook: %w", err)
		}
	}

	// Write Config
	if len(item.IndexConfig) > 0 || item.IndexConfigWriter != nil {
		path := filepath.Join(tempDir, item.Name+".config")
		var buf bytes.Buffer
		w := &buf
		// Priority: Writer > Bytes
		if item.IndexConfigWriter != nil {
			if err := item.IndexConfigWriter(w); err != nil {
				return fmt.Errorf("failed to sync index config: %w", err)
			}
		} else if _, err := w.Write(item.IndexConfig); err != nil {
			return fmt.Errorf("failed to write index config: %w", err)
		}
		if err := e.writeProtectedFile(path, buf.Bytes()); err != nil {
			return fmt.Errorf("failed to write index config: %w", err)
		}
	}
	return nil
}

// LoadSnapshots reads all snapshots and calls loader for each item.
func (e *StorageEngine) LoadSnapshots(loader func(*SnapshotItem) error) error {
	// If backend is present, we might want to list from backend first?
	// For now, load from local as snapshots are typically local first.
	// If S3 restoration is needed, it should be explicit 'Restore' command or Init logic.

	snapshotDir := filepath.Join(e.dataPath, snapshotDirName)
	entries, err := os.ReadDir(snapshotDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	partials := make(map[string]*SnapshotItem)
	var loadErrors []error

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		log.Trace().Str("entry", entry.Name()).Msg("LoadSnapshots: Processing entry")
		ext := filepath.Ext(entry.Name())
		name := entry.Name()[:len(entry.Name())-len(ext)]
		fullPath := filepath.Join(snapshotDir, entry.Name())

		item, ok := partials[name]
		if !ok {
			// Strip additional .graph from name if it exists
			if len(name) > 6 && name[len(name)-6:] == ".graph" {
				name = name[:len(name)-6]
			}
			item, ok = partials[name]
			if !ok {
				item = &SnapshotItem{Name: name}
				partials[name] = item
			}
		}

		switch entry.Name() {
		case name + ".parquet", name + ".graph.parquet", name + ".pq", name + ".config":
		default:
			// Ignore unrelated files in the snapshot directory.
			continue
		}

		data, err := e.readProtectedFile(fullPath)
		if err != nil {
			loadErrors = append(loadErrors, fmt.Errorf("snapshot file %q: %w", entry.Name(), err))
			continue
		}

		switch entry.Name() {
		case name + ".parquet":
			rec, err := readParquet(bytes.NewReader(data), int64(len(data)), e.mem) // #nosec G115
			if err == nil && rec != nil {
				log.Debug().Str("name", name).Int64("rows", rec.NumRows()).Msg("LoadSnapshots: Loaded records")
				item.Records = append(item.Records, rec)
			} else if err != nil {
				log.Error().Err(err).Str("name", name).Msg("LoadSnapshots: Failed to read parquet")
				loadErrors = append(loadErrors, fmt.Errorf("snapshot %q: %w", name, err))
			}
		case name + ".graph.parquet":
			rec, err := readGraphParquet(bytes.NewReader(data), int64(len(data)), e.mem) // #nosec G115
			if err == nil && rec != nil {
				item.GraphRecords = append(item.GraphRecords, rec)
			} else if err != nil {
				loadErrors = append(loadErrors, fmt.Errorf("snapshot graph %q: %w", name, err))
			}
		case name + ".pq":
			item.PQCodebook = data
		case name + ".config":
			item.IndexConfig = data
		}
	}

	for _, item := range partials {
		if err := loader(item); err != nil {
			loadErrors = append(loadErrors, fmt.Errorf("loader failed for %q: %w", item.Name, err))
		}
	}

	if len(loadErrors) > 0 {
		return fmt.Errorf("snapshot load completed with %d error(s): %w", len(loadErrors), errors.Join(loadErrors...))
	}

	return nil
}

func validateRecordBatch(rec arrow.RecordBatch) error {
	if rec.NumRows() == 0 {
		return fmt.Errorf("empty batch")
	}
	return nil
}

// Close closes the engine.
func (e *StorageEngine) Close() error {
	if e.stopChan != nil {
		close(e.stopChan)
	}
	if e.walBatcher != nil {
		_ = e.walBatcher.Stop()
	}
	if e.wal != nil {
		_ = e.wal.Close()
	}
	return nil
}

// SetSnapshotBackend sets the snapshot backend. When encryption is enabled,
// external snapshot uploads are sealed before reaching the backend.
func (e *StorageEngine) SetSnapshotBackend(backend SnapshotBackend) {
	if e.cipher != nil && backend != nil {
		backend = NewEncryptedSnapshotBackend(backend, e.cipher)
	}
	e.snapshotBackend = backend
}

// GetSnapshotBackend returns the snapshot backend.
func (e *StorageEngine) GetSnapshotBackend() SnapshotBackend {
	return e.snapshotBackend
}

// SetReplicator injects the WAL Replicator for HA deployments.
func (e *StorageEngine) SetReplicator(r WALReplicator) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.walBatcher != nil {
		e.walBatcher.SetReplicator(r)
	}
}

// AppendReplicatedWAL writes a raw replicated WAL block directly to disk.
func (e *StorageEngine) AppendReplicatedWAL(data []byte) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.walBatcher != nil && e.walBatcher.backend != nil {
		_, err := e.walBatcher.backend.Write(data)
		if err != nil {
			return err
		}
		// Synchronous replication implies sync to disk locally too
		return e.walBatcher.backend.Sync()
	}

	if e.wal != nil {
		return fmt.Errorf("AppendReplicatedWAL not supported for non-batched WAL")
	}

	return fmt.Errorf("WAL not initialized")
}

// GetAllocator returns the memory allocator used by the engine.
func (e *StorageEngine) GetAllocator() memory.Allocator {
	return e.mem
}

// WriteWAL runs the write wal operation.
func (e *StorageEngine) WriteWAL(name string, rec arrow.RecordBatch, seq uint64, ts int64) error {
	return e.WriteToWAL(name, rec, seq, ts)
}

// CreateSnapshot runs the create snapshot operation.
func (e *StorageEngine) CreateSnapshot(item *SnapshotItem) error {
	tempDir := filepath.Join(e.dataPath, snapshotDirName+"_tmp")
	_ = os.MkdirAll(tempDir, 0750) // nosec G301
	return e.writeSnapshotItem(item, tempDir)
}

// FlushWAL runs the flush wal operation.
func (e *StorageEngine) FlushWAL() error {
	return e.SyncWAL()
}

// TruncateWAL runs the truncate wal operation.
func (e *StorageEngine) TruncateWAL(seq uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	hadBatcher := e.walBatcher != nil
	if hadBatcher {
		if err := e.walBatcher.Stop(); err != nil {
			return fmt.Errorf("failed to stop wal batcher for truncation: %w", err)
		}
		e.walBatcher = nil
	}

	if e.wal != nil {
		_ = e.wal.Close()
	}

	walPath := filepath.Join(e.dataPath, walFileName)
	if err := os.Truncate(walPath, 0); err != nil {
		if os.IsNotExist(err) {
			f, createErr := os.Create(walPath) // #nosec G304 -- walPath is filepath.Join(configuredDataPath, constant)
			if createErr != nil {
				return fmt.Errorf("failed to create WAL file on truncation: %w", createErr)
			}
			_ = f.Close()
		} else {
			return fmt.Errorf("failed to truncate WAL file: %w", err)
		}
	}

	// Re-initialize WAL and restart batcher if needed
	e.wal = e.newWAL()

	if hadBatcher {
		batcherCfg := e.newWALBatcherConfig()
		e.walBatcher = NewWALBatcher(e.dataPath, &batcherCfg)
		if err := e.walBatcher.Start(); err != nil {
			return fmt.Errorf("failed to restart wal batcher after truncation: %w", err)
		}
	}

	log.Info().Uint64("seq", seq).Msg("WAL truncated successfully")
	return nil
}
