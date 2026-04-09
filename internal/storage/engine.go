package storage

import (
	"context"
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
}

// StorageEngine manages the persistence layer (WAL and Snapshots).
type StorageEngine struct {
	config          StorageConfig
	dataPath        string
	mem             memory.Allocator
	wal             WAL
	walBatcher      *WALBatcher
	mu              sync.RWMutex // Protects reconfiguration
	stopChan        chan struct{}
	snapshotBackend SnapshotBackend
}

// NewStorageEngine creates and initializes a new storage engine.
func NewStorageEngine(cfg StorageConfig, mem memory.Allocator) (*StorageEngine, error) {
	if cfg.DataPath == "" {
		return nil, fmt.Errorf("storage: data path required")
	}

	if err := os.MkdirAll(cfg.DataPath, 0750); err != nil {
		return nil, fmt.Errorf("storage: failed to create data directory: %w", err)
	}

	e := &StorageEngine{
		config:   cfg,
		dataPath: cfg.DataPath,
		mem:      mem,
		stopChan: make(chan struct{}),
	}
	// Note: WAL and Batcher are initialized after Replay in typical flow.
	// But we can initialize WAL struct early if it doesn't truncate.
	// We'll leave explicit initialization to InitWAL() to match previous logic logic.

	return e, nil
}

// InitWAL initializes the WAL and Batcher.
func (e *StorageEngine) InitWAL() error {
	e.wal = NewWAL(e.dataPath)

	batcherCfg := DefaultWALBatcherConfig()
	if e.config.DoPutBatchSize > 0 {
		batcherCfg.MaxBatchSize = e.config.DoPutBatchSize
	}
	batcherCfg.AsyncFsync.Enabled = e.config.AsyncFsync
	batcherCfg.UseIOUring = e.config.UseIOUring
	batcherCfg.UseDirectIO = e.config.UseDirectIO
	batcherCfg.WALCompression = e.config.WALCompression
	batcherCfg.WALCompressionType = e.config.WALCompressionType

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
		return nil
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
}

type SnapshotSource interface {
	Iterate(func(SnapshotItem) error) error
}

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

	err := source.Iterate(func(item SnapshotItem) error {
		if e.snapshotBackend != nil && item.OnSnapshot != nil {
			if err := item.OnSnapshot(e.snapshotBackend); err != nil {
				return err
			}
		}
		return e.writeSnapshotItem(&item, tempDir)
	})
	if err != nil {
		return err
	}

	if err := os.RemoveAll(snapshotDir); err != nil {
		log.Warn().Err(err).Msg("Snapshot: failed to remove old snapshot dir")
	}

	if err := os.Rename(tempDir, snapshotDir); err != nil {
		return fmt.Errorf("failed to rename snapshot dir: %w", err)
	}

	// Truncate WAL and Restart Batcher
	e.mu.Lock()
	defer e.mu.Unlock()

	hadBatcher := e.walBatcher != nil

	if hadBatcher {
		if err := e.walBatcher.Stop(); err != nil {
			return fmt.Errorf("failed to stop wal batcher for snapshot: %w", err)
		}
		e.walBatcher = nil
	}

	if e.wal != nil {
		_ = e.wal.Close()
		walPath := filepath.Join(e.dataPath, walFileName)
		if err := os.Truncate(walPath, 0); err != nil {
			log.Error().Err(err).Msg("Snapshot: failed to truncate WAL")
		}
		e.wal = NewWAL(e.dataPath)
	}

	if hadBatcher {
		batcherCfg := DefaultWALBatcherConfig()
		if e.config.DoPutBatchSize > 0 {
			batcherCfg.MaxBatchSize = e.config.DoPutBatchSize
		}
		batcherCfg.AsyncFsync.Enabled = e.config.AsyncFsync
		batcherCfg.UseIOUring = e.config.UseIOUring
		batcherCfg.UseDirectIO = e.config.UseDirectIO
		batcherCfg.WALCompression = e.config.WALCompression
		batcherCfg.WALCompressionType = e.config.WALCompressionType

		e.walBatcher = NewWALBatcher(e.dataPath, &batcherCfg)
		if err := e.walBatcher.Start(); err != nil {
			return fmt.Errorf("failed to restart wal batcher: %w", err)
		}
	}

	metrics.SnapshotDurationSeconds.Observe(time.Since(start).Seconds())
	return nil
}

func (e *StorageEngine) writeSnapshotItem(item *SnapshotItem, tempDir string) error {
	limit := e.config.SnapshotRateLimit
	ctx := context.Background()

	getWriter := func(f *os.File) io.Writer {
		return NewRateLimitedWriter(f, limit, ctx)
	}

	// Write Data Records
	if len(item.Records) > 0 {
		path := filepath.Join(tempDir, item.Name+".parquet")
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("failed to create record parquet: %w", err)
		}

		w := getWriter(f)

		// Use configured compression
		compression := e.config.SnapshotCompression
		if compression == "" {
			compression = "zstd" // Default
		}

		if err := writeParquet(w, compression, item.Records...); err != nil {
			if closeErr := f.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("failed to close parquet file on write error")
			}
			return fmt.Errorf("failed to write record parquet: %w", err)
		}
		if err := f.Close(); err != nil {
			log.Error().Err(err).Msg("failed to close parquet file after write")
			return fmt.Errorf("failed to close parquet file: %w", err)
		}
	}

	// Write Graph Records
	if len(item.GraphRecords) > 0 {
		path := filepath.Join(tempDir, item.Name+".graph.parquet")
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("failed to create graph parquet: %w", err)
		}

		w := getWriter(f)
		compression := e.config.SnapshotCompression
		if compression == "" {
			compression = "zstd"
		}

		for _, rec := range item.GraphRecords {
			if err := writeGraphParquet(w, compression, rec); err != nil {
				if closeErr := f.Close(); closeErr != nil {
					log.Error().Err(closeErr).Msg("failed to close graph parquet file on write error")
				}
				return fmt.Errorf("failed to write graph parquet: %w", err)
			}
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("failed to close graph parquet file: %w", err)
		}
	}

	// Write PQ
	if len(item.PQCodebook) > 0 {
		path := filepath.Join(tempDir, item.Name+".pq")
		if err := os.WriteFile(path, item.PQCodebook, 0600); err != nil {
			return fmt.Errorf("failed to write PQ codebook: %w", err)
		}
	}

	// Write Config
	if len(item.IndexConfig) > 0 || item.IndexConfigWriter != nil {
		path := filepath.Join(tempDir, item.Name+".config")
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("failed to create index config file: %w", err)
		}

		w := getWriter(f)

		// Priority: Writer > Bytes
		if item.IndexConfigWriter != nil {
			if err := item.IndexConfigWriter(w); err != nil {
				_ = f.Close()
				return fmt.Errorf("failed to sync index config: %w", err)
			}
		} else if len(item.IndexConfig) > 0 {
			if _, err := w.Write(item.IndexConfig); err != nil {
				_ = f.Close()
				return fmt.Errorf("failed to write index config: %w", err)
			}
		}

		if err := f.Close(); err != nil {
			return fmt.Errorf("failed to close index config file: %w", err)
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

	for _, entry := range entries {
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

		f, err := os.Open(fullPath)
		if err != nil {
			continue
		}
		info, _ := f.Stat()

		switch entry.Name() {
		case name + ".parquet":
			rec, err := readParquet(f, info.Size(), e.mem)
			if err == nil && rec != nil {
				log.Debug().Str("name", name).Int64("rows", rec.NumRows()).Msg("LoadSnapshots: Loaded records")
				item.Records = append(item.Records, rec)
			} else if err != nil {
				log.Error().Err(err).Str("name", name).Msg("LoadSnapshots: Failed to read parquet")
			}
		case name + ".graph.parquet":
			rec, err := readGraphParquet(f, info.Size(), e.mem)
			if err == nil && rec != nil {
				item.GraphRecords = append(item.GraphRecords, rec)
			}
		case name + ".pq":
			data, err := os.ReadFile(fullPath)
			if err == nil {
				item.PQCodebook = data
			}
		case name + ".config":
			data, err := os.ReadFile(fullPath)
			if err == nil {
				item.IndexConfig = data
			}
		}
		if err := f.Close(); err != nil {
			log.Error().Err(err).Str("path", fullPath).Msg("failed to close snapshot file")
		}
	}

	for _, item := range partials {
		if err := loader(item); err != nil {
			return err
		}
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

func (e *StorageEngine) SetSnapshotBackend(backend SnapshotBackend) {
	e.snapshotBackend = backend
}

func (e *StorageEngine) GetSnapshotBackend() SnapshotBackend {
	return e.snapshotBackend
}

func (e *StorageEngine) WriteWAL(name string, rec arrow.RecordBatch, seq uint64, ts int64) error {
	return e.WriteToWAL(name, rec, seq, ts)
}

func (e *StorageEngine) CreateSnapshot(item *SnapshotItem) error {
	tempDir := filepath.Join(e.dataPath, snapshotDirName+"_tmp")
	_ = os.MkdirAll(tempDir, 0750) // nosec G301
	return e.writeSnapshotItem(item, tempDir)
}

func (e *StorageEngine) FlushWAL() error {
	return e.SyncWAL()
}

func (e *StorageEngine) TruncateWAL(seq uint64) error {
	return nil
}
