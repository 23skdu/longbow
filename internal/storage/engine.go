package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/golang/snappy"
)

const (
	walFileName     = "wal.log"
	snapshotDirName = "snapshots"
)

// StorageConfig holds configuration for persistence
type StorageConfig struct {
	DataPath         string
	SnapshotInterval time.Duration
	AsyncFsync       bool
	DoPutBatchSize   int
	UseIOUring       bool
	UseDirectIO      bool
	WALCompression   bool
}

// StorageEngine manages the persistence layer (WAL and Snapshots).
type StorageEngine struct {
	config     StorageConfig
	dataPath   string
	mem        memory.Allocator
	wal        WAL
	walBatcher *WALBatcher
	mu         sync.RWMutex // Protects reconfiguration
	stopChan   chan struct{}
}

// NewStorageEngine creates and initializes a new storage engine.
func NewStorageEngine(cfg StorageConfig, mem memory.Allocator) (*StorageEngine, error) {
	if cfg.DataPath == "" {
		return nil, fmt.Errorf("storage: data path required")
	}

	if err := os.MkdirAll(cfg.DataPath, 0o755); err != nil {
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
		// WALBatcher is naturally async, Sync is best-effort or requires implementing a wait
		return nil
	}
	if e.wal != nil {
		return e.wal.Sync()
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
func (e *StorageEngine) ReplayWAL(applier ApplierFunc) (uint64, error) {
	start := time.Now()
	defer func() {
		metrics.WalReplayDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	walPath := filepath.Join(e.dataPath, walFileName)
	f, err := os.Open(walPath)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	// Logic copied from previous implementation
	var maxSeq uint64
	count := 0

	// Buffer for reading
	// Reuse a buffer pool for inner decompression if needed?

	for {
		header := make([]byte, 32)
		// 1. Read Header
		if _, err := io.ReadFull(f, header); err != nil {
			if err == io.EOF {
				break
			}
			if err == io.ErrUnexpectedEOF {
				break
			}
			return 0, fmt.Errorf("header read: %w", err)
		}

		storedChecksum := binary.LittleEndian.Uint32(header[0:4])
		seq := binary.LittleEndian.Uint64(header[4:12])
		ts := int64(binary.LittleEndian.Uint64(header[12:20]))
		nameLen := binary.LittleEndian.Uint32(header[20:24])
		recLen := binary.LittleEndian.Uint64(header[24:32])

		if seq > maxSeq {
			maxSeq = seq
		}

		if nameLen > 1024*1024 || recLen > 1024*1024*1024 {
			break
		}

		// 2. Read Name
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(f, nameBytes); err != nil {
			break
		}
		name := string(nameBytes)

		// 3. Read Record
		recBytes := make([]byte, recLen)
		if _, err := io.ReadFull(f, recBytes); err != nil {
			break
		}

		// 4. Verify Checksum
		crc := crc32.NewIEEE()
		_, _ = crc.Write(nameBytes)
		_, _ = crc.Write(recBytes)
		calculatedCRC := crc.Sum32()

		// Sentinel logic (Compressed)
		if storedChecksum == 0xFFFFFFFF {
			if nameLen != 1 || nameBytes[0] != 1 {
				continue
			}
			decompressed, err := snappy.Decode(nil, recBytes)
			if err != nil {
				continue
			}

			// Inner items
			dr := bytes.NewReader(decompressed)
			innerHeader := make([]byte, 32)
			for {
				if _, err := io.ReadFull(dr, innerHeader); err != nil {
					break
				}
				inSeq := binary.LittleEndian.Uint64(innerHeader[4:12])
				inTs := int64(binary.LittleEndian.Uint64(innerHeader[12:20]))
				inNameLen := binary.LittleEndian.Uint32(innerHeader[20:24])
				inRecLen := binary.LittleEndian.Uint64(innerHeader[24:32])

				if inSeq > maxSeq {
					maxSeq = inSeq
				}

				inNameBytes := make([]byte, inNameLen)
				_, _ = io.ReadFull(dr, inNameBytes)
				inRecBytes := make([]byte, inRecLen)
				_, _ = io.ReadFull(dr, inRecBytes)

				// Apply Inner
				r, err := ipc.NewReader(bytes.NewReader(inRecBytes), ipc.WithAllocator(e.mem))
				if err == nil {
					if r.Next() {
						rec := r.RecordBatch()
						rec.Retain()
						_ = applier(string(inNameBytes), rec, inSeq, inTs)
						count++
					}
					r.Release()
				}
			}
			continue
		}

		if calculatedCRC != storedChecksum {
			return 0, fmt.Errorf("wal crc mismatch at seq %d: expected %x, got %x", seq, storedChecksum, calculatedCRC)
		}

		// Deserialize Record
		r, err := ipc.NewReader(bytes.NewReader(recBytes), ipc.WithAllocator(e.mem))
		if err == nil {
			if r.Next() {
				rec := r.RecordBatch()
				rec.Retain()
				_ = applier(name, rec, seq, ts)
				count++
			}
			r.Release()
		}
	}

	metrics.WalReplayDurationSeconds.Observe(time.Since(start).Seconds())
	return maxSeq, nil
}

// SnapshotItem represents a dataset snapshot.
type SnapshotItem struct {
	Name         string
	Records      []arrow.RecordBatch
	GraphRecords []arrow.RecordBatch
	PQCodebook   []byte
	IndexConfig  []byte
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
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return fmt.Errorf("failed to create temp snapshot dir: %w", err)
	}

	err := source.Iterate(func(item SnapshotItem) error {
		e.writeSnapshotItem(&item, tempDir)
		return nil
	})
	if err != nil {
		return err
	}

	_ = os.RemoveAll(snapshotDir)
	if err := os.Rename(tempDir, snapshotDir); err != nil {
		return fmt.Errorf("failed to rename snapshot dir: %w", err)
	}

	// Truncate WAL
	// Truncate WAL and Restart Batcher
	e.mu.Lock()
	defer e.mu.Unlock()

	hadBatcher := e.walBatcher != nil

	// 1. Stop Batcher
	if hadBatcher {
		if err := e.walBatcher.Stop(); err != nil {
			return fmt.Errorf("failed to stop wal batcher for snapshot: %w", err)
		}
		e.walBatcher = nil
	}

	// 2. Truncate WAL
	if e.wal != nil {
		_ = e.wal.Close()
		walPath := filepath.Join(e.dataPath, walFileName)
		_ = os.Truncate(walPath, 0)
		e.wal = NewWAL(e.dataPath)
	}

	// 3. Restart Batcher
	if hadBatcher {
		batcherCfg := DefaultWALBatcherConfig()
		if e.config.DoPutBatchSize > 0 {
			batcherCfg.MaxBatchSize = e.config.DoPutBatchSize
		}
		batcherCfg.AsyncFsync.Enabled = e.config.AsyncFsync
		batcherCfg.UseIOUring = e.config.UseIOUring
		batcherCfg.UseDirectIO = e.config.UseDirectIO
		batcherCfg.WALCompression = e.config.WALCompression

		e.walBatcher = NewWALBatcher(e.dataPath, &batcherCfg)
		if err := e.walBatcher.Start(); err != nil {
			return fmt.Errorf("failed to restart wal batcher: %w", err)
		}
	}

	metrics.SnapshotDurationSeconds.Observe(time.Since(start).Seconds())
	return nil
}

func (e *StorageEngine) writeSnapshotItem(item *SnapshotItem, tempDir string) {
	// Write Data Records
	if len(item.Records) > 0 {
		path := filepath.Join(tempDir, item.Name+".parquet")
		f, err := os.Create(path)
		if err == nil {
			for _, rec := range item.Records {
				_ = writeParquet(f, rec)
			}
			_ = f.Close()
		}
	}

	// Write Graph Records
	if len(item.GraphRecords) > 0 {
		path := filepath.Join(tempDir, item.Name+".graph.parquet")
		f, err := os.Create(path)
		if err == nil {
			for _, rec := range item.GraphRecords {
				_ = writeGraphParquet(f, rec)
			}
			_ = f.Close()
		}
	}

	// Write PQ
	if len(item.PQCodebook) > 0 {
		path := filepath.Join(tempDir, item.Name+".pq")
		_ = os.WriteFile(path, item.PQCodebook, 0o644)
	}

	// Write Config
	if len(item.IndexConfig) > 0 {
		path := filepath.Join(tempDir, item.Name+".config")
		_ = os.WriteFile(path, item.IndexConfig, 0o644)
	}
}

// LoadSnapshots reads all snapshots and calls loader for each item.
func (e *StorageEngine) LoadSnapshots(loader func(SnapshotItem) error) error {
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
				item.Records = append(item.Records, rec)
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
		_ = f.Close()
	}

	for _, item := range partials {
		if err := loader(*item); err != nil {
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
