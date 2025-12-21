package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath" // Added by user instruction
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"go.uber.org/zap"
)

const (
	walFileName     = "wal.log"
	snapshotDirName = "snapshots"
)

// InitPersistence initializes the WAL and loads any existing data
func (s *VectorStore) InitPersistence(dataPath string, snapshotInterval time.Duration) error {
	s.dataPath = dataPath
	if err := os.MkdirAll(s.dataPath, 0o755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Load latest snapshot if exists
	if err := s.loadSnapshots(); err != nil {
		s.logger.Error("Failed to load snapshots", zap.Error(err))
		// Continue, maybe partial load or fresh start
	}

	// Replay WAL
	if err := s.replayWAL(); err != nil {
		s.logger.Error("Failed to replay WAL", zap.Error(err))
		return err
	}

	// Initialize WAL
	s.wal = NewWAL(s.dataPath, s)

	// Initialize WAL batcher for async writes
	cfg := DefaultWALBatcherConfig()
	s.walBatcher = NewWALBatcher(s.dataPath, &cfg)
	if err := s.walBatcher.Start(); err != nil {
		return fmt.Errorf("failed to start WAL batcher: %w", err)
	}

	// Start snapshot ticker
	go s.runSnapshotTicker(snapshotInterval)

	return nil
}

func (s *VectorStore) writeToWAL(rec arrow.RecordBatch, name string) error {
	s.walMu.Lock()
	defer s.walMu.Unlock()

	if rec == nil {
		return fmt.Errorf("record is nil")
	}
	if s.wal == nil {
		return nil // Persistence disabled or not initialized
	}

	return s.wal.Write(name, rec)
}

func (s *VectorStore) replayWAL() error {
	start := time.Now()
	defer func() {
		metrics.WalReplayDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	walPath := filepath.Join(s.dataPath, walFileName)
	f, err := os.Open(walPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	s.logger.Info("Replaying WAL...")
	count := 0

	for {
		header := make([]byte, 16)
		if _, err := io.ReadFull(f, header); err != nil {
			if err == io.EOF {
				break
			}
			return NewWALError("read", walPath, 0, fmt.Errorf("header: %w", err))
		}

		storedChecksum := binary.LittleEndian.Uint32(header[0:4])
		nameLen := binary.LittleEndian.Uint32(header[4:8])
		recLen := binary.LittleEndian.Uint64(header[8:16])

		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(f, nameBytes); err != nil {
			return NewWALError("read", walPath, 0, fmt.Errorf("name: %w", err))
		}
		name := string(nameBytes)

		recBytes := make([]byte, recLen)
		if _, err := io.ReadFull(f, recBytes); err != nil {
			return NewWALError("read", walPath, 0, fmt.Errorf("recBytes: %w", err))
		}

		// Verify Checksum
		crc := crc32.NewIEEE()
		_, _ = crc.Write(nameBytes)
		_, _ = crc.Write(recBytes)
		if crc.Sum32() != storedChecksum {
			metrics.WalWritesTotal.WithLabelValues("corruption").Inc()
			return NewWALError("read", walPath, 0, fmt.Errorf("crc mismatch: corrupted WAL entry"))
		}

		// Deserialize Record
		r, err := safeIPCNewReader(bytes.NewReader(recBytes))
		if err != nil {
			metrics.IpcDecodeErrorsTotal.WithLabelValues("wal", "error").Inc()
			return NewWALError("read", walPath, 0, fmt.Errorf("ipc reader: %w", err))
		}
		if r.Next() {
			rec := r.RecordBatch()
			rec.Retain()

			// Validate record integrity before adding to store
			if err := validateRecordBatch(rec); err != nil {
				metrics.ValidationFailuresTotal.WithLabelValues("WAL", "invalid_batch").Inc()
				s.logger.Warn("Skipping corrupted record in WAL",
					zap.Error(err),
					zap.String("dataset", name),
					zap.Int64("numCols", rec.NumCols()),
					zap.Int("numFields", rec.Schema().NumFields()))
				rec.Release()
				continue
			}

			// Append to store (skipping WAL write)
			s.mu.Lock()
			ds, ok := s.datasets[name]
			if !ok {
				ds = &Dataset{Records: []arrow.RecordBatch{}, lastAccess: time.Now().UnixNano(), Name: name}
				s.datasets[name] = ds
			}
			s.mu.Unlock()

			ds.dataMu.Lock()
			ds.Records = append(ds.Records, rec)
			ds.dataMu.Unlock()
			s.currentMemory.Add(CachedRecordSize(rec))
			count++
		}
		r.Release()
	}

	s.logger.Info("WAL Replay complete", zap.Any("records_loaded", count))
	return nil
}

func (s *VectorStore) Snapshot() error {
	start := time.Now()
	s.logger.Info("Starting Snapshot...")

	snapshotDir := filepath.Join(s.dataPath, snapshotDirName)
	tempDir := filepath.Join(s.dataPath, snapshotDirName+"_tmp")

	// Clean up any previous temp dir
	if err := os.RemoveAll(tempDir); err != nil {
		return fmt.Errorf("failed to clean temp snapshot dir: %w", err)
	}
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		metrics.SnapshotTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("failed to create temp snapshot dir: %w", err)
	}

	// Save each dataset to temp dir as Parquet
	s.mu.RLock()
	// Snapshot the map keys/values to avoid holding lock during I/O
	type snapItem struct {
		Name string
		Recs []arrow.RecordBatch
	}
	items := make([]snapItem, 0, len(s.datasets))
	for name, ds := range s.datasets {
		ds.dataMu.RLock()
		if len(ds.Records) > 0 {
			recs := make([]arrow.RecordBatch, len(ds.Records))
			// Retain? Parquet writer might not need retain if we write synchronously
			// But to be safe from concurrent eviction? Eviction holds dataMu lock?
			// ds.Evict holds dataMu. So RLock is enough.
			copy(recs, ds.Records)
			for _, r := range recs {
				r.Retain()
			}
			items = append(items, snapItem{Name: name, Recs: recs})
		}
		ds.dataMu.RUnlock()
	}
	s.mu.RUnlock()

	for _, item := range items {
		defer func(recs []arrow.RecordBatch) {
			for _, r := range recs {
				r.Release()
			}
		}(item.Recs)

		path := filepath.Join(tempDir, item.Name+".parquet")
		f, err := os.Create(path)
		if err != nil {
			s.logger.Error("Failed to create snapshot file",
				zap.Any("name", item.Name),
				zap.Error(err))
			continue
		}

		// Write all records to the parquet file
		for _, rec := range item.Recs {
			if err := writeParquet(f, rec); err != nil {
				s.logger.Error("Failed to write record to parquet snapshot",
					zap.Any("name", item.Name),
					zap.Error(err))
				break
			}
		}
		_ = f.Close()
	}

	// Atomic swap: Remove old, Rename temp to new
	if err := os.RemoveAll(snapshotDir); err != nil {
		s.logger.Error("Failed to remove old snapshot dir", zap.Error(err))
	}
	if err := os.Rename(tempDir, snapshotDir); err != nil {
		metrics.SnapshotTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("failed to rename snapshot dir: %w", err)
	}

	// Truncate WAL
	s.walMu.Lock()
	if s.wal != nil {
		_ = s.wal.Close()
		walPath := filepath.Join(s.dataPath, walFileName)
		if err := os.Truncate(walPath, 0); err != nil {
			s.logger.Error("Failed to truncate WAL", zap.Error(err))
		}
		// Reopen
		s.wal = NewStdWAL(s.dataPath, s)
	}
	s.walMu.Unlock()

	metrics.SnapshotTotal.WithLabelValues("ok").Inc()
	metrics.SnapshotDurationSeconds.Observe(time.Since(start).Seconds())
	metrics.SnapshotWriteDurationSeconds.Observe(time.Since(start).Seconds())
	// Calculate and track snapshot size
	snapshotSize := int64(0)
	if entries, err := os.ReadDir(snapshotDir); err == nil {
		for _, entry := range entries {
			if info, err := entry.Info(); err == nil {
				snapshotSize += info.Size()
			}
		}
	}
	metrics.SnapshotSizeBytes.Observe(float64(snapshotSize))
	s.logger.Info("Snapshot complete")
	return nil
}

func (s *VectorStore) loadSnapshots() error {
	snapshotDir := filepath.Join(s.dataPath, snapshotDirName)
	entries, err := os.ReadDir(snapshotDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".parquet" {
			continue
		}
		name := entry.Name()[:len(entry.Name())-8] // remove .parquet
		path := filepath.Join(snapshotDir, entry.Name())

		f, err := os.Open(path)
		if err != nil {
			s.logger.Error("Failed to open snapshot file",
				zap.Any("name", name),
				zap.Error(err))
			continue
		}
		stat, _ := f.Stat()

		// Read Parquet file
		rec, err := readParquet(f, stat.Size(), s.mem)
		_ = f.Close()
		if err != nil {
			s.logger.Error("Failed to read parquet snapshot",
				zap.Any("name", name),
				zap.Error(err))
			continue
		}

		// Validate record integrity
		if err := validateRecordBatch(rec); err != nil {
			metrics.ValidationFailuresTotal.WithLabelValues("Snapshot", "invalid_batch").Inc()
			s.logger.Warn("Skipping corrupted record in snapshot",
				zap.Error(err),
				zap.String("dataset", name),
				zap.Int64("numCols", rec.NumCols()),
				zap.Int("numFields", rec.Schema().NumFields()))
			rec.Release()
			continue
		}

		rec.Retain()
		s.mu.Lock()
		ds, ok := s.datasets[name]
		if !ok {
			ds = &Dataset{Records: []arrow.RecordBatch{}, lastAccess: time.Now().UnixNano(), Name: name}
			s.datasets[name] = ds
		}
		s.mu.Unlock()

		ds.dataMu.Lock()
		ds.Records = append(ds.Records, rec)
		ds.dataMu.Unlock()
		s.currentMemory.Add(CachedRecordSize(rec))
	}
	return nil
}

func (s *VectorStore) runSnapshotTicker(initialInterval time.Duration) {
	var ticker *time.Ticker
	if initialInterval > 0 {
		ticker = time.NewTicker(initialInterval)
	}

	// Helper to get channel safely
	getTickChan := func() <-chan time.Time {
		if ticker == nil {
			return nil
		}
		return ticker.C
	}

	for {
		select {
		case <-getTickChan():
			if err := s.Snapshot(); err != nil {
				s.logger.Error("Scheduled snapshot failed", zap.Error(err))
			}
		case newInterval := <-s.snapshotReset:
			s.logger.Info("Snapshot ticker updating",
				zap.Duration("old_interval", initialInterval),
				zap.Duration("new_interval", newInterval))
			if ticker != nil {
				ticker.Stop()
				ticker = nil
			}
			if newInterval > 0 {
				ticker = time.NewTicker(newInterval)
			}
			initialInterval = newInterval
		}
	}
}

// Close ensures the WAL is flushed and closed properly
func (s *VectorStore) Close() error {
	s.logger.Info("Closing VectorStore...")

	// Stop WAL batcher first to flush pending writes
	if s.walBatcher != nil {
		if err := s.walBatcher.Stop(); err != nil {
			s.logger.Error("Failed to stop WAL batcher", zap.Error(err))
		}
		s.walBatcher = nil
	}

	s.walMu.Lock()
	defer s.walMu.Unlock()

	if s.wal != nil {
		s.logger.Info("Syncing and closing WAL")
		if err := s.wal.Sync(); err != nil {
			s.logger.Error("Failed to sync WAL", zap.Error(err))
		}
		if err := s.wal.Close(); err != nil {
			return NewWALError("close", s.dataPath, 0, err)
		}
		s.wal = nil
	}
	return nil
}

// safeIPCNewReader wraps ipc.NewReader with panic recovery to protect the server
// from malformed IPC payloads that might cause the arrow library to panic.
func safeIPCNewReader(r io.Reader, opts ...ipc.Option) (reader *ipc.Reader, err error) {
	defer func() {
		if r := recover(); r != nil {
			metrics.IpcDecodeErrorsTotal.WithLabelValues("wal", "panic").Inc()
			err = fmt.Errorf("panic in ipc.NewReader: %v", r)
		}
	}()
	return ipc.NewReader(r, opts...)
}
