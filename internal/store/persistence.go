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
	if s.walFile == nil {
		return nil // Persistence disabled or not initialized
	}

	// Format: [NameLen: uint32][Name: bytes][RecordLen: uint64][RecordBytes: bytes]

	// 1. Serialize Record to buffer
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(s.mem))
	if err := w.Write(rec); err != nil {
		metrics.WalWritesTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("failed to serialize record for WAL: %w", err)
	}
	if err := w.Close(); err != nil {
		metrics.WalWritesTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("failed to close IPC writer: %w", err)
	}
	recBytes := buf.Bytes()

	// 2. Write Header & Data
	nameBytes := []byte(name)
	nameLen := uint32(len(nameBytes))
	recLen := uint64(len(recBytes))

	// Calc CRC
	crc := crc32.NewIEEE()
	_, _ = crc.Write(nameBytes)
	_, _ = crc.Write(recBytes)
	checksum := crc.Sum32()

	// Write 16-byte header
	header := make([]byte, 16)
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	binary.LittleEndian.PutUint32(header[4:8], nameLen)
	binary.LittleEndian.PutUint64(header[8:16], recLen)

	if _, err := s.walFile.Write(header); err != nil {
		metrics.WalWritesTotal.WithLabelValues("error").Inc()
		return err
	}
	if _, err := s.walFile.Write(nameBytes); err != nil {
		metrics.WalWritesTotal.WithLabelValues("error").Inc()
		return err
	}
	n, err := s.walFile.Write(recBytes)
	if err != nil {
		metrics.WalWritesTotal.WithLabelValues("error").Inc()
		return err
	}

	// Metrics success
	metrics.WalWritesTotal.WithLabelValues("ok").Inc()
	metrics.WalBytesWritten.Add(float64(4 + len(nameBytes) + 8 + n))

	// Ensure it's on disk
	// return s.walFile.Sync() // Optional: Sync every write for durability vs performance
	return nil
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
			// Append to store (skipping WAL write)
			ds := s.vectors.GetOrCreate(name, func() *Dataset {
				return &Dataset{Records: []arrow.RecordBatch{}, lastAccess: time.Now().UnixNano()}
			})
			ds.mu.Lock()
			ds.Records = append(ds.Records, rec)
			ds.mu.Unlock()
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
	s.vectors.Range(func(name string, ds *Dataset) bool {
		ds.mu.RLock()
		recs := make([]arrow.RecordBatch, len(ds.Records))
		copy(recs, ds.Records)
		ds.mu.RUnlock()
		if len(recs) == 0 {
			return true
		}
		path := filepath.Join(tempDir, name+".parquet")
		f, err := os.Create(path)
		if err != nil {
			s.logger.Error("Failed to create snapshot file",
				zap.Any("name", name),
				zap.Error(err))
			return true
		}

		// Write all records to the parquet file
		for _, rec := range recs {
			if err := writeParquet(f, rec); err != nil {
				s.logger.Error("Failed to write record to parquet snapshot",
					zap.Any("name", name),
					zap.Error(err))
				break
			}
		}
		_ = f.Close()
		return true
	})

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
	if s.walFile != nil {
		_ = s.walFile.Close()
		if err := os.Truncate(filepath.Join(s.dataPath, walFileName), 0); err != nil {
			s.logger.Error("Failed to truncate WAL", zap.Error(err))
		}
		// Reopen
		f, err := os.OpenFile(filepath.Join(s.dataPath, walFileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			s.walFile = f
		} else {
			s.logger.Error("Failed to reopen WAL after snapshot", zap.Error(err))
		}
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

		rec.Retain()
		ds := s.vectors.GetOrCreate(name, func() *Dataset {
			return &Dataset{Records: []arrow.RecordBatch{}, lastAccess: time.Now().UnixNano()}
		})
		ds.mu.Lock()
		ds.Records = append(ds.Records, rec)
		ds.mu.Unlock()
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

	s.stopCompaction()
	// Stop WAL batcher first to flush pending writes
	if s.walBatcher != nil {
		if err := s.walBatcher.Stop(); err != nil {
			s.logger.Error("Failed to stop WAL batcher", zap.Error(err))
		}
		s.walBatcher = nil
	}

	s.walMu.Lock()
	defer s.walMu.Unlock()

	if s.walFile != nil {
		s.logger.Info("Syncing and closing WAL file")
		if err := s.walFile.Sync(); err != nil {
			s.logger.Error("Failed to sync WAL", zap.Error(err))
		}
		if err := s.walFile.Close(); err != nil {
			return NewWALError("close", s.dataPath, 0, err)
		}
		s.walFile = nil
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
