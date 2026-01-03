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
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
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
}

// InitPersistence initializes the WAL and loads any existing data
func (s *VectorStore) InitPersistence(cfg StorageConfig) error {
	s.dataPath = cfg.DataPath
	if err := os.MkdirAll(s.dataPath, 0o755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Load latest snapshot if exists
	if err := s.loadSnapshots(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to load snapshots")
		// Continue, maybe partial load or fresh start
	}

	// Replay WAL
	if err := s.replayWAL(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to replay WAL")
		return err
	}

	// Initialize WAL
	s.wal = NewWAL(s.dataPath, s)

	// Initialize WAL batcher for async writes
	batcherCfg := DefaultWALBatcherConfig()
	if cfg.DoPutBatchSize > 0 {
		batcherCfg.MaxBatchSize = cfg.DoPutBatchSize
	}
	batcherCfg.AsyncFsync.Enabled = cfg.AsyncFsync
	batcherCfg.UseIOUring = cfg.UseIOUring
	batcherCfg.UseDirectIO = cfg.UseDirectIO

	s.walBatcher = NewWALBatcher(s.dataPath, &batcherCfg)
	if err := s.walBatcher.Start(); err != nil {
		return fmt.Errorf("failed to start WAL batcher: %w", err)
	}

	// Start snapshot ticker
	go s.runSnapshotTicker(cfg.SnapshotInterval)

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

	seq := s.sequence.Add(1)
	ts := time.Now().UnixNano()
	return s.wal.Write(name, seq, ts, rec)
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

	s.logger.Info().Msg("Replaying WAL...")
	count := 0

	var maxSeq uint64
	for {
		header := make([]byte, 32)
		if _, err := io.ReadFull(f, header); err != nil {
			if err == io.EOF {
				break
			}
			return NewWALError("read", walPath, 0, fmt.Errorf("header: %w", err))
		}

		storedChecksum := binary.LittleEndian.Uint32(header[0:4])
		seq := binary.LittleEndian.Uint64(header[4:12])
		ts := int64(binary.LittleEndian.Uint64(header[12:20]))
		nameLen := binary.LittleEndian.Uint32(header[20:24])
		recLen := binary.LittleEndian.Uint64(header[24:32])

		_ = ts // LWW logic will use this later

		if seq > maxSeq {
			maxSeq = seq
		}

		// Safety checks for corrupted data
		if nameLen > 1024*1024 || recLen > 1024*1024*1024 { // 1MB name, 1GB record
			return NewWALError("read", walPath, 0, fmt.Errorf("invalid entry lengths: nameLen=%d, recLen=%d", nameLen, recLen))
		}

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

			if err := s.ApplyDelta(name, rec, seq, ts); err != nil {
				s.logger.Warn().
					Err(err).
					Str("dataset", name).
					Msg("Failed to apply WAL record")
			}
			count++
		}
		r.Release()
	}

	s.sequence.Store(maxSeq)
	s.logger.Info().
		Int("records_loaded", count).
		Uint64("max_seq", maxSeq).
		Msg("WAL Replay complete")
	return nil
}

func (s *VectorStore) Snapshot() error {
	start := time.Now()
	s.logger.Info().Msg("Starting Snapshot...")

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
	items := make([]snapItem, 0, len(s.datasets))
	for name, ds := range s.datasets {
		ds.dataMu.RLock()

		var graphRecs []arrow.RecordBatch
		if ds.Graph != nil && ds.Graph.EdgeCount() > 0 {
			gRec, err := ds.Graph.ToArrowBatch(s.mem)
			if err == nil {
				gRec.Retain()
				graphRecs = append(graphRecs, gRec)
			} else {
				s.logger.Error().Err(err).Msg("Failed to convert graph to arrow for snapshot")
			}
		}

		if len(ds.Records) > 0 || len(graphRecs) > 0 {
			recs := make([]arrow.RecordBatch, len(ds.Records))
			copy(recs, ds.Records)
			for _, r := range recs {
				r.Retain()
			}
			items = append(items, snapItem{Name: name, Recs: recs, GraphRecs: graphRecs})
		}
		ds.dataMu.RUnlock()
	}
	s.mu.RUnlock()

	for _, item := range items {
		s.writeSnapshotItem(item, tempDir)
	}

	// Atomic swap: Remove old, Rename temp to new
	if err := os.RemoveAll(snapshotDir); err != nil {
		s.logger.Error().Err(err).Msg("Failed to remove old snapshot dir")
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
			s.logger.Error().Err(err).Msg("Failed to truncate WAL")
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
	s.logger.Info().Msg("Snapshot complete")
	return nil
}

type snapItem struct {
	Name      string
	Recs      []arrow.RecordBatch
	GraphRecs []arrow.RecordBatch
}

func (s *VectorStore) writeSnapshotItem(item snapItem, tempDir string) {
	defer func(recs []arrow.RecordBatch) {
		for _, r := range recs {
			r.Release()
		}
	}(item.Recs)
	defer func(grecs []arrow.RecordBatch) {
		for _, r := range grecs {
			r.Release()
		}
	}(item.GraphRecs)

	// 1. Write Data Records
	if len(item.Recs) > 0 {
		path := filepath.Join(tempDir, item.Name+".parquet")
		f, err := os.Create(path)
		if err != nil {
			s.logger.Error().
				Str("name", item.Name).
				Err(err).
				Msg("Failed to create snapshot file")
			return
		}

		// Write all records to the parquet file
		for _, rec := range item.Recs {
			if err := writeParquet(f, rec); err != nil {
				s.logger.Error().
					Str("name", item.Name).
					Err(err).
					Msg("Failed to write record to parquet snapshot")
				f.Close()
				return
			}
		}

		// Hint to kernel that we won't need this file in cache
		if err := AdviseDontNeed(f); err != nil {
			s.logger.Debug().Err(err).Msg("Failed to advise DONTNEED on snapshot write")
		}
		_ = f.Close()
	}

	// 2. Write Graph Records
	if len(item.GraphRecs) > 0 {
		path := filepath.Join(tempDir, item.Name+".graph.parquet")
		f, err := os.Create(path)
		if err != nil {
			s.logger.Error().
				Str("name", item.Name).
				Err(err).
				Msg("Failed to create graph snapshot file")
			return
		}

		for _, rec := range item.GraphRecs {
			if err := writeGraphParquet(f, rec); err != nil {
				s.logger.Error().
					Str("name", item.Name).
					Err(err).
					Msg("Failed to write graph record to parquet snapshot")
				f.Close()
				return
			}
		}
		if err := AdviseDontNeed(f); err != nil {
			s.logger.Debug().Err(err).Msg("Failed to advise DONTNEED on graph snapshot write")
		}
		_ = f.Close()
	}
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

		fileName := entry.Name()
		name := fileName[:len(fileName)-8] // remove .parquet
		isGraph := false

		if len(name) > 6 && name[len(name)-6:] == ".graph" {
			name = name[:len(name)-6] // remove .graph
			isGraph = true
		}

		path := filepath.Join(snapshotDir, entry.Name())

		f, err := os.Open(path)
		if err != nil {
			s.logger.Error().
				Str("name", name).
				Err(err).
				Msg("Failed to open snapshot file")
			continue
		}
		stat, _ := f.Stat()

		// Read Parquet file
		var rec arrow.RecordBatch
		if isGraph {
			rec, err = readGraphParquet(f, stat.Size(), s.mem)
		} else {
			rec, err = readParquet(f, stat.Size(), s.mem)
		}

		// Hint kernel we are done with this file
		if err := AdviseDontNeed(f); err != nil {
			s.logger.Debug().Err(err).Msg("Failed to advise DONTNEED on snapshot read")
		}
		_ = f.Close()
		if err != nil {
			s.logger.Error().
				Str("name", name).
				Err(err).
				Msg("Failed to read parquet snapshot")
			continue
		}

		// Validate record integrity
		if err := validateRecordBatch(rec); err != nil {
			metrics.ValidationFailuresTotal.WithLabelValues("Snapshot", "invalid_batch").Inc()
			s.logger.Warn().
				Err(err).
				Str("dataset", name).
				Int64("numCols", rec.NumCols()).
				Int("numFields", rec.Schema().NumFields()).
				Msg("Skipping corrupted record in snapshot")
			rec.Release()
			continue
		}

		rec.Retain()
		s.mu.Lock()
		ds, ok := s.datasets[name]
		if !ok {
			// Use constructor to ensure all fields (Graph, Indexes) are initialized
			ds = NewDataset(name, rec.Schema())
			ds.SetLastAccess(time.Now())
			s.datasets[name] = ds
		}
		s.mu.Unlock()

		if isGraph {
			if ds.Graph == nil {
				ds.Graph = NewGraphStore()
			}
			if err := ds.Graph.FromArrowBatch(rec); err != nil {
				s.logger.Error().Err(err).Msg("Failed to load graph from snapshot")
			}
			rec.Release()
		} else {
			ds.dataMu.Lock()
			ds.Records = append(ds.Records, rec)
			ds.dataMu.Unlock()
			s.currentMemory.Add(CachedRecordSize(rec))
		}
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
		case <-s.stopChan:
			s.logger.Info().Msg("Snapshot ticker exiting")
			return
		case <-getTickChan():
			if err := s.Snapshot(); err != nil {
				s.logger.Error().Err(err).Msg("Scheduled snapshot failed")
			}
		case newInterval := <-s.snapshotReset:
			s.logger.Info().
				Dur("old_interval", initialInterval).
				Dur("new_interval", newInterval).
				Msg("Snapshot ticker updating")
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
	var closeErr error
	s.stopOnce.Do(func() {
		s.logger.Info().Msg("Closing VectorStore...")

		// Signal background tickers to stop
		close(s.stopChan)

		// Stop compaction worker
		s.stopCompaction()

		// Stop and drain indexing queue
		if s.indexQueue != nil {
			s.logger.Info().Msg("Stopping index queue...")
			s.indexQueue.Stop()
		}

		// Wait for all indexing workers to finish
		s.logger.Info().Msg("Waiting for indexing workers to finish...")
		s.indexWg.Wait()

		// Stop WAL batcher first to flush pending writes
		if s.walBatcher != nil {
			if err := s.walBatcher.Stop(); err != nil {
				s.logger.Error().Err(err).Msg("Failed to stop WAL batcher")
			}
			s.walBatcher = nil
		}

		s.walMu.Lock()
		defer s.walMu.Unlock()

		if s.wal != nil {
			s.logger.Info().Msg("Syncing and closing WAL")
			if err := s.wal.Sync(); err != nil {
				s.logger.Error().Err(err).Msg("Failed to sync WAL")
			}
			if err := s.wal.Close(); err != nil {
				closeErr = NewWALError("close", s.dataPath, 0, err)
			}
			s.wal = nil
		}
	})
	return closeErr
}

func (s *VectorStore) ApplyDelta(name string, rec arrow.RecordBatch, seq uint64, ts int64) error {
	// 1. Validate
	if err := validateRecordBatch(rec); err != nil {
		metrics.ValidationFailuresTotal.WithLabelValues("Apply", "invalid_batch").Inc()
		rec.Release()
		return err
	}

	// 2. Get or create dataset
	s.mu.Lock()
	ds, ok := s.datasets[name]
	if !ok {
		ds = NewDataset(name, rec.Schema())
		s.datasets[name] = ds
	}
	s.mu.Unlock()

	// Check if this is a graph batch
	if rec.Schema().Metadata().FindKey("longbow.entry_type") != -1 {
		val, _ := rec.Schema().Metadata().GetValue("longbow.entry_type")
		if val == "graph" {
			// Apply to GraphStore
			if ds.Graph == nil {
				ds.Graph = NewGraphStore()
			}
			if err := ds.Graph.FromArrowBatch(rec); err != nil {
				return fmt.Errorf("failed to apply graph batch: %w", err)
			}
			return nil // Done, do not proceed to vector indexing
		}
	}

	// Ensure index exists
	if ds.Index == nil {
		config := AutoShardingConfig{ShardThreshold: 10000}
		ds.Index = NewAutoShardingIndex(ds, config)
	}

	// 3. Row-level LWW if "id" column exists
	idColIdx := -1
	for i, f := range rec.Schema().Fields() {
		if f.Name == "id" {
			idColIdx = i
			break
		}
	}

	if idColIdx >= 0 {
		column := rec.Column(idColIdx)
		if ids, ok := column.(*array.Uint32); ok {
			for i := 0; i < int(rec.NumRows()); i++ {
				vid := VectorID(ids.Value(i))
				if ds.LWW.Update(vid, ts) {
					if ds.Merkle != nil {
						ds.Merkle.Update(vid, ts)
					}
				}
			}
		}
	}

	// 4. Append to dataset
	ds.dataMu.Lock()
	batchIdx := len(ds.Records)
	ds.Records = append(ds.Records, rec)
	ds.dataMu.Unlock()

	s.currentMemory.Add(CachedRecordSize(rec))
	ds.SizeBytes.Add(CachedRecordSize(rec))

	// 5. Queue for indexing (Batch-level)
	rec.Retain()
	if !s.indexQueue.Send(IndexJob{
		DatasetName: name,
		Record:      rec,
		BatchIdx:    batchIdx,
		CreatedAt:   time.Now(),
	}) {
		rec.Release()
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
