package store

import (
	"fmt"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
)

// InitPersistence initializes the storage engine and replays data.
func (s *VectorStore) InitPersistence(cfg storage.StorageConfig) error {
	s.dataPath = cfg.DataPath

	// Create Engine
	engine, err := storage.NewStorageEngine(cfg, s.mem)
	if err != nil {
		return err
	}
	s.engine = engine

	// Load Snapshots
	if err := s.engine.LoadSnapshots(func(item storage.SnapshotItem) error {
		return s.loadSnapshotItem(&item)
	}); err != nil {
		s.logger.Error().Err(err).Msg("Failed to load snapshots")
	}

	// Replay WAL
	maxSeq, err := s.engine.ReplayWAL(func(name string, rec arrow.RecordBatch, seq uint64, ts int64) error {
		return s.ApplyDelta(name, rec, seq, ts)
	})
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to replay WAL")
		return err
	}

	s.sequence.Store(maxSeq)

	// Initialize WAL for writing
	if err := s.engine.InitWAL(); err != nil {
		return err
	}

	// Start snapshot ticker
	go s.runSnapshotTicker(cfg.SnapshotInterval)

	return nil
}

func (s *VectorStore) loadSnapshotItem(item *storage.SnapshotItem) error {
	s.mu.Lock()
	ds, ok := s.datasets[item.Name]
	if !ok {
		// Infer schema from first record
		var schema *arrow.Schema
		if len(item.Records) > 0 {
			schema = item.Records[0].Schema()
		} else if len(item.GraphRecords) > 0 {
			schema = item.GraphRecords[0].Schema()
		}
		if schema == nil {
			s.mu.Unlock()
			return nil // Empty snapshot item?
		}

		ds = NewDataset(item.Name, schema)
		ds.SetLastAccess(time.Now())

		// Load PQ
		if len(item.PQCodebook) > 0 {
			enc, err := DeserializePQEncoder(item.PQCodebook)
			if err == nil {
				ds.PQEncoder = enc
			} else {
				s.logger.Error().Err(err).Str("dataset", item.Name).Msg("Failed to deserialize PQ encoder")
			}
		}

		s.datasets[item.Name] = ds
	}
	s.mu.Unlock()

	// Initialize Index if missing (Critical for restoration)
	if ds.Index == nil {
		// Use default config for now, ideally persisted in snapshot metadata
		ds.Index = NewAutoShardingIndex(ds, DefaultAutoShardingConfig())
	}

	// Load Records and Rebuild Index
	for _, rec := range item.Records {
		rec.Retain()

		ds.dataMu.Lock()
		ds.Records = append(ds.Records, rec)
		batchIdx := len(ds.Records) - 1
		s.currentMemory.Add(CachedRecordSize(rec))
		ds.dataMu.Unlock() // Unlock before indexing to avoid potential deadlocks

		// Rebuild Index entry for this batch
		numRows := int(rec.NumRows())
		if numRows > 0 {
			rowIdxs := make([]int, numRows)
			batchIdxs := make([]int, numRows)
			recs := make([]arrow.RecordBatch, numRows)
			for i := 0; i < numRows; i++ {
				rowIdxs[i] = i
				batchIdxs[i] = batchIdx
				recs[i] = rec
			}

			// We ignore errors here? Or log them?
			// If indexing fails, we have data but no search.
			if _, err := ds.Index.AddBatch(recs, rowIdxs, batchIdxs); err != nil {
				s.logger.Error().Err(err).Str("dataset", ds.Name).Msg("Failed to rebuild index from snapshot")
				// Proceeding, but data might be unsearchable
			}
		}
	}

	// Load Graph
	if len(item.GraphRecords) > 0 {
		if ds.Graph == nil {
			ds.Graph = NewGraphStore()
		}
		for _, rec := range item.GraphRecords {
			if err := ds.Graph.FromArrowBatch(rec); err != nil {
				s.logger.Error().Err(err).Str("dataset", item.Name).Msg("Failed to load graph record")
			}
		}
	}

	return nil
}

func (s *VectorStore) ApplyDelta(name string, rec arrow.RecordBatch, seq uint64, ts int64) error {
	// 1. Validate
	if rec.NumRows() == 0 {
		return nil
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
			ds.dataMu.Lock()
			if ds.Graph == nil {
				ds.Graph = NewGraphStore()
			}
			if err := ds.Graph.FromArrowBatch(rec); err != nil {
				ds.dataMu.Unlock()
				return fmt.Errorf("failed to apply graph batch: %w", err)
			}
			ds.dataMu.Unlock()
			return nil
		}
	}

	// Ensure index exists
	ds.dataMu.Lock()
	if ds.Index == nil {
		config := AutoShardingConfig{ShardThreshold: 10000}
		ds.Index = NewAutoShardingIndex(ds, config)
	}
	ds.dataMu.Unlock()

	// 3. Row-level LWW
	s.updateLWWAndMerkle(ds, rec, ts)

	// 4. Append to dataset
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	batchIdx := len(ds.Records) - 1
	// Capture records slice for AddBatch to ensure we pass a valid view
	// Capture records slice for AddBatch to ensure we pass a valid view (Now handled by recs slice below)
	ds.dataMu.Unlock()

	// 5. Update Index (CRITICAL: SyncWorker relies on this)
	// We must index the batch so it's searchable and visible to IndexLen()
	numRows := int(rec.NumRows())
	rowIdxs := make([]int, numRows)
	batchIdxs := make([]int, numRows)

	// Prepare separate slice of records for AddBatch as it expects 1:1 mapping if used this way
	// Or better, use AddSafe in a loop if AddBatch is strictly for scatter-gather?
	// AddBatch implementation: loops i < len(recs), calls extractVector(recs[i], rowIdxs[i]).
	// So we need recs[i] to be the batch for rowIdxs[i].
	recs := make([]arrow.RecordBatch, numRows)

	for i := 0; i < numRows; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = batchIdx
		recs[i] = rec // All point to the same batch
	}

	if ds.Index != nil {
		if _, err := ds.Index.AddBatch(recs, rowIdxs, batchIdxs); err != nil {
			return fmt.Errorf("failed to index delta: %w", err)
		}
	}

	return nil
}

func (s *VectorStore) writeToWAL(rec arrow.RecordBatch, name string) error {
	if s.engine == nil {
		return nil
	}
	seq := s.sequence.Add(1)
	ts := time.Now().UnixNano()
	return s.engine.WriteToWAL(name, rec, seq, ts)
}

func (s *VectorStore) Snapshot() error {
	if s.engine == nil {
		return nil
	}

	// Create source iterator
	source := &storeSnapshotSource{s: s}
	return s.engine.Snapshot(source)
}

type storeSnapshotSource struct {
	s *VectorStore
}

func (src *storeSnapshotSource) Iterate(fn func(storage.SnapshotItem) error) error {
	src.s.mu.RLock()
	datasets := make([]*Dataset, 0, len(src.s.datasets))
	for _, ds := range src.s.datasets {
		datasets = append(datasets, ds)
	}
	src.s.mu.RUnlock()

	for _, ds := range datasets {
		ds.dataMu.RLock()

		item := storage.SnapshotItem{
			Name: ds.Name,
		}

		// Data Records
		for _, r := range ds.Records {
			r.Retain()
			item.Records = append(item.Records, r)
		}

		// Graph Records
		if ds.Graph != nil && ds.Graph.EdgeCount() > 0 {
			gRec, err := ds.Graph.ToArrowBatch(src.s.mem)
			if err == nil {
				gRec.Retain()
				item.GraphRecords = append(item.GraphRecords, gRec)
			}
		}

		// PQ
		if ds.PQEncoder != nil {
			item.PQCodebook = ds.PQEncoder.Serialize()
		}

		ds.dataMu.RUnlock()

		// Call callback
		if err := fn(item); err != nil {
			releaseItem(&item)
			return err
		}

		// Release retained
		releaseItem(&item)
	}
	return nil
}

func releaseItem(item *storage.SnapshotItem) {
	for _, r := range item.Records {
		r.Release()
	}
	for _, r := range item.GraphRecords {
		r.Release()
	}
}

func (s *VectorStore) runSnapshotTicker(interval time.Duration) {
	if interval <= 0 {
		return
	}
	timer := time.NewTicker(interval)
	defer timer.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-timer.C:
			if err := s.Snapshot(); err != nil {
				metrics.SnapshotTotal.WithLabelValues("error").Inc()
				s.logger.Error().Err(err).Msg("Scheduled snapshot failed")
			} else {
				metrics.SnapshotTotal.WithLabelValues("ok").Inc()
			}
		case newInterval := <-s.snapshotReset:
			timer.Stop()
			if newInterval > 0 {
				timer = time.NewTicker(newInterval)
			}
		}
	}
}

// ClosePersistence closes the storage engine.
func (s *VectorStore) ClosePersistence() error {
	if s.engine != nil {
		return s.engine.Close()
	}
	return nil
}

// FlushWAL forces a sync of the WAL to disk.
func (s *VectorStore) FlushWAL() error {
	if s.engine != nil {
		return s.engine.SyncWAL()
	}
	return nil
}

// TruncateWAL wrapper for backward compatibility or testing.
// In new engine, Snapshot calls handle rollback/truncate. This might be no-op.
func (s *VectorStore) TruncateWAL() error {
	// Engine handles WAL truncation during snapshot.
	// We can expose explicit truncate if needed, but Snapshot() does it.
	return nil
}
