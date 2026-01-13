package store

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
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

	// Start indexing workers early to process streaming snapshots
	// This prevents memory bloat by processing/indexing chunks as they arrive
	s.StartIndexingWorkers(runtime.NumCPU())

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
	s.workerWg.Add(1)
	go s.runSnapshotTicker(cfg.SnapshotInterval)

	return nil
}

func (s *VectorStore) loadSnapshotItem(item *storage.SnapshotItem) error {
	ds, _ := s.getOrCreateDataset(item.Name, func() *Dataset {
		// Infer schema from first record
		var schema *arrow.Schema
		if len(item.Records) > 0 {
			schema = item.Records[0].Schema()
		} else if len(item.GraphRecords) > 0 {
			schema = item.GraphRecords[0].Schema()
		}
		if schema == nil {
			return nil // Should handle this better?
		}

		ds := NewDataset(item.Name, schema)
		ds.SetLastAccess(time.Now())

		// Load PQ
		if len(item.PQCodebook) > 0 {
			enc, err := pq.DeserializePQEncoder(item.PQCodebook)
			if err == nil {
				ds.PQEncoder = enc
			} else {
				s.logger.Error().Err(err).Str("dataset", item.Name).Msg("Failed to deserialize PQ encoder")
			}
		}
		return ds
	})

	if ds == nil {
		return nil // Could happen if schema inference failed
	}

	// Initialize Index if missing (Critical for restoration)
	if ds.Index == nil {
		// Try to load persisted config
		var asConfig AutoShardingConfig

		if len(item.IndexConfig) > 0 {
			var arrowConfig ArrowHNSWConfig
			if err := json.Unmarshal(item.IndexConfig, &arrowConfig); err != nil {
				s.logger.Error().Err(err).Str("dataset", ds.Name).Msg("Failed to unmarshal index config")
				asConfig = DefaultAutoShardingConfig()
			} else {
				// We have a config!
				s.logger.Info().Str("dataset", ds.Name).Bool("bq_enabled", arrowConfig.BQEnabled).Msg("Restoring index with persisted config")
				asConfig = DefaultAutoShardingConfig()
				asConfig.IndexConfig = &arrowConfig
			}
		} else {
			asConfig = DefaultAutoShardingConfig()
		}

		ds.Index = NewAutoShardingIndex(ds, asConfig)
	}

	// Load Records and Queue for Indexing
	const maxQueueBytes = 100 * 1024 * 1024 // 100MB backpressure limit during restore

	for _, rec := range item.Records {
		rec.Retain()

		ds.dataMu.Lock()
		ds.Records = append(ds.Records, rec)
		fmt.Printf("loadSnapshotItem: Appended batch to ds %p. Total batches: %d\n", ds, len(ds.Records))
		batchIdx := len(ds.Records) - 1
		ds.UpdatePrimaryIndex(batchIdx, rec)
		s.currentMemory.Add(CachedRecordSize(rec))
		ds.dataMu.Unlock()

		// Rebuild Index Async with Backpressure
		// Apply Backpressure if queue is too full
		for s.indexQueue.EstimatedBytes() > maxQueueBytes {
			time.Sleep(10 * time.Millisecond)
		}

		numRows := int(rec.NumRows())
		if numRows > 0 {
			if ds.Index != nil {
				rec.Retain()
				job := IndexJob{
					DatasetName: item.Name,
					Record:      rec,
					BatchIdx:    batchIdx,
					CreatedAt:   time.Now(),
				}
				s.indexQueue.Send(job) // Buffer should be large enough, or overflow
				ds.PendingIndexJobs.Add(int64(numRows))
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
	s.logger.Info().Str("dataset", name).Uint64("seq", seq).Int64("ts", ts).Int64("rows", rec.NumRows()).Msg("ApplyDelta called")
	// 1. Validate
	if rec.NumRows() == 0 {
		return nil
	}

	// 2. Get or create dataset
	// 2. Get or create dataset
	ds, _ := s.getOrCreateDataset(name, func() *Dataset {
		return NewDataset(name, rec.Schema())
	})

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
		if s.datasetInitHook != nil {
			s.datasetInitHook(ds)
		} else {
			config := AutoShardingConfig{ShardThreshold: 10000}
			ds.Index = NewAutoShardingIndex(ds, config)
		}
	}
	ds.dataMu.Unlock()

	// 3. Row-level LWW
	s.updateLWWAndMerkle(ds, rec, ts)

	// 4. Append to dataset
	ds.dataMu.Lock()
	var baseRowID uint32
	for _, r := range ds.Records {
		baseRowID += uint32(r.NumRows())
	}
	ds.Records = append(ds.Records, rec)
	batchIdx := len(ds.Records) - 1
	ds.UpdatePrimaryIndex(batchIdx, rec)
	ds.dataMu.Unlock()

	// 5. Index text columns for Hybrid Search (Phase 13)
	s.indexTextColumnsForHybridSearch(ds, rec, baseRowID)

	if ds.Index != nil {
		numRows := int(rec.NumRows())
		rec.Retain()
		job := IndexJob{
			DatasetName: name,
			Record:      rec,
			BatchIdx:    batchIdx,
			CreatedAt:   time.Now(),
		}

		// Send with simple backpressure policy (Spin/Wait)
		// This ensures we never drop data, but propagate backpressure to caller (WAL or Network)
		for !s.indexQueue.Send(job) {
			time.Sleep(10 * time.Millisecond)
		}
		ds.PendingIndexJobs.Add(int64(numRows))
	}

	return nil
}

func (s *VectorStore) writeToWAL(rec arrow.RecordBatch, name string, ts int64) error {
	if s.engine == nil {
		return nil
	}
	seq := s.sequence.Add(1)
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
	datasets := make([]*Dataset, 0)
	src.s.IterateDatasets(func(_ string, ds *Dataset) {
		datasets = append(datasets, ds)
	})

	for _, ds := range datasets {
		if ds == nil {
			continue
		}
		ds.dataMu.RLock()

		item := storage.SnapshotItem{
			Name: ds.Name,
		}

		// Wire up DiskStore snapshot if available
		if ds.DiskStore != nil {
			// We need to capture ds and name for the closure
			dsName := ds.Name
			diskStore := ds.DiskStore
			item.OnSnapshot = func(backend storage.SnapshotBackend) error {
				return diskStore.SnapshotTo(context.Background(), backend, dsName)
			}
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

		// 4. PQ Codebook
		if ds.PQEncoder != nil {
			item.PQCodebook = ds.PQEncoder.Serialize()
		}

		// Rate Limit (Snapshot)
		// Estimate size:
		// Records: 0 (retained, pointer) - but we will write them.
		// We should account for them.
		// records size + graph records size + pq size
		// This iteration yields the item which will then be serialized.
		// So we are throttling the *production* of items.
		// Approximate size.
		var approxSize int
		for _, r := range item.Records {
			// Arrow RecordBatch GetRecordBatchInMemorySize is not available directly on interface?
			// Allow approximation: rows * cols * 8?
			// Let's use a helper if available, or just ignore for now and assume 1MB overhead?
			// Actually we can sum up buffer sizes.
			approxSize += int(r.NumRows() * int64(r.NumCols()) * 8) // Very rough
		}
		for _, r := range item.GraphRecords {
			approxSize += int(r.NumRows() * int64(r.NumCols()) * 8)
		}
		approxSize += len(item.PQCodebook)

		if src.s.rateLimiter != nil {
			startWait := time.Now()
			_ = src.s.rateLimiter.Wait(context.Background(), approxSize)
			metrics.SnapshotRateLimitWaitSeconds.Observe(time.Since(startWait).Seconds())
		}

		// Serialize Index Config (JSON)
		if ds.Index != nil {
			if asi, ok := ds.Index.(*AutoShardingIndex); ok {
				asi.mu.RLock()
				// access current index
				// Since we are in same package, we can access generic current
				// But we need to check if it's ArrowHNSW
				if ahnsw, ok := asi.current.(*ArrowHNSW); ok {
					cfg := ahnsw.config
					if data, err := json.Marshal(cfg); err == nil {
						item.IndexConfig = data
					}
				}
				asi.mu.RUnlock()
			}
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
	defer s.workerWg.Done()
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
