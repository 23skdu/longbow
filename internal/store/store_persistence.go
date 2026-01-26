package store

import (
	"fmt"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
)

// InitPersistence initializes the storage layer, replays WAL, and loads snapshots.
func (s *VectorStore) InitPersistence(cfg StorageConfig) error {
	s.configMu.Lock()
	defer s.configMu.Unlock()

	if s.engine != nil {
		return fmt.Errorf("persistence already initialized")
	}

	// 1. Initialize Storage Engine
	engine, err := storage.NewStorageEngine(cfg, s.mem)
	if err != nil {
		return fmt.Errorf("failed to create storage engine: %w", err)
	}
	s.engine = engine
	s.dataPath = cfg.DataPath

	// 2. Initialize WAL (and Batcher)
	if err := s.engine.InitWAL(); err != nil {
		return fmt.Errorf("failed to initialize WAL: %w", err)
	}

	// 3. Replay WAL
	maxSeq, err := s.engine.ReplayWAL(s.applyReplayBatch)
	if err != nil {
		return fmt.Errorf("failed to replay WAL: %w", err)
	}
	s.sequence.Store(maxSeq)

	// 4. Load Snapshots
	err = s.engine.LoadSnapshots(s.loadSnapshotItem)
	if err != nil {
		return fmt.Errorf("failed to load snapshots: %w", err)
	}

	s.logger.Info().
		Uint64("max_seq", maxSeq).
		Msg("Persistence initialized (WAL replayed, Snapshots loaded)")

	return nil
}

// applyReplayBatch applies a batch from WAL replay to memory.
func (s *VectorStore) applyReplayBatch(name string, rec arrow.RecordBatch, seq uint64, ts int64) error {
	if rec == nil || rec.NumRows() == 0 {
		return nil
	}

	rec.Retain()
	defer rec.Release()

	// 1. Get/Create Dataset
	ds, _ := s.getOrCreateDataset(name, func() *Dataset {
		d := NewDataset(name, rec.Schema())
		d.Logger = s.logger
		d.Topo = s.numaTopology

		// Initialize Index
		hasVectorColumn := false
		var vectorDim int
		for _, f := range rec.Schema().Fields() {
			if f.Name == "vector" {
				if fst, ok := f.Type.(*arrow.FixedSizeListType); ok {
					hasVectorColumn = true
					vectorDim = int(fst.Len())
					dataType := InferVectorDataType(rec.Schema(), "vector")
					if dataType == VectorTypeComplex64 || dataType == VectorTypeComplex128 {
						vectorDim /= 2
					}
				}
				break
			}
		}

		if hasVectorColumn {
			config := s.autoShardingConfig
			if config.ShardThreshold == 0 {
				config.ShardThreshold = 10000
				config.Enabled = true
			}
			aIdx := NewAutoShardingIndex(d, config)
			aIdx.SetInitialDimension(vectorDim)
			d.Index = aIdx
		}

		return d
	})

	// 2. Update Schema (Evolution)
	if err := ds.SchemaManager.Evolve(rec.Schema()); err != nil {
		return fmt.Errorf("schema mismatch during replay: %w", err)
	}
	ds.dataMu.Lock()
	ds.Schema = ds.SchemaManager.GetCurrentSchema()
	ds.dataMu.Unlock()

	// 3. Apply to Memory (LWW)
	// We reuse logic similar to ingestion but skip WAL write
	// Note: We need to ensure we don't double-count metrics or re-trigger WAL

	// Update LWW
	s.updateLWWAndMerkle(ds, rec, ts)

	// Add to Records
	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()

	rec.Retain() // Dataset takes ownership
	ds.Records = append(ds.Records, rec)
	ds.SizeBytes.Add(estimateBatchSize(rec))
	s.currentMemory.Add(estimateBatchSize(rec))

	// Note: Indexing should happen?
	// If we replay WAL, we might re-index.
	// Usually snapshots contain the index or we rebuild it.
	// If we have snapshots, WAL only contains recent changes.
	// We should probably just add to dataset and let async indexer handle it?
	// But during startup we want to be ready?
	// For now, let's assume we just add to memory.
	// Indexing will happen if we enqueue it or if we reconstruct index.
	// Ideally we should rebuild index or load it.

	// If we are just replaying WAL, we should queue for indexing?
	// Yes, usually WAL replay feeds into memory and index.
	// But we must NOT write to WAL again.

	// queue for indexing
	ds.PendingIndexJobs.Add(rec.NumRows())
	s.indexQueue.Send(IndexJob{
		DatasetName: name,
		Record:      rec, // Queue takes ownership (Retain?)
		CreatedAt:   time.Now(),
		BatchIdx:    len(ds.Records) - 1,
	})
	rec.Retain() // For Queue

	return nil
}

// loadSnapshotItem loads a snapshot item into memory.
func (s *VectorStore) loadSnapshotItem(item storage.SnapshotItem) error {
	if len(item.Records) == 0 {
		return nil
	}

	// Get Schema from first record
	schema := item.Records[0].Schema()

	ds, _ := s.getOrCreateDataset(item.Name, func() *Dataset {
		d := NewDataset(item.Name, schema)
		d.Logger = s.logger
		d.Topo = s.numaTopology

		// Initialize Index (Same logic as applyReplayBatch)
		hasVectorColumn := false
		var vectorDim int
		for _, f := range schema.Fields() {
			if f.Name == "vector" {
				if fst, ok := f.Type.(*arrow.FixedSizeListType); ok {
					hasVectorColumn = true
					vectorDim = int(fst.Len())
					dataType := InferVectorDataType(schema, "vector")
					if dataType == VectorTypeComplex64 || dataType == VectorTypeComplex128 {
						vectorDim /= 2
					}
				}
				break
			}
		}

		if hasVectorColumn {
			config := s.autoShardingConfig
			if config.ShardThreshold == 0 {
				config.ShardThreshold = 10000
				config.Enabled = true
			}
			aIdx := NewAutoShardingIndex(d, config)
			aIdx.SetInitialDimension(vectorDim)
			d.Index = aIdx
		}

		return d
	})

	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()

	for _, rec := range item.Records {
		rec.Retain()
		ds.Records = append(ds.Records, rec)

		size := estimateBatchSize(rec)
		ds.SizeBytes.Add(size)
		s.currentMemory.Add(size)

		// Indexing?
		// Snapshots might contain GraphRecords usage in HNSW/DiskGraph
		// If "IndexConfig" exists, passing to index...
		// For now just load data.
	}

	// If there are graph records or config, we might restore index state.
	// But ArrowHNSW restores from DiskGraph usually.

	return nil
}
