package store

import (
	"context"
	"runtime"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// StoreLifecycle manages startup/shutdown of standard components
// such as managing memory pressure, eviction, and startup.

// evictDataset evicts a dataset from memory.
// evictDataset evicts a dataset from memory.
func (s *VectorStore) evictDataset(name string) {
	var ds *Dataset
	s.updateDatasets(func(m map[string]*Dataset) {
		if d, ok := m[name]; ok {
			ds = d
			delete(m, name)
		}
	})

	if ds == nil {
		return
	}

	size := ds.SizeBytes.Load()
	s.currentMemory.Add(-size)

	if ds.Index != nil {
		_ = ds.Index.Close()
	}

	// Release records
	// Note: We need lock to safely read records?
	// The dataset is removed from map, but other readers might still hold a pointer.
	// We can't immediately release if RCU readers are active.
	// But Arrow Release() decrements refcount. If readers retained, it's fine.
	// If store owns the "base" refcount, we release it here.
	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()
	for _, r := range ds.Records {
		r.Release()
	}

	// Metrics updated elsewhere
}

// PrewarmDataset initializes a dataset and its index before first data ingestion
func (s *VectorStore) PrewarmDataset(name string, schema *arrow.Schema) {
	_, created := s.getOrCreateDataset(name, func() *Dataset {
		ds := NewDataset(name, schema)
		ds.Topo = s.numaTopology

		// Initialize index immediately used default config or a simplified one
		config := s.autoShardingConfig
		if config.ShardThreshold == 0 {
			// Fallback default
			config.ShardThreshold = 10000
			config.Enabled = true
			config.ShardCount = runtime.NumCPU()
		}

		aIdx := NewAutoShardingIndex(ds, config)

		// Try to find vector dimension
		// We'll copy the logic from applyBatchToMemory or extract it
		for _, f := range schema.Fields() {
			if f.Name == "vector" {
				if fst, ok := f.Type.(*arrow.FixedSizeListType); ok {
					aIdx.SetInitialDimension(int(fst.Len()))
				}
			}
		}

		// Pre-warm to mitigate cold start latency
		// We use a reasonable default batch size (4096 fits in 4 chunks)
		aIdx.PreWarm(4096)

		ds.Index = aIdx

		return ds
	})

	if created {
		s.logger.Info().Str("dataset", name).Msg("Pre-warmed dataset with index")
	}
}

// StartLifecycleManager starts the lifecycle manager background task.
func (s *VectorStore) StartLifecycleManager(ctx context.Context) {
	// Simple background task placeholder
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Perform maintenance
				s.enforceMemoryLimits()
			}
		}
	}()
}

// enforceMemoryLimits checks current memory usage and triggers eviction if needed.
func (s *VectorStore) enforceMemoryLimits() {
	limit := s.maxMemory.Load()
	current := s.currentMemory.Load()
	if current > limit {
		// Try to evict down to limit
		_ = s.evictToTarget(limit, "")
	}
}

// evictIfNeeded is an alias for enforceMemoryLimits (used by tests)
func (s *VectorStore) evictIfNeeded() {
	s.enforceMemoryLimits()
}

func (s *VectorStore) StartWALCheckTicker(d time.Duration)                                      {}
func (s *VectorStore) UpdateConfig(maxMemory, maxWALSize int64, snapshotInterval time.Duration) {}
func (s *VectorStore) StartMetricsTicker(d time.Duration)                                       {}

// StartEvictionTicker is defined later

// StartIndexingWorkers starts the background indexing workers
func (s *VectorStore) StartIndexingWorkers(numWorkers int) {
	s.startIndexingOnce.Do(func() {
		for i := 0; i < numWorkers; i++ {
			s.indexWg.Add(1)
			go func() {
				defer s.indexWg.Done()
				s.runIndexWorker(nil)
			}()
		}
		s.logger.Info().Int("count", numWorkers).Msg("Started indexing workers")
	})
}

func (s *VectorStore) runIndexWorker(_ memory.Allocator) {
	minBatch := 128
	maxBatch := 1000
	var currentBatch int

	jobs := make([]IndexJob, 0, maxBatch)

	// Dynamic ticker: start standard
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	processBatch := func(group []IndexJob) {
		if len(group) == 0 {
			return
		}
		// log.Printf("[DEBUG] processBatch processing %d jobs", len(group))

		// Sort by dataset to batch index additions
		byDataset := make(map[string][]IndexJob)
		for _, j := range group {
			byDataset[j.DatasetName] = append(byDataset[j.DatasetName], j)
		}

		for dsName, dsGroup := range byDataset {
			func() {
				// Defer recovery to ensure pending count is decremented on panic
				defer func() {
					if r := recover(); r != nil {
						s.logger.Error().Msgf("Panic in index worker for %s: %v", dsName, r)
						// Try to decrement pending jobs if dataset is available
						if ds, ok := s.getDataset(dsName); ok {
							var totalRows int64
							for _, j := range dsGroup {
								totalRows += j.Record.NumRows()
							}
							ds.PendingIndexJobs.Add(-totalRows)
						}
					}
				}()

				ds, ok := s.getDataset(dsName)
				if !ok {
					s.logger.Error().
						Str("dataset", dsName).
						Msg("Dataset not found for indexing job")
					for _, j := range dsGroup {
						if j.Record != nil {
							j.Record.Release()
						}
					}
					return
				}

				// Log start
				s.logger.Debug().Str("dataset", dsName).Int("jobs", len(dsGroup)).Msg("Processing batch starting")

				// Total rows in this group for this dataset
				totalRowsInGroup := 0
				for _, j := range dsGroup {
					totalRowsInGroup += int(j.Record.NumRows())
				}

				recs := make([]arrow.RecordBatch, 0, totalRowsInGroup)
				rowIdxs := make([]int, 0, totalRowsInGroup)
				batchIdxs := make([]int, 0, totalRowsInGroup)
				for _, j := range dsGroup {
					n := int(j.Record.NumRows())
					for r := 0; r < n; r++ {
						recs = append(recs, j.Record)
						rowIdxs = append(rowIdxs, r)
						batchIdxs = append(batchIdxs, j.BatchIdx)
					}
				}

				var docIDs []uint32
				var addErr error
				if ds.Index != nil {
					docIDs, addErr = ds.Index.AddBatch(recs, rowIdxs, batchIdxs)
					if addErr != nil {
						s.logger.Error().
							Str("dataset", dsName).
							Err(addErr).
							Msg("Async batched index add failed")
					} else {
						// Update memory tracking for index overhead
						// Check if Index is still valid (dataset might have been evicted)
						var newIndexSize int64
						if ds.Index != nil {
							newIndexSize = ds.Index.EstimateMemory()
						} else if ds.GetHNSW2Index() != nil {
							if est, ok := ds.GetHNSW2Index().(interface{ EstimateMemory() int64 }); ok {
								newIndexSize = est.EstimateMemory()
							}
						}

						if newIndexSize > 0 {
							oldIndexSize := ds.IndexMemoryBytes.Swap(newIndexSize)
							delta := newIndexSize - oldIndexSize
							if delta != 0 {
								s.currentMemory.Add(delta)
							}
						}
					}
				} else {
					s.logger.Warn().Str("dataset", dsName).Msg("Dataset has no index initialized, skipping AddBatch")
				}

				// Update Inverted Indexes (Hybrid Search)
				if len(docIDs) == totalRowsInGroup {
					docIDIdx := 0
					for _, j := range dsGroup {
						schema := j.Record.Schema()
						numRows := int(j.Record.NumRows())

						// Identify string columns once per batch
						stringCols := make([]int, 0)
						for colIdx, field := range schema.Fields() {
							if field.Type.ID() == arrow.STRING {
								stringCols = append(stringCols, colIdx)
							}
						}

						if len(stringCols) > 0 {
							// Cache BM25 index lookup
							ds.dataMu.RLock()
							bm25 := ds.BM25Index
							ds.dataMu.RUnlock()

							for r := 0; r < numRows; r++ {
								docID := docIDs[docIDIdx]
								docIDIdx++

								for _, colIdx := range stringCols {
									fieldName := schema.Field(colIdx).Name

									// Double-checked locking avoidance: Use RLock first, verify, then Lock?
									// Optimizing for Phase 1: Keep logical structure but potentially minimize critical section?
									// For now keeping existing logic to avoid regression, focusing on Index Worker structure.

									ds.dataMu.RLock()
									var invIdx *InvertedIndex
									if ds.InvertedIndexes != nil {
										invIdx = ds.InvertedIndexes[fieldName]
									}
									ds.dataMu.RUnlock()

									if invIdx == nil {
										ds.dataMu.Lock()
										if ds.InvertedIndexes == nil {
											ds.InvertedIndexes = make(map[string]*InvertedIndex)
										}
										invIdx = ds.InvertedIndexes[fieldName]
										if invIdx == nil {
											invIdx = NewInvertedIndex()
											ds.InvertedIndexes[fieldName] = invIdx
										}
										ds.dataMu.Unlock()
									}

									colI := j.Record.Column(colIdx)
									if col, ok := colI.(*array.String); ok {
										if r < col.Len() && col.IsValid(r) {
											text := col.Value(r)
											invIdx.Add(text, docID)
											if bm25 != nil {
												bm25.Add(VectorID(docID), text)
												metrics.BM25DocumentsIndexedTotal.Inc()
											}
										}
									}
								}
							}
						} else {
							docIDIdx += numRows
						}
					}
				}

				// Release records and record latency
				for _, j := range dsGroup {
					j.Record.Release()
					metrics.IndexJobLatencySeconds.WithLabelValues(dsName).Observe(time.Since(j.CreatedAt).Seconds())

					// Update Memory Pressure on Queue
					// Approximate size calculation matching Send()
					size := int64(j.Record.NumRows() * int64(j.Record.NumCols()) * 8)
					s.indexQueue.DecreaseEstimatedBytes(size)
				}

				// Decrement pending jobs count
				// Decrement pending jobs count
				ds.PendingIndexJobs.Add(int64(-totalRowsInGroup))
				s.logger.Debug().Str("dataset", dsName).Msg("Processing batch finished")
			}()
		}
	}

	for {
		// Adaptive logic
		queueDepth := s.indexQueue.Len()

		if queueDepth > 100 {
			ticker.Reset(1 * time.Millisecond)
			currentBatch = maxBatch
		} else if queueDepth > 10 {
			ticker.Reset(5 * time.Millisecond)
			currentBatch = 500
		} else {
			ticker.Reset(10 * time.Millisecond)
			currentBatch = minBatch
		}

		select {
		case <-s.stopChan:
			if len(jobs) > 0 {
				processBatch(jobs)
			}
			return
		case job, ok := <-s.indexQueue.Jobs():
			if !ok {
				if len(jobs) > 0 {
					processBatch(jobs)
				}
				return
			}
			jobs = append(jobs, job)
			// Process batch if full
			if len(jobs) >= currentBatch {
				processBatch(jobs)
				jobs = jobs[:0]
			}
		case <-ticker.C:
			if len(jobs) > 0 {
				// Process whatever we have
				processBatch(jobs)
				jobs = jobs[:0]
			}
		}
	}
}

// StartEvictionTicker starts the background eviction ticker (used by tests/shutdown)
func (s *VectorStore) StartEvictionTicker(interval time.Duration) {
	s.workerWg.Add(1)
	go func() {
		defer s.workerWg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopChan:
				return
			case <-ticker.C:
				s.enforceMemoryLimits()
			}
		}
	}()
}
