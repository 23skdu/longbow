package store

import (
	"context"
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

	// metrics.DatasetCount.Dec()
	// metrics.EvaluatedEvictions.Inc()
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
		_ = s.evictToTarget(limit)
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
	for i := 0; i < numWorkers; i++ {
		s.indexWg.Add(1)
		go func() {
			defer s.indexWg.Done()
			s.runIndexWorker(nil)
		}()
	}
	s.logger.Info().Int("count", numWorkers).Msg("Started indexing workers")
}

func (s *VectorStore) runIndexWorker(_ memory.Allocator) {
	batchSize := 128
	jobs := make([]IndexJob, 0, batchSize)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	processBatch := func(group []IndexJob) {
		if len(group) == 0 {
			return
		}

		// Sort by dataset to batch index additions
		byDataset := make(map[string][]IndexJob)
		for _, j := range group {
			byDataset[j.DatasetName] = append(byDataset[j.DatasetName], j)
		}

		for dsName, dsGroup := range byDataset {
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
				continue
			}

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
				// No vector index, but we still need docIDs if we want to support keyword indexing?
				// Actually, if there is no HNSW index, we usually don't have docIDs.
				// But we can fallback to using batchIdx/rowIdx as unique ID if needed,
				// or just skip keyword indexing if no vector index exists.
				// Most datasets in Longbow have at least one vector column.
				s.logger.Warn().Str("dataset", dsName).Msg("Dataset has no index initialized, skipping AddBatch")
			}

			// Update Inverted Indexes (Hybrid Search)
			// For simplicity/safety, skip inverted index if docIDs mismatch total row length
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

								// Double-checked locking for per-column inverted index
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
			}
		}
	}
	for {
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
			if len(jobs) >= 1000 { // Large batch for throughput
				processBatch(jobs)
				jobs = jobs[:0]
			}
		case <-ticker.C:
			if len(jobs) > 0 {
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
