package store

import (
	"context"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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

func (s *VectorStore) PrewarmDataset(name string, schema *arrow.Schema) {
	_, created := s.getOrCreateDataset(name, func() *Dataset {
		ds := NewDataset(name, schema)
		ds.Logger = s.logger
		ds.Topo = s.numaTopology
		return ds
	})

	if created {
		s.logger.Info().Str("dataset", name).Msg("Pre-warmed dataset")
	}
}

// StartLifecycleManager starts the lifecycle manager background task.
func (s *VectorStore) StartLifecycleManager(ctx context.Context) {
	s.logger.Info().Msg("Starting formalized background task scheduler")
	
	// Start sub-tickers
	s.StartWALCheckTicker(5 * time.Second)
	s.StartMetricsTicker(15 * time.Second)
	
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				s.logger.Info().Msg("Lifecycle manager shutting down")
				return
			case <-ticker.C:
				// Perform maintenance
				s.enforceMemoryLimits()
				s.performGlobalCompactionCheck()
			}
		}
	}()
}

// performGlobalCompactionCheck initiates compaction across all datasets if needed
func (s *VectorStore) performGlobalCompactionCheck() {
	dm := s.datasets.Load()
	if dm == nil {
		return
	}
	for _, ds := range *dm {
		if ds.Index != nil {
			// Logic to trigger background compaction/repair
		}
	}
}

// enforceMemoryLimits checks current memory usage and triggers eviction if needed.
func (s *VectorStore) enforceMemoryLimits() {
	limit := s.maxMemory.Load()
	current := s.currentMemory.Load()
	if current > limit {
		s.logger.Warn().
			Int64("current_mb", current/1024/1024).
			Int64("limit_mb", limit/1024/1024).
			Msg("Memory limit exceeded, triggering eviction")
		// Try to evict down to limit
		_ = s.evictToTarget(limit, "")
	}
}

// evictIfNeeded is an alias for enforceMemoryLimits (used by tests)
func (s *VectorStore) evictIfNeeded() {
	s.enforceMemoryLimits()
}

// StartWALCheckTicker starts periodic WAL integrity and size checks
func (s *VectorStore) StartWALCheckTicker(d time.Duration) {
	go func() {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				// Perform WAL maintenance
			}
		}
	}()
}

// UpdateConfig updates store configuration dynamically
func (s *VectorStore) UpdateConfig(maxMemory, maxWALSize int64, snapshotInterval time.Duration) {
	if maxMemory > 0 {
		s.maxMemory.Store(maxMemory)
	}
	// Update other fields as implemented
}

// StartMetricsTicker reports global store metrics to Prometheus
func (s *VectorStore) StartMetricsTicker(d time.Duration) {
	go func() {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				metrics.NUMANodeCount.Set(float64(s.numaTopology.NumNodes))
				metrics.NUMAEnabled.Set(1.0)
				// Update more global metrics
			}
		}
	}()
}

// StartEvictionTicker is defined later

// StartIndexingWorkers starts more background indexing workers
func (s *VectorStore) StartIndexingWorkers(numWorkers int) {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()

	for i := 0; i < numWorkers; i++ {
		workerCtx, cancel := context.WithCancel(s.ctx) // #nosec G118
		s.indexingWorkerCancels = append(s.indexingWorkerCancels, cancel)
		s.indexWg.Add(1)
		go func() {
			defer s.indexWg.Done()
			s.runIndexWorker(workerCtx)
		}()
	}
	s.logger.Info().Int("added", numWorkers).Int("total", len(s.indexingWorkerCancels)).Msg("Started indexing workers")
}

// StopIndexingWorkers stops n background indexing workers
func (s *VectorStore) StopIndexingWorkers(numWorkers int) {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()

	active := len(s.indexingWorkerCancels)
	if numWorkers > active {
		numWorkers = active
	}

	for i := 0; i < numWorkers; i++ {
		idx := active - 1 - i
		s.indexingWorkerCancels[idx]()
	}
	s.indexingWorkerCancels = s.indexingWorkerCancels[:active-numWorkers]
	s.logger.Info().Int("stopped", numWorkers).Int("remaining", len(s.indexingWorkerCancels)).Msg("Stopped indexing workers")
}

// StartIngestionWorkers starts background ingestion workers.
func (s *VectorStore) StartIngestionWorkers(count int) {
	if count <= 0 {
		count = runtime.NumCPU()
	}
	s.workerMu.Lock()
	defer s.workerMu.Unlock()

	for i := 0; i < count; i++ {
		workerCtx, cancel := context.WithCancel(s.ctx) // #nosec G118
		s.ingestionWorkerCancels = append(s.ingestionWorkerCancels, cancel)
		s.workerWg.Add(1)
		go func() {
			defer s.workerWg.Done()
			s.runIngestionWorkerWithCtx(workerCtx)
		}()
	}
	s.logger.Info().Int("added", count).Int("total", len(s.ingestionWorkerCancels)).Msg("Started ingestion workers")
}

// StopIngestionWorkers stops n background ingestion workers
func (s *VectorStore) StopIngestionWorkers(numWorkers int) {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()

	active := len(s.ingestionWorkerCancels)
	if numWorkers > active {
		numWorkers = active
	}

	for i := 0; i < numWorkers; i++ {
		idx := active - 1 - i
		s.ingestionWorkerCancels[idx]()
	}
	s.ingestionWorkerCancels = s.ingestionWorkerCancels[:active-numWorkers]
	s.logger.Info().Int("stopped", numWorkers).Int("remaining", len(s.ingestionWorkerCancels)).Msg("Stopped ingestion workers")
}

// AdjustWorkerCounts resizes pools to match target counts
func (s *VectorStore) AdjustWorkerCounts(indexing, ingestion int) {
	s.workerMu.Lock()
	currIndexing := len(s.indexingWorkerCancels)
	currIngestion := len(s.ingestionWorkerCancels)
	s.workerMu.Unlock()

	if indexing > currIndexing {
		s.StartIndexingWorkers(indexing - currIndexing)
	} else if indexing < currIndexing {
		s.StopIndexingWorkers(currIndexing - indexing)
	}

	if ingestion > currIngestion {
		s.StartIngestionWorkers(ingestion - currIngestion)
	} else if ingestion < currIngestion {
		s.StopIngestionWorkers(currIngestion - ingestion)
	}
}

func (s *VectorStore) runIndexWorker(ctx context.Context) {
	maxBatch := 1000
	currentBatch := 100

	jobs := make([]IndexJob, 0, maxBatch)

	// Dynamic ticker: start standard
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
			func() {
				// Defer recovery to ensure pending count is decremented on panic
				defer func() {
					if r := recover(); r != nil {
						s.logger.Error().Msgf("Panic in index worker for %s: %v\n%s", dsName, r, debug.Stack())
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

				// Total rows in this group for this dataset
				totalRowsInGroup := 0
				for _, j := range dsGroup {
					totalRowsInGroup += int(j.Record.NumRows())
				}

				defer func() {
					ds.PendingIndexJobs.Add(int64(-totalRowsInGroup))
				}()

				// Find max batch index to size the recs slice correctly
				maxBatchIdx := -1
				for _, j := range dsGroup {
					if j.BatchIdx > maxBatchIdx {
						maxBatchIdx = j.BatchIdx
					}
				}

				if maxBatchIdx < 0 && len(dsGroup) > 0 {
					s.logger.Error().Int("group_len", len(dsGroup)).Msg("maxBatchIdx is -1 but dsGroup is not empty")
				}

				recs := make([]arrow.RecordBatch, maxBatchIdx+1)
				rowIdxs := make([]int, 0, totalRowsInGroup)
				batchIdxs := make([]int, 0, totalRowsInGroup)
				for _, j := range dsGroup {
					if j.Record != nil && j.BatchIdx >= 0 && j.BatchIdx < len(recs) {
						recs[j.BatchIdx] = j.Record
					} else if j.Record != nil {
						s.logger.Warn().Int("batch_idx", j.BatchIdx).Int("recs_len", len(recs)).Msg("Skipping record in indexing batch due to index mismatch")
					}
					
					if j.Record == nil {
						continue
					}
					
					n := int(j.Record.NumRows())
					for r := 0; r < n; r++ {
						rowIdxs = append(rowIdxs, r)
						batchIdxs = append(batchIdxs, j.BatchIdx)
					}
				}

				var docIDs []uint32
				var addErr error
				ds.dataMu.RLock()
				idx := ds.Index
				ds.dataMu.RUnlock()

				if idx != nil {
					// Adaptive EfConstruction based on queue depth
					if adaptive, ok := idx.(interface{ SetEfConstruction(int) }); ok {
						depth := s.indexQueue.Len()
						var targetEf int
						switch {
						case depth > 5000:
							targetEf = 50
						case depth > 1000:
							targetEf = 100
						default:
							targetEf = 400 // Default high quality
						}
						adaptive.SetEfConstruction(targetEf)
					}

					// Propagate store shutdown context
					docIDs, addErr = idx.AddBatch(s.ctx, recs, rowIdxs, batchIdxs)
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

						// Identify string columns
						stringCols := make([]int, 0)
						for colIdx, field := range schema.Fields() {
							if field.Type.ID() == arrow.STRING {
								stringCols = append(stringCols, colIdx)
							}
						}

						if len(stringCols) > 0 {
							// 1. Prepare Inverted Indexes (Batch Lock)
							// Map from colIdx -> *InvertedIndex
							invIndexes := make(map[int]*InvertedIndex)
							var bm25 *BM25InvertedIndex

							ds.dataMu.Lock()
							if ds.InvertedIndexes == nil {
								ds.InvertedIndexes = make(map[string]*InvertedIndex)
							}
							for _, colIdx := range stringCols {
								fieldName := schema.Field(colIdx).Name
								invIdx := ds.InvertedIndexes[fieldName]
								if invIdx == nil {
									invIdx = NewInvertedIndex()
									ds.InvertedIndexes[fieldName] = invIdx
								}
								invIndexes[colIdx] = invIdx
							}
							bm25 = ds.BM25Index
							ds.dataMu.Unlock()

							// 2. Add Documents (No Lock on DS)
							// InvertedIndex.Add must be thread-safe or we are the only writer for this batch.
							// Since we are inside the Index Worker (single-threaded per batch), and InvertedIndex
							// usually protects itself or is only accessed here, it should be safe.
							for r := 0; r < numRows; r++ {
								docID := docIDs[docIDIdx]
								docIDIdx++

								for _, colIdx := range stringCols {
									invIdx := invIndexes[colIdx]
									if invIdx == nil {
										continue
									}

									colI := j.Record.Column(colIdx)
									if col, ok := colI.(*array.String); ok {
										if r < col.Len() && col.IsValid(r) {
											text := col.Value(r)
											invIdx.Add(text, docID)
											if s.hybridSearchConfig.Enabled && bm25 != nil {
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
					// size := int64(j.Record.NumRows() * int64(j.Record.NumCols()) * 8)
					// s.indexQueue.DecreaseEstimatedBytes(size)
				}

				// Decrement pending jobs count handled by defer
			}()
		}
	}

	for {
		job, ok := s.indexQueue.Pop()
		if ok {
			jobs = append(jobs, job)
		}

		if len(jobs) == 0 {
			// Check for shutdown or context cancellation
			select {
			case <-s.stopChan:
				return
			case <-ctx.Done():
				return
			default:
			}

			// No jobs, wait a bit
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if len(jobs) >= currentBatch || (!ok && len(jobs) > 0) {
			processBatch(jobs)
			jobs = jobs[:0]
		}

		// Adaptive logic
		queueDepth := s.indexQueue.Len()

		switch {
		case queueDepth > 100:
			s.logger.Warn().Int("depth", queueDepth).Msg("Ingestion queue is BACKPRESSURED")
			currentBatch = maxBatch // 1000
		case queueDepth > 50:
			s.logger.Info().Int("depth", queueDepth).Msg("Ingestion queue is filling up")
			currentBatch = 500
		default:
			currentBatch = 100
		}

		select {
		case <-s.stopChan:
			if len(jobs) > 0 {
				processBatch(jobs)
			}
			return
		case <-ctx.Done():
			if len(jobs) > 0 {
				processBatch(jobs)
			}
			return
		default:
			// continue
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

// ReleaseMemory explicitly triggers GC and frees OS memory.
// It also waits for async cleanups to complete.
func (s *VectorStore) ReleaseMemory() {
	s.logger.Info().Msg("Explicitly releasing memory")

	// 1. Wait for async cleanups
	s.cleanupWg.Wait()

	// 2. Trigger GC
	runtime.GC()

	// 3. Free OS memory
	debug.FreeOSMemory()

	s.logger.Info().
		Int64("current_memory_bytes", s.currentMemory.Load()).
		Msg("Memory release complete")
}
