package store

import (
	"context"
	"runtime"
	"runtime/debug"
	"time"

	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// StartIndexingWorkers spawns a specified number of background indexing workers.
// Under DeterministicIngestEnabled, the pool is clamped to a single worker so that
// the order rows land in the graph depends only on the input stream and not on the scheduler.
func (s *VectorStore) StartIndexingWorkers(numWorkers int) {
	s.workerMu.Lock()
	if DeterministicIngestEnabled() {
		if len(s.indexingWorkerCancels) > 0 {
			s.workerMu.Unlock()
			return
		}
		numWorkers = 1
	}
	defer s.workerMu.Unlock()

	for i := 0; i < numWorkers; i++ {
		workerCtx, cancel := context.WithCancel(s.ctx) // #nosec G118
		s.indexingWorkerCancels = append(s.indexingWorkerCancels, cancel)
		s.indexWg.Add(1)
		go func(workerIdx int) {
			defer s.indexWg.Done()
			// Pin to NUMA node if enabled
			if s.numaEnabled && s.numaTopology != nil && s.numaTopology.NumNodes > 0 {
				nodeID := workerIdx % s.numaTopology.NumNodes
				_ = lbmem.PinToNUMANode(s.numaTopology, nodeID)
				s.logger.Debug().Int("worker", workerIdx).Int("node", nodeID).Msg("Pinned index worker to NUMA node")
			}
			s.runIndexWorker(workerCtx)
		}(i)
	}
	s.logger.Info().Int("added", numWorkers).Int("total", len(s.indexingWorkerCancels)).Msg("Started indexing workers")
}

// StopIndexingWorkers stops a specified number of background indexing workers.
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

// StartIngestionWorkers spawns a specified number of background ingestion workers.
func (s *VectorStore) StartIngestionWorkers(count int) {
	if count <= 0 {
		count = runtime.NumCPU()
	}
	s.workerMu.Lock()
	if DeterministicIngestEnabled() {
		if len(s.ingestionWorkerCancels) > 0 {
			s.workerMu.Unlock()
			return
		}
		count = 1
	}
	defer s.workerMu.Unlock()

	for i := 0; i < count; i++ {
		workerCtx, cancel := context.WithCancel(s.ctx) // #nosec G118
		s.ingestionWorkerCancels = append(s.ingestionWorkerCancels, cancel)
		s.workerWg.Add(1)
		go func(workerIdx int) {
			defer s.workerWg.Done()
			// Pin to NUMA node if enabled
			if s.numaEnabled && s.numaTopology != nil && s.numaTopology.NumNodes > 0 {
				nodeID := workerIdx % s.numaTopology.NumNodes
				_ = lbmem.PinToNUMANode(s.numaTopology, nodeID)
				s.logger.Debug().Int("worker", workerIdx).Int("node", nodeID).Msg("Pinned ingestion worker to NUMA node")
			}
			s.runIngestionWorkerWithCtx(workerCtx)
		}(i)
	}
	s.logger.Info().Int("added", count).Int("total", len(s.ingestionWorkerCancels)).Msg("Started ingestion workers")
}

// StopIngestionWorkers stops a specified number of background ingestion workers.
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

// AdjustWorkerCounts scales indexing and ingestion workers dynamically.
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

// StartIngestionAutoscaler starts an adaptive goroutine that dynamically scales ingestion workers.
func (s *VectorStore) StartIngestionAutoscaler(ctx context.Context) {
	s.workerWg.Add(1)
	go func() {
		defer s.workerWg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopChan:
				return
			case <-ticker.C:
				qLen := 0
				if s.ingestionQueue != nil {
					qLen = s.ingestionQueue.Len()
				}
				s.workerMu.Lock()
				currWorkers := len(s.ingestionWorkerCancels)
				s.workerMu.Unlock()

				maxWorkers := runtime.NumCPU() * 2
				if maxWorkers < 4 {
					maxWorkers = 4
				}

				if qLen > 100 && currWorkers < maxWorkers {
					s.StartIngestionWorkers(1)
				} else if qLen == 0 && currWorkers > calculateMinWorkers(4) {
					s.StopIngestionWorkers(1)
				}
			}
		}
	}()
}

func (s *VectorStore) runIndexWorker(ctx context.Context) {
	maxBatch := 16
	currentBatch := 2
	if DeterministicIngestEnabled() {
		maxBatch = 1
		currentBatch = 1
	}

	jobs := make([]IndexJob, 0, maxBatch)
	var lastLogTime time.Time

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	processBatch := func(group []IndexJob) {
		if len(group) == 0 {
			return
		}

		byDataset := make(map[string][]IndexJob)
		for _, j := range group {
			byDataset[j.DatasetName] = append(byDataset[j.DatasetName], j)
		}

		for dsName, dsGroup := range byDataset {
			func() {
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

				totalRowsInGroup := 0
				for _, j := range dsGroup {
					totalRowsInGroup += int(j.Record.NumRows())
				}

				defer func() {
					ds.PendingIndexJobs.Add(int64(-totalRowsInGroup))
				}()

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
					if adaptive, ok := idx.(interface{ SetEfConstruction(int) }); ok && !DeterministicIngestEnabled() {
						depth := s.indexQueue.Len()
						var targetEf int
						switch {
						case depth > 5000:
							targetEf = 100
						case depth > 1000:
							targetEf = 200
						default:
							targetEf = 400
						}
						adaptive.SetEfConstruction(targetEf)
					}

					batchCtx := s.ctx
					if len(dsGroup) > 0 && dsGroup[0].HighPriority {
						batchCtx = types.WithHighPriority(batchCtx)
					}
					docIDs, addErr = idx.AddBatch(batchCtx, recs, rowIdxs, batchIdxs)
					if addErr != nil {
						s.logger.Error().
							Str("dataset", dsName).
							Err(addErr).
							Msg("Async batched index add failed")
					} else {
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

				if len(docIDs) == totalRowsInGroup {
					docIDIdx := 0
					for _, j := range dsGroup {
						schema := j.Record.Schema()
						numRows := int(j.Record.NumRows())

						stringCols := make([]int, 0)
						for colIdx, field := range schema.Fields() {
							if field.Type.ID() == arrow.STRING {
								stringCols = append(stringCols, colIdx)
							}
						}

						if len(stringCols) > 0 {
							invIndexes := make(map[int]*InvertedIndex)

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
							ds.dataMu.Unlock()

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
											invIdx.Add(col.Value(r), docID)
										}
									}
								}
							}
						} else {
							docIDIdx += numRows
						}
					}
				}

				for _, j := range dsGroup {
					j.Record.Release()
					metrics.IndexJobLatencySeconds.WithLabelValues(dsName).Observe(time.Since(j.CreatedAt).Seconds())
				}
			}()
		}
	}

	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	for {
		job, ok := s.indexQueue.Pop()
		if ok {
			jobs = append(jobs, job)
		}

		if len(jobs) == 0 {
			timer.Reset(100 * time.Millisecond)
			select {
			case <-s.stopChan:
				return
			case <-ctx.Done():
				return
			case <-s.indexQueue.Notify():
				continue
			case <-timer.C:
				continue
			}
		}

		if len(jobs) >= currentBatch || (!ok && len(jobs) > 0) {
			processBatch(jobs)
			jobs = jobs[:0]
		}

		if DeterministicIngestEnabled() {
			currentBatch = 1
		} else {
			queueDepth := s.indexQueue.Len()

			switch {
			case queueDepth > 100:
				if time.Since(lastLogTime) > 2*time.Second {
					s.logger.Warn().Int("depth", queueDepth).Msg("Indexing queue is BACKPRESSURED")
					lastLogTime = time.Now()
				}
				currentBatch = 4
			case queueDepth > 20:
				if time.Since(lastLogTime) > 5*time.Second {
					s.logger.Info().Int("depth", queueDepth).Msg("Indexing queue is filling up")
					lastLogTime = time.Now()
				}
				currentBatch = 2
			default:
				currentBatch = 1
			}
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
		}
	}
}
