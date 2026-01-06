package store

import (
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/metrics"
)

func NewVectorStore(mem memory.Allocator, logger zerolog.Logger, maxMemory, maxWALSize int64, ttl time.Duration) *VectorStore {
	cfg := DefaultIndexJobQueueConfig()

	// Detect NUMA topology (Phase 4/5)
	topo, err := DetectNUMATopology()
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to detect NUMA topology")
		topo = &NUMATopology{NumNodes: 1}
	}
	if topo.NumNodes > 1 {
		logger.Info().
			Int("nodes", topo.NumNodes).
			Str("topology", topo.String()).
			Msg("NUMA topology detected")
	}

	if logger.GetLevel() == zerolog.Disabled {
		logger = zerolog.Nop()
	}

	s := &VectorStore{
		mem:                mem,
		logger:             logger,
		datasets:           make(map[string]*Dataset),
		indexQueue:         NewIndexJobQueue(cfg),
		meshStatusCache:    NewMeshStatusCache(100 * time.Millisecond), // Cache mesh status for 100ms
		numaTopology:       topo,
		hybridSearchConfig: DefaultHybridSearchConfig(),
		columnIndex:        NewColumnInvertedIndex(),
		compactionConfig:   DefaultCompactionConfig(),
		autoShardingConfig: DefaultAutoShardingConfig(),
		nsManager:          newNamespaceManager(),
		stopChan:           make(chan struct{}),
		snapshotReset:      make(chan time.Duration, 1),
		memoryConfig:       DefaultMemoryConfig(),
	}
	s.maxMemory.Store(maxMemory)
	s.memoryConfig.MaxMemory = maxMemory
	// Initialize global BM25 if needed (default disabled)
	if s.hybridSearchConfig.Enabled {
		s.bm25Index = NewBM25InvertedIndex(s.hybridSearchConfig.BM25)
	}

	// Initialize compaction if enabled (Phase 11/14)
	if s.compactionConfig.Enabled {
		s.compactionWorker = NewCompactionWorker(s, s.compactionConfig)
		s.compactionWorker.Start()
	}

	// Start workers based on CPU count (Phase 5: Scale up)
	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 1
	}
	s.startNUMAWorkers(numWorkers)

	// Start memory metrics updater
	s.startMemoryMetricsUpdater()

	return s
}

func (s *VectorStore) StartEvictionTicker(d time.Duration) {
	ticker := time.NewTicker(d)
	go func() {
		for range ticker.C {
			s.evictIfNeeded()
		}
	}()
}

func (s *VectorStore) evictIfNeeded() {
	curr := s.currentMemory.Load()
	limit := s.maxMemory.Load()
	if curr <= limit {
		return
	}

	// Snapshot datasets list safely
	s.mu.RLock()
	candidates := make([]*Dataset, 0, len(s.datasets))
	for _, ds := range s.datasets {
		candidates = append(candidates, ds)
	}
	s.mu.RUnlock()

	// Sort by LastAccess (Ascending = Oldest first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].LastAccess().Before(candidates[j].LastAccess())
	})

	// Evict until memory < limit
	for _, ds := range candidates {
		if s.currentMemory.Load() <= limit {
			break
		}
		s.evictDataset(ds)
	}
}

func (s *VectorStore) evictDataset(ds *Dataset) {
	// 1. Mark as evicting BEFORE acquiring lock
	// This allows in-flight queries to detect eviction and fail gracefully
	if !ds.evicting.CompareAndSwap(false, true) {
		return // Already evicting
	}

	// 2. Acquire WRITE lock (blocks all readers)
	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()

	size := ds.SizeBytes.Load()
	if size == 0 {
		ds.evicting.Store(false) // Reset flag
		return                   // Already empty or not tracked
	}

	s.logger.Warn().
		Str("dataset", ds.Name).
		Int64("size_bytes", size).
		Msg("EVICTION TRIGGERED")

	// 3. Clear records (safe now - no readers due to write lock)
	for _, rec := range ds.Records {
		rec.Release()
	}
	ds.Records = nil
	ds.Tombstones = make(map[int]*Bitset)
	ds.Index = nil

	// 4. Update tracking
	ds.SizeBytes.Store(0)
	s.currentMemory.Add(-size)
	metrics.EvictionsTotal.WithLabelValues("lru").Inc()

	// Note: evicting flag stays true to prevent future queries
}

func (s *VectorStore) StartWALCheckTicker(d time.Duration)                                      {}
func (s *VectorStore) UpdateConfig(maxMemory, maxWALSize int64, snapshotInterval time.Duration) {}
func (s *VectorStore) StartMetricsTicker(d time.Duration)                                       {}

func (s *VectorStore) startNUMAWorkers(numWorkers int) {
	if s.numaTopology == nil || s.numaTopology.NumNodes <= 1 {
		// Fallback to regular workers
		s.startIndexingWorkers(numWorkers)
		return
	}

	// Distribute workers across NUMA nodes
	workersPerNode := numWorkers / s.numaTopology.NumNodes
	if workersPerNode < 1 {
		workersPerNode = 1
	}

	// Adjust total to account for rounding
	totalStarted := 0

	for nodeID := 0; nodeID < s.numaTopology.NumNodes; nodeID++ {
		for i := 0; i < workersPerNode; i++ {
			s.indexWg.Add(1)
			totalStarted++
			go func(node int) {
				defer s.indexWg.Done()

				// Pin to NUMA node
				if err := PinToNUMANode(s.numaTopology, node); err != nil {
					s.logger.Warn().
						Int("node", node).
						Err(err).
						Msg("Failed to pin to NUMA node")
				}

				// Create NUMA-aware allocator (future use for allocations)
				allocator := NewNUMAAllocator(s.numaTopology, node)

				metrics.NumaWorkerDistribution.WithLabelValues(strconv.Itoa(node)).Inc()
				s.logger.Info().Int("node", node).Msg("Started NUMA-aware worker")

				s.runIndexWorker(allocator)
			}(nodeID)
		}
	}
	s.logger.Info().
		Int("count", totalStarted).
		Int("nodes", s.numaTopology.NumNodes).
		Msg("Started NUMA indexing workers")
}

func (s *VectorStore) startIndexingWorkers(numWorkers int) {
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
			ds, err := s.getDataset(dsName)
			if err != nil {
				s.logger.Error().
					Str("dataset", dsName).
					Err(err).
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
				processBatch(jobs)
				return
			}
			jobs = append(jobs, job)
			metrics.IndexQueueDepth.Set(float64(s.indexQueue.Len()))
			if len(jobs) >= batchSize {
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
