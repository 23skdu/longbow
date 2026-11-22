package store

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	lmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// StoreRecordBatch ingests a record batch into the specified dataset.
func (s *VectorStore) StoreRecordBatch(ctx context.Context, name string, rec arrow.RecordBatch) error {
	if rec == nil {
		return errors.New("nil record batch")
	}
	if err := s.GetSchemaRegistry().ValidateBatch(ctx, name, rec); err != nil {
		return err
	}
	ts := time.Now().UnixNano()

	ds, _ := s.getOrCreateDataset(name, func() *Dataset {
		d := NewDataset(name, rec.Schema())
		d.Logger = s.logger
		return d
	})

	rec.Retain() // For Persistence
	rec.Retain() // For Ingestion
	metrics.IngestionLagCount.Add(float64(rec.NumRows()))

	select {
	case s.persistenceQueue <- persistenceJob{datasetName: name, batch: rec, ts: ts}:
	case <-ctx.Done():
		rec.Release()
		rec.Release()
		return ctx.Err()
	}

	// Track pending ingestion BEFORE enqueuing to fix WaitForIndexing races
	ds.PendingIngestion.Add(1)
	s.federatedDirty.Add(1)

	// Dispatch for ingestion
	if !s.ingestionQueue.PushBlocking(IngestionJob{DS: ds, Batch: rec, TS: ts}, 10*time.Second) {
		ds.PendingIngestion.Add(-1)
		return errors.New("failed to enqueue ingestion job")
	}

	return nil
}

// applyBatchToMemory applies a batch to the in-memory dataset and dispatches indexing
func (s *VectorStore) applyBatchToMemory(ds *Dataset, rec arrow.RecordBatch, ts int64) error {
	name := ds.Name
	traceStart := time.Now()
	traceLog := func(step string) {
		if elapsed := time.Since(traceStart); elapsed > 5*time.Second {
			s.logger.Warn().
				Str("dataset", name).
				Str("step", step).
				Int64("elapsed_ms", elapsed.Milliseconds()).
				Msg("applyBatchToMemory trace: slow step")
		}
	}

	// Memory tracking
	batchSize := estimateBatchSize(rec)
	// Check memory limit
	if err := s.checkMemoryBeforeWrite(batchSize, name); err != nil {
		return err
	}
	traceLog("checkMemoryBeforeWrite")

	// Auto-quantization under memory pressure (>60% capacity).
	// Threshold intentionally lowered from 70% to 60% to give the compression path
	// a wider window before the heap is exhausted — preventing ResourceExhausted at ~425K vectors.
	s.applyMemoryPressureQuantization(ds)

	metrics.DoPutPayloadSizeBytes.Observe(float64(batchSize))

	if batchSize > 100*1024*1024 {
		s.logger.Warn().Int64("size", batchSize).Msg("Large memory addition in DoPut")
	}
	s.currentMemory.Add(batchSize)
	ds.SizeBytes.Add(batchSize)
	metrics.FlightRowsProcessed.WithLabelValues("put", "ok").Add(float64(rec.NumRows()))

	// Extract IDs and Vectors outside lock for better concurrency
	idMap := ds.ExtractIDs(rec)

	// Determine vector column for DiskStore (Zero-Copy Persistence)
	diskVecColIdx := s.ensureBatchDiskStore(ds, name, rec.Schema())

	dsLockStart := time.Now()

	// dataMu leak-guard: always unlock on panic so subsequent ingestion workers
	// don't block forever at dataMu.Lock(). The explicit Unlock at the end of the
	// critical section clears the flag so the defer is a no-op on the normal path.
	var dataMuLocked bool
	defer func() {
		if dataMuLocked {
			s.logger.Error().
				Str("dataset", name).
				Msg("dataMu was locked at panic time — recovering and unlocking to prevent worker deadlock")
			ds.dataMu.Unlock()
		}
	}()
	s.datasetLockWaiters.Add(1)
	ds.dataMu.Lock()
	s.datasetLockWaiters.Add(-1)
	dataMuLocked = true

	s.lazyInitDatasetIndexLocked(ds, name, rec)

	currCPU := lmem.GetCurrentCPU()
	currNode := -1
	if s.numaTopology != nil {
		currNode = s.numaTopology.GetNodeForCPU(currCPU)
	}

	// Global row offset of this batch, read from the running counter rather
	// than summed over every historical batch. The old O(N) loop sat inside
	// the dataset write lock, so ingestion latency grew super-linearly with
	// dataset size.
	baseRowID := ds.TotalIngestedRows.Load()

	// Amortised O(1) append: Records and BatchNodes grow geometrically and the
	// row counter is advanced in place, replacing three O(N) passes (two
	// reallocations plus the row sum) under the same lock.
	batchIdx := ds.appendRecordLocked(rec, currNode)
	rec.Retain()

	// Index categorical string columns into 16-bit dictionary codes for vectorized SIMD filter evaluation
	ds.indexCategoricalColumns(rec, batchIdx)

	// Mark dataset as ready after first successful ingestion
	if !ds.IsReady.Load() {
		ds.IsReady.Store(true)
		s.logger.Info().Str("dataset", name).Msg("Dataset metadata registration complete (Ready for queries)")
	}

	metrics.DatasetLockWaitDurationSeconds.WithLabelValues("put").Observe(time.Since(dsLockStart).Seconds())

	ds.dataMu.Unlock()
	dataMuLocked = false
	traceLog("dataMu critical section")

	s.dispatchSecondaryIngestHooks(ds, rec, batchIdx, idMap, baseRowID, ts, diskVecColIdx, traceLog)
	s.enqueueBatchIndexJob(ds, name, rec, batchIdx, currNode)

	return nil
}

// lazyInitDatasetIndexLocked creates or recreates the primary vector index while ds.dataMu is held.
func (s *VectorStore) lazyInitDatasetIndexLocked(ds *Dataset, name string, rec arrow.RecordBatch) {
	// Lazy Index Initialization
	// Also check if existing index has wrong DataType (e.g. Pre-warmed with Float32 but data is Float64)
	var needsReindex bool
	if ds.Index != nil {
		if hnsw, ok := ds.Index.(*ArrowHNSW); ok {
			for _, f := range ds.Schema.Fields() {
				if f.Name == "vector" {
					break
				}
			}
			wantType := InferVectorDataType(ds.Schema, "vector")
			if hnsw.GetConfig().DataType != wantType {
				s.logger.Info().Str("dataset", name).Str("have", hnsw.GetConfig().DataType.String()).Str("want", wantType.String()).Msg("Re-creating index for DataType mismatch")
				needsReindex = true
			}
		}
	}
	if ds.Index == nil || needsReindex {
		ds.initMu.Lock()
		needsReindex = false
		if ds.Index != nil {
			if hnsw, ok := ds.Index.(*ArrowHNSW); ok {
				wantType := InferVectorDataType(ds.Schema, "vector")
				if hnsw.GetConfig().DataType != wantType {
					needsReindex = true
				}
			}
		}
		if ds.Index == nil || needsReindex {
			s.logger.Info().Str("dataset", name).Msg("Attempting lazy index initialization")
			config := s.autoShardingConfig
			if config.ShardThreshold == 0 {
				config.ShardThreshold = 10000
				config.Enabled = true
				config.ShardCount = runtime.NumCPU()
			}

			// Infer DataType from the FIRST record
			vecColName := "vector"
			for _, f := range rec.Schema().Fields() {
				if f.Name == "vector" || f.Name == "embedding" {
					vecColName = f.Name
					break
				}
			}
			dataType := InferVectorDataType(rec.Schema(), vecColName)

			// Unspecified default logic: promote to turboquant8
			hasMetadataType := false
			if rec.Schema() != nil {
				md := rec.Schema().Metadata()
				if _, ok := md.GetValue("longbow.vector_type"); ok {
					hasMetadataType = true
				} else {
					idx := rec.Schema().FieldIndices(vecColName)
					if len(idx) > 0 {
						f := rec.Schema().Field(idx[0])
						if _, ok := f.Metadata.GetValue("longbow.vector_type"); ok {
							hasMetadataType = true
						}
					}
				}
			}

			// Infer dimension
			dim := 0
			if vecCol := findVectorColumn(rec); vecCol != nil {
				if listArr, ok := vecCol.(*array.FixedSizeList); ok {
					dim = int(listArr.DataType().(*arrow.FixedSizeListType).Len())
				}
			}

			// Automatic TurboQuant standardization for high-scale (500k+) or memory-constrained infrastructure
			autoQuantizeEnabled := os.Getenv("LONGBOW_AUTO_QUANTIZE") == "1" || os.Getenv("LONGBOW_AUTO_QUANTIZE") == "true"
			if config.IndexConfig != nil && config.IndexConfig.AutoQuantize {
				autoQuantizeEnabled = true
			}

			if autoQuantizeEnabled && !hasMetadataType {
				threshold := int64(100000)
				if config.IndexConfig != nil && config.IndexConfig.AutoQuantizeThreshold > 0 {
					threshold = config.IndexConfig.AutoQuantizeThreshold
				}
				maxRAM := s.maxMemory.Load()
				if maxRAM <= 0 {
					maxRAM = lmem.GetPhysicalMemory()
				}
				estimatedCount := ds.GetRowCount() + rec.NumRows()
				if estimatedCount < threshold && maxRAM <= 24*1024*1024*1024 {
					estimatedCount = threshold
				}
				if ShouldAutoQuantize(estimatedCount, dim, dataType, maxRAM, threshold) {
					dataType = types.VectorTypeTQ
					ds.SetPreferredVectorType(types.VectorTypeTQ)
					tqBits := 4
					if config.IndexConfig != nil && config.IndexConfig.AutoQuantizeBits > 0 {
						tqBits = config.IndexConfig.AutoQuantizeBits
					}
					if v := os.Getenv("LONGBOW_AUTO_QUANTIZE_BITS"); v != "" {
						if b, err := strconv.Atoi(v); err == nil && (b == 2 || b == 4 || b == 8) {
							tqBits = b
						}
					}
					ds.SetTurboQuantBits(tqBits)
					if ds.DiskStore != nil {
						ds.DiskStore.SetTurboQuant(tqBits)
					}
					metrics.AutoQuantizeEngaged.WithLabelValues(name).Inc()
					s.logger.Info().
						Str("dataset", name).
						Int64("estimatedVectors", estimatedCount).
						Int("dim", dim).
						Int("tqBits", tqBits).
						Msg("Standardized on TurboQuant as default storage engine (LONGBOW_AUTO_QUANTIZE threshold crossed)")
				}
			}

			if pvt := ds.GetPreferredVectorType(); pvt != types.VectorTypeUnknown {
				dataType = pvt
			} else if !hasMetadataType && dataType == types.VectorTypeFloat32 {
				dataType = types.VectorTypeTQ
				ds.SetPreferredVectorType(types.VectorTypeTQ)
			}

			// Ensure automatic spill to disk is engaged if memory projection exceeds threshold
			if ds.DiskStore == nil && s.ShouldSpillToDisk(ds, rec.Schema()) {
				s.initDiskStore(ds, name, rec.Schema())
			}

			s.logger.Info().Str("dataset", name).Str("dataType", dataType.String()).Str("column", vecColName).Msg("Inferred vector data type for new index")

			if config.IndexConfig == nil {
				hnswCfg := DefaultArrowHNSWConfig()
				hnswCfg.Metric = ds.Metric
				hnswCfg.DataType = dataType
				if ds.DiskStore != nil {
					hnswCfg.UseDisk = true
				}

				if dataType == VectorTypeTQ {
					hnswCfg.TurboQuantEnabled = true
					if ds.TurboQuantBits() > 0 {
						hnswCfg.TurboQuantBits = ds.TurboQuantBits()
					} else if hnswCfg.TurboQuantBits == 0 {
						hnswCfg.TurboQuantBits = 4
					}
				}
				config.IndexConfig = &hnswCfg
			} else {
				// Clone the config to avoid polluting the shared autoShardingConfig
				clonedCfg := *config.IndexConfig

				// Use preferred type if specified
				if pvt := ds.GetPreferredVectorType(); pvt != types.VectorTypeUnknown {
					dataType = pvt
				}
				clonedCfg.DataType = dataType
				if ds.DiskStore != nil {
					clonedCfg.UseDisk = true
				}

				if dataType == VectorTypeTQ {
					clonedCfg.TurboQuantEnabled = true
					if ds.TurboQuantBits() > 0 {
						clonedCfg.TurboQuantBits = ds.TurboQuantBits()
					} else if clonedCfg.TurboQuantBits == 0 {
						clonedCfg.TurboQuantBits = 4
					}
				}
				config.IndexConfig = &clonedCfg
			}

			aIdx := NewAutoShardingIndex(ds, config)
			if vecCol := findVectorColumn(rec); vecCol != nil {
				if listArr, ok := vecCol.(*array.FixedSizeList); ok {
					dim := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
					s.logger.Info().Str("dataset", name).Int("dim", dim).Str("dataType", dataType.String()).Msg("findVectorColumn result")
					switch dataType {
					case VectorTypeFloat32:
						if listType, ok := listArr.DataType().(*arrow.FixedSizeListType); ok {
							if listType.Elem().ID() == arrow.FLOAT32 && dim%2 == 0 {
								// Only detect complex if field name suggests it
								if strings.Contains(strings.ToLower(vecCol.Data().DataType().Name()), "complex") {
									dataType = VectorTypeComplex64
									dim /= 2
									config.IndexConfig.DataType = dataType
									aIdx = NewAutoShardingIndex(ds, config)
									s.logger.Info().Str("dataset", name).Int("dim", dim).Str("dataType", dataType.String()).Msg("Detected complex64 from physical dimension")
								}
							}
						}
					case VectorTypeComplex64, VectorTypeComplex128:
						dim /= 2
					}
					if setter, ok := aIdx.(interface{ SetInitialDimension(int) }); ok {
						setter.SetInitialDimension(dim)
					}
				}
			} else {
				s.logger.Error().Str("dataset", name).Msg("findVectorColumn returned nil")
			}
			ds.Index = aIdx

			// Pre-warm the index metadata cache with the current schema so the first
			// AddBatch call doesn't pay lazy-cache population latency.
			if hnsw, ok := aIdx.(*ArrowHNSW); ok {
				hnsw.PreWarmMetadata(rec.Schema())
			} else if asi, ok := aIdx.(*AutoShardingIndex); ok {
				asi.mu.RLock()
				if current, ok := asi.current.(*ArrowHNSW); ok {
					current.PreWarmMetadata(rec.Schema())
				}
				asi.mu.RUnlock()
			}
			s.initGPUIfEnabled(aIdx)
		}
		ds.initMu.Unlock()
	}
}
