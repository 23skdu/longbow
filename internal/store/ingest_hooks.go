package store

import (
	"context"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// applyMemoryPressureQuantization promotes a dataset to TurboQuant8 under >60% memory pressure.
func (s *VectorStore) applyMemoryPressureQuantization(ds *Dataset) {
	maxMem := s.maxMemory.Load()
	if maxMem > 0 && s.currentMemory.Load() > int64(float64(maxMem)*0.60) {
		pvt := ds.GetPreferredVectorType()
		if pvt == types.VectorTypeFloat32 || pvt == types.VectorTypeUnknown {
			s.logger.Warn().Str("dataset", ds.Name).Msg("Memory pressure >60%. Dynamically promoting dataset to TurboQuant8.")
			ds.SetPreferredVectorType(types.VectorTypeTQ)

			if h, ok := ds.Index.(*ArrowHNSW); ok {
				h.EnableTurboQuant(8)
			} else if asi, ok := ds.Index.(*AutoShardingIndex); ok {
				asi.mu.Lock()
				if h, ok := asi.current.(*ArrowHNSW); ok {
					h.EnableTurboQuant(8)
				} else if sh, ok := asi.current.(*ShardedHNSW); ok {
					for _, shardIdx := range sh.Shards() {
						if ah, ok := shardIdx.(*ArrowHNSW); ok {
							ah.EnableTurboQuant(8)
						}
					}
				}
				asi.mu.Unlock()
			} else if sh, ok := ds.Index.(*ShardedHNSW); ok {
				for _, shardIdx := range sh.Shards() {
					if ah, ok := shardIdx.(*ArrowHNSW); ok {
						ah.EnableTurboQuant(8)
					}
				}
			}
		}
	}
}

// ensureBatchDiskStore initializes DiskStore on demand and returns the vector column index if active.
func (s *VectorStore) ensureBatchDiskStore(ds *Dataset, name string, schema *arrow.Schema) int {
	ds.initMu.Lock()
	needsDiskInit := ds.DiskStore == nil
	ds.initMu.Unlock()
	if needsDiskInit && s.ShouldSpillToDisk(ds, schema) {
		s.initDiskStore(ds, name, schema)
	}
	ds.initMu.Lock()
	hasDisk := ds.DiskStore != nil
	ds.initMu.Unlock()
	if !hasDisk {
		return -1
	}
	for i, f := range schema.Fields() {
		if f.Name == "vector" || f.Name == "embedding" {
			return i
		}
	}
	return -1
}

// dispatchSecondaryIngestHooks runs asynchronous/post-lock secondary indexing hooks after dataMu release.
func (s *VectorStore) dispatchSecondaryIngestHooks(ds *Dataset, rec arrow.RecordBatch, batchIdx int, idMap *IDMap, baseRowID, ts int64, diskVecColIdx int, traceLog func(string)) {
	name := ds.Name
	// Hybrid text indexing runs on the background text indexer. Tokenising
	// every text cell and rewriting the BM25 dictionaries is orders of
	// magnitude more expensive than publishing the record, and it used to run
	// inline here, stalling every other ingestion worker on the dataset lock.
	s.enqueueTextIndexing(ds, rec, uint32(baseRowID)) // #nosec G115 -- batches cannot exceed 4B rows

	// Update Primary Index and LWW/Merkle outside the main lock to prevent search-blocking
	// contention while processing O(N) metadata updates.
	// metadataMu inside these methods ensures consistency.
	ds.UpdatePrimaryIndexAsync(batchIdx, idMap)
	idMap.Release()
	s.updateLWWAndMerkle(ds, rec, ts)
	s.featuresMu.Lock()
	fs := s.features
	var mv *MaterializedViewManager
	if fs != nil && fs.matViews != nil {
		mv = fs.matViews
	}
	s.featuresMu.Unlock()
	if mv != nil {
		mv.OnDatasetIngest(context.Background(), name)
	}

	s.appendDiskStoreBatch(ds, name, rec, diskVecColIdx)
	s.updateTemporalIndexForBatch(ds, rec, baseRowID, ts)
	traceLog("temporal index")
	s.updateGeoIndexForBatch(ds, rec, baseRowID)
}

func (s *VectorStore) appendDiskStoreBatch(ds *Dataset, name string, rec arrow.RecordBatch, diskVecColIdx int) {
	// Batch append to DiskStore outside main dataset lock to avoid blocking other workers
	if diskVecColIdx != -1 {
		if _, err := ds.DiskStore.BatchAppendArrow(rec, diskVecColIdx); err != nil {
			s.logger.Error().Err(err).Msg("Failed to batch append to DiskStore (Zero-Copy)")
		} else {
			elemSize := 4
			if ds.GetPreferredVectorType() == types.VectorTypeFloat64 {
				elemSize = 8
			}
			metrics.DiskStoreWriteBytesTotal.WithLabelValues(name).Add(float64(rec.NumRows() * int64(ds.DiskStore.dim) * int64(elemSize)))
		}
	}
}

func (s *VectorStore) updateTemporalIndexForBatch(ds *Dataset, rec arrow.RecordBatch, baseRowID, ts int64) {
	// Temporal Index Hook
	if s.temporalConfig.Enabled && ds.TemporalIndex != nil {
		idColIdx := -1
		vecColIdx := -1
		tsColIdx := -1
		for i, f := range rec.Schema().Fields() {
			switch f.Name {
			case "id":
				idColIdx = i
			case "vector", "embedding":
				vecColIdx = i
			case "timestamp":
				tsColIdx = i
			}
		}

		if vecColIdx != -1 {
			numRows := int(rec.NumRows())
			ids := make([]uint64, 0, numRows)
			norms := make([]float32, 0, numRows)
			timestamps := make([]int64, 0, numRows)

			var idCol arrow.Array
			if idColIdx != -1 {
				idCol = rec.Column(idColIdx)
			}
			vecCol := rec.Column(vecColIdx)

			var tsArr arrow.Array
			if tsColIdx != -1 {
				tsArr = rec.Column(tsColIdx)
			}

			var listLen int
			if fs, ok := vecCol.DataType().(*arrow.FixedSizeListType); ok {
				listLen = int(fs.Len())
			}

			// Compute L2 norms directly from the Arrow buffer without allocating
			// duplicate per-row []float32 copies on the Go heap (vectors are already
			// stored off-heap in ds.Index).
			for i := 0; i < numRows; i++ {
				if !vecCol.IsValid(i) || (idCol != nil && !idCol.IsValid(i)) {
					continue
				}
				rowTs := ts
				if tsArr != nil && tsArr.IsValid(i) {
					switch arr := tsArr.(type) {
					case *array.Int64:
						rowTs = arr.Value(i)
					case *array.Timestamp:
						rowTs = int64(arr.Value(i))
					}
				}
				norm, ok := computeVectorNormAt(vecCol, listLen, i)
				if !ok {
					continue
				}
				ids = append(ids, uint64(baseRowID)+uint64(i))
				norms = append(norms, norm)
				timestamps = append(timestamps, rowTs)
			}

			// Batch Add (norm-only to avoid multi-GB duplicate Go-heap vector copies at 1M scale)
			if len(ids) > 0 {
				_ = ds.TemporalIndex.AddBatchNorms(ids, norms, timestamps, listLen)
			}
		}
	}
}

func (s *VectorStore) updateGeoIndexForBatch(ds *Dataset, rec arrow.RecordBatch, baseRowID int64) {
	var dataMuLocked bool
	defer func() {
		if dataMuLocked {
			s.logger.Error().Str("dataset", ds.Name).Msg("dataMu was locked at panic time in geo hook — recovering and unlocking")
			ds.dataMu.Unlock()
		}
	}()
	// Geospatial Index Hook
	geoPointIdx := -1
	for i, f := range rec.Schema().Fields() {
		if f.Name == "geo_point" {
			geoPointIdx = i
			break
		}
	}

	if geoPointIdx != -1 {
		ds.dataMu.Lock()
		dataMuLocked = true
		if ds.GeoIndex == nil {
			// Initialize with default config if missing
			geoCfg := &GeoSearchConfig{
				DistanceType: GeoDistanceHaversine,
				EarthRadius:  6371.0,
			}
			// Use dimension from vector column if possible, otherwise default to 128
			dim := 128
			if vecCol := findVectorColumn(rec); vecCol != nil {
				if listArr, ok := vecCol.(*array.FixedSizeList); ok {
					dim = int(listArr.DataType().(*arrow.FixedSizeListType).Len())
				}
			}
			ds.GeoIndex = NewGeoIndex(ds.Name, dim, geoCfg)
			ds.GeoIndex.ds = ds
			s.logger.Info().Str("dataset", ds.Name).Int("dim", dim).Msg("Lazily initialized GeoIndex")
		}
		ds.dataMu.Unlock()
		dataMuLocked = false

		idColIdx := -1
		vecColIdx := -1
		for i, f := range rec.Schema().Fields() {
			switch f.Name {
			case "id":
				idColIdx = i
			case "vector", "embedding":
				vecColIdx = i
			}
		}

		if vecColIdx != -1 {
			numRows := int(rec.NumRows())
			validIds := make([]uint64, 0, numRows)
			validPoints := make([]types.GeoPoint, 0, numRows)

			var idCol arrow.Array
			if idColIdx != -1 {
				idCol = rec.Column(idColIdx)
			}
			vecCol := rec.Column(vecColIdx)

			if geoArr, ok := rec.Column(geoPointIdx).(*array.FixedSizeList); ok {
				if geoValArr, ok := geoArr.ListValues().(*array.Float64); ok {
					geoValues := geoValArr.Float64Values()
					for i := 0; i < numRows; i++ {
						if (idCol != nil && !idCol.IsValid(i)) || !vecCol.IsValid(i) || !geoArr.IsValid(i) {
							continue
						}
						if i*2+1 >= len(geoValues) {
							continue
						}
						validIds = append(validIds, uint64(baseRowID)+uint64(i))
						validPoints = append(validPoints, types.GeoPoint{Lat: geoValues[i*2], Lon: geoValues[i*2+1]})
					}
					// Pass nil vectors to avoid duplicating 1M []float32 copies on the Go heap;
					// vectors are already stored off-heap in ds.Index.
					_ = ds.GeoIndex.AddBatch(validIds, nil, validPoints, nil)
				}
			}
		}
	}
}

func (s *VectorStore) enqueueBatchIndexJob(ds *Dataset, name string, rec arrow.RecordBatch, batchIdx, currNode int) {
	// Dispatch batch-level indexing job asynchronously to avoid blocking DoPut
	rec.Retain() // IndexJob holds ref
	job := IndexJob{
		DatasetName: name,
		Record:      rec,
		BatchIdx:    batchIdx,
		CreatedAt:   time.Now(),
		NodeID:      currNode,
	}

	ds.PendingIndexJobs.Add(rec.NumRows())
	if !s.EnqueueIndexJob(job) {
		metrics.IndexJobsOverflowTotal.Inc()
		s.pendingOverflowJobs.Add(1)
		go func() {
			defer s.pendingOverflowJobs.Add(-1)
			if !s.BlockEnqueueIndexJob(job, 5*time.Second) {
				ds.PendingIndexJobs.Add(-rec.NumRows())
				rec.Release()
				s.logger.Warn().Str("dataset", name).Msg("Index job dropped after blocking timeout")
			}
		}()
	}
}

func computeVectorNormAt(vecCol arrow.Array, listLen, row int) (float32, bool) {
	var vStart, vEnd int
	var values arrow.Array
	switch col := vecCol.(type) {
	case *array.FixedSizeList:
		vStart = row * listLen
		vEnd = (row + 1) * listLen
		values = col.ListValues()
	case *array.List:
		offsets := col.Offsets()
		vStart = int(offsets[row])
		vEnd = int(offsets[row+1])
		values = col.ListValues()
	}
	if values == nil || vStart < 0 || vEnd <= vStart {
		return 0, false
	}
	switch valArr := values.(type) {
	case *array.Float32:
		if vEnd <= valArr.Len() {
			src := valArr.Float32Values()[vStart:vEnd]
			var sum float64
			for _, v := range src {
				fv := float64(v)
				sum += fv * fv
			}
			return float32(math.Sqrt(sum)), true
		}
	case *array.Float64:
		if vEnd <= valArr.Len() {
			f64Values := valArr.Float64Values()[vStart:vEnd]
			var sum float64
			for _, v := range f64Values {
				sum += v * v
			}
			return float32(math.Sqrt(sum)), true
		}
	}
	return 0, false
}
