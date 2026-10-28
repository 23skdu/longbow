package store

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
)

// loadDatasets returns a copy of the active dataset map.
func (vs *VectorStore) loadDatasets() map[string]*Dataset {
	return *vs.datasets.Load()
}

// getDataset retrieves a dataset by name.
func (vs *VectorStore) getDataset(name string) (*Dataset, bool) {
	m := vs.loadDatasets()
	ds, ok := m[name]
	return ds, ok
}

// UpdateDatasets executes a CAS loop to update the dataset map safely.
// fn receives a copy of the current map to modify.
func (vs *VectorStore) UpdateDatasets(fn func(map[string]*Dataset)) {
	for {
		oldPtr := vs.datasets.Load()
		oldMap := *oldPtr

		newMap := make(map[string]*Dataset, len(oldMap)+1)
		for k, v := range oldMap {
			newMap[k] = v
		}

		fn(newMap)

		if vs.datasets.CompareAndSwap(oldPtr, &newMap) {
			return
		}
		metrics.DatasetUpdateRetriesTotal.Inc()
		runtime.Gosched()
	}
}

// IterateDatasets safely iterates over all datasets.
func (vs *VectorStore) IterateDatasets(fn func(string, *Dataset)) {
	m := vs.loadDatasets()
	for name, ds := range m {
		fn(name, ds)
	}
}

// getOrCreateDataset atomically gets an existing dataset or creates a new one using the provider.
func (vs *VectorStore) getOrCreateDataset(name string, createFn func() *Dataset) (*Dataset, bool) {
	if ds, ok := vs.getDataset(name); ok && ds != nil {
		return ds, false
	}

	var result *Dataset
	var created bool
	vs.UpdateDatasets(func(m map[string]*Dataset) {
		if ds, ok := m[name]; ok && ds != nil {
			result = ds
			created = false
			return
		}

		newDs := createFn()
		if newDs != nil {
			newDs.EvictionManager = vs.evictionManager
			newDs.SetAdmission(vs.admission)
			if vs.hnsw2Config != nil && newDs.Index == nil {
				newDs.Index = index.NewArrowHNSW(newDs, vs.hnsw2Config, nil)
			}
			if vs.hybridSearchConfig.Enabled {
				newDs.BM25Index = NewBM25InvertedIndex(vs.hybridSearchConfig.BM25)
				slabArena := lbmem.NewSlabArena(4 * 1024 * 1024)
				newDs.BM25ArenaIndex = NewBM25ArenaIndex(slabArena, 10000)
			}
			if vs.temporalConfig.Enabled {
				newDs.TemporalIndex = NewTemporalIndex(0)
			}
			m[name] = newDs
			result = newDs
			created = true
		}
	})

	// Register dataset in namespace
	if result != nil {
		nsName, _ := ParseNamespacedPath(name)
		if ns := vs.GetNamespace(nsName); ns != nil {
			if !ns.HasDataset(name) {
				ns.AddDataset(name)
			}
		}
	}

	return result, created
}

// evictDataset removes a dataset from memory and releases its resources safely.
func (s *VectorStore) evictDataset(name string) {
	var ds *Dataset
	s.UpdateDatasets(func(m map[string]*Dataset) {
		if d, ok := m[name]; ok {
			ds = d
			delete(m, name)
		}
	})

	if ds == nil {
		return
	}

	ds.WaitForIndexing()

	totalMemory := ds.SizeBytes.Load() + ds.IndexMemoryBytes.Load()
	s.currentMemory.Add(-totalMemory)

	ds.Close()

	s.logger.Info().Str("dataset", name).Int64("freed_bytes", totalMemory).Msg("Dataset evicted safely")
}

// PrewarmDataset ensures a dataset is initialized in memory, creating it if necessary.
func (s *VectorStore) PrewarmDataset(name string, schema *arrow.Schema) {
	_, created := s.getOrCreateDataset(name, func() *Dataset {
		ds := NewDataset(name, schema)
		ds.Logger = s.logger
		ds.Topo = s.numaTopology
		s.initDiskStore(ds, name, schema)
		return ds
	})

	if created {
		s.logger.Info().Str("dataset", name).Msg("Pre-warmed dataset")
	}
}

// ShouldSpillToDisk evaluates whether dataset memory projection exceeds the configured physical RAM threshold (default 70%).
func (s *VectorStore) ShouldSpillToDisk(ds *Dataset, schema *arrow.Schema) bool {
	if os.Getenv("LONGBOW_AUTO_SPILL_DISK") == "0" || os.Getenv("LONGBOW_AUTO_SPILL_DISK") == "false" {
		return false
	}

	dim := 0
	vecColName := ""
	if schema != nil {
		for _, f := range schema.Fields() {
			if f.Name == "vector" || f.Name == "embedding" {
				if fst, ok := f.Type.(*arrow.FixedSizeListType); ok {
					dim = int(fst.Len())
					vecColName = f.Name
					break
				}
			}
		}
	}
	if dim == 0 {
		return false
	}

	dt := types.VectorTypeFloat32
	if schema != nil && vecColName != "" {
		dt = index.InferVectorDataType(schema, vecColName)
	}
	if ds != nil && ds.GetPreferredVectorType() != types.VectorTypeUnknown {
		dt = ds.GetPreferredVectorType()
	}

	rowCount := int64(0)
	if ds != nil {
		rowCount = ds.GetRowCount()
	}

	thresholdRatio := 0.70
	if v := os.Getenv("LONGBOW_SPILL_THRESHOLD_RATIO"); v != "" {
		if r, err := strconv.ParseFloat(v, 64); err == nil && r > 0 && r < 1 {
			thresholdRatio = r
		}
	}

	maxRAM := s.maxMemory.Load()
	if maxRAM <= 0 {
		maxRAM = lbmem.GetPhysicalMemory()
	}

	evalCount := rowCount
	if evalCount < 50000 && maxRAM <= 24*1024*1024*1024 {
		if dt == types.VectorTypeFloat64 || dt == types.VectorTypeComplex64 ||
			dt == types.VectorTypeComplex128 || dt == types.VectorTypeInt64 {
			evalCount = 500000
		}
	}

	return ShouldSpillToDisk(evalCount, dim, dt, maxRAM, thresholdRatio)
}

// initDiskStore initializes the DiskVectorStore for a dataset if conditions are met.
func (s *VectorStore) initDiskStore(ds *Dataset, name string, schema *arrow.Schema) {
	if ds == nil || schema == nil || ds.DiskStore != nil {
		return
	}
	ds.initMu.Lock()
	defer ds.initMu.Unlock()
	if ds.DiskStore != nil {
		return
	}

	useDisk := os.Getenv("LONGBOW_USE_DISK") == "1" || strings.HasPrefix(name, "test_disk")
	isAutoSpill := false

	if !useDisk {
		if s.ShouldSpillToDisk(ds, schema) {
			useDisk = true
			isAutoSpill = true
		}
	}

	if !useDisk {
		return
	}

	path := filepath.Join(s.dataPath, name+"_vectors.bin")
	dim := 0
	for _, f := range schema.Fields() {
		if f.Name == "vector" || f.Name == "embedding" {
			if fst, ok := f.Type.(*arrow.FixedSizeListType); ok {
				dim = int(fst.Len())
				break
			}
		}
	}

	if dim == 0 {
		return
	}

	dvs, err := NewDiskVectorStore(path, dim)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to create DiskVectorStore")
		return
	}
	if ds.TurboQuantBits() > 0 {
		dvs.SetTurboQuant(ds.TurboQuantBits())
	}
	ds.DiskStore = dvs

	if isAutoSpill {
		metrics.AutoSpillToDiskEngaged.WithLabelValues(name).Inc()
		s.logger.Info().
			Str("dataset", name).
			Str("path", path).
			Int("dim", dim).
			Msg("Automatic spill-to-disk paging engaged (exceeded 70% physical memory threshold)")
	} else {
		s.logger.Info().Str("path", path).Int("dim", dim).Msg("DiskVectorStore initialized")
	}
}
