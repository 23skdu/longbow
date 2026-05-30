package store

import (
	"github.com/23skdu/longbow/internal/autoscale"
	"github.com/23skdu/longbow/internal/gc"
	"github.com/23skdu/longbow/internal/gpu"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/mesh"

	lbcore "github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/wal"
)

// GetNUMATopology returns the detected NUMA topology of the system.
func (vs *VectorStore) GetNUMATopology() *lbmem.NUMATopology {
	return vs.numaTopology
}

// IsNUMAEnabled returns true if NUMA-aware memory allocation is active.
func (vs *VectorStore) IsNUMAEnabled() bool {
	return vs.numaEnabled
}

// SetGCTuner sets the memory tuner for backpressure.
func (vs *VectorStore) SetGCTuner(tuner *lbmem.GCTuner) {
	vs.tuner.Store(tuner)
	if vs.admission != nil {
		vs.admission.SetTuner(tuner)
	}
	if tuner != nil {
		tuner.RegisterCleanup(func() {
			vs.logger.Warn().Msg("Emergency memory cleanup: clearing query cache and releasing slab pools")
			vs.queryCache.Clear()
			released := lbmem.ReleaseGlobalSlabPoolsUnused()
			if released > 0 {
				vs.logger.Info().Int("released_slabs", released).Msg("Released unused slabs back to the OS during emergency cleanup")
			}
		})
	}
	// Wire to global worker pool for indexing backpressure
	lbcore.GetSharedPool().SetTuner(tuner)
}

// GetGCTuner returns the memory tuner.
func (vs *VectorStore) GetGCTuner() *lbmem.GCTuner {
	return vs.tuner.Load()
}

// GetAdmissionController returns the admission controller for the store.
func (vs *VectorStore) GetAdmissionController() *AdmissionController {
	return vs.admission
}

// SetAutoScaler registers an auto-scaler for load monitoring.
func (vs *VectorStore) SetAutoScaler(scaler *autoscale.AutoScaler) {
	vs.scaler = scaler
	vs.admission.scaler = scaler
}

// SetCoordinator sets the global search coordinator for the vector store.
func (vs *VectorStore) SetCoordinator(c *GlobalSearchCoordinator) {
	vs.coordinator = c
}

// SetMesh sets the mesh gossip instance for the vector store and initializes the WAL replicator.
func (vs *VectorStore) SetMesh(m *mesh.Gossip) {
	vs.Mesh = m
	if m != nil {
		engine := vs.engine.Load()
		if engine != nil {
			replicator := wal.NewFlightWALReplicator(vs.pool, m)
			engine.SetReplicator(replicator)
		}
	}
}

// GetMeshMembers returns the current members from the mesh gossip instance.
func (vs *VectorStore) GetMeshMembers() []mesh.Member {
	if vs.Mesh == nil {
		return nil
	}
	return vs.Mesh.GetMembers()
}

// SetIndexedColumns updates columns that should be indexed for fast equality lookups
func (vs *VectorStore) SetIndexedColumns(cols []string) {
	vs.indexedColumns = cols
}

// EnableAdaptiveGC starts the adaptive GC controller with the given configuration.
// This is optional and disabled by default. Call this after NewVectorStore if you want
// dynamic GOGC adjustment based on allocation rate and memory pressure.
func (vs *VectorStore) EnableAdaptiveGC(config gc.AdaptiveGCConfig) {
	if vs.gcController != nil {
		vs.gcController.Stop() // Stop existing controller if any
	}

	vs.gcController = gc.NewAdaptiveGCController(config)
	vs.gcController.Start()

	vs.logger.Info().
		Int("min_gogc", config.MinGOGC).
		Int("max_gogc", config.MaxGOGC).
		Dur("adjust_interval", config.AdjustInterval).
		Msg("Adaptive GC controller enabled")
}

// DisableAdaptiveGC stops the adaptive GC controller
func (vs *VectorStore) DisableAdaptiveGC() {
	if vs.gcController != nil {
		vs.gcController.Stop()
		vs.logger.Info().Msg("Adaptive GC controller disabled")
	}
}

// GetIndexedColumns returns columns currently being indexed
func (vs *VectorStore) GetIndexedColumns() []string {
	return vs.indexedColumns
}

// SetAutoShardingConfig updates the auto-sharding configuration
func (vs *VectorStore) SetAutoShardingConfig(cfg AutoShardingConfig) {
	vs.autoShardingConfig = cfg
}

// GetAutoShardingConfig returns the current auto-sharding configuration
func (vs *VectorStore) GetAutoShardingConfig() AutoShardingConfig {
	return vs.autoShardingConfig
}

// SetGPUConfig manually configures the GPU backend and device
func (vs *VectorStore) SetGPUConfig(backend gpu.GPUBackend, deviceID int32) {
	vs.configMu.Lock()
	vs.gpuBackend = backend
	vs.gpuEnabled = true
	vs.gpuDeviceID = deviceID
	vs.configMu.Unlock()

	if backend != gpu.BackendCPU {
		pool, err := gpu.NewGPUMemPool(backend, deviceID)
		if err == nil {
			vs.gpuMemPool = pool
		}

		// Initialize GPU index pool
		vs.gpuIndexPool = gpu.NewGPUIndexPool(gpu.DefaultGPUIndexPoolConfig())

		// Update all dataset-local temporal indices with GPU acceleration
		vs.IterateDatasets(func(name string, ds *Dataset) {
			if ds.TemporalIndex != nil {
				cfg := gpu.GPUConfig{
					DeviceID:  deviceID,
					Dimension: ds.TemporalIndex.dimension,
					Enabled:   true,
					Backend:   backend,
				}
				gIdx, err := gpu.NewIndexWithBackend(cfg, backend)
				if err == nil {
					ds.TemporalIndex.SetGPUIndex(gIdx)
				}
			}
		})

		// Also update any existing GeoIndexes
		vs.IterateDatasets(func(name string, ds *Dataset) {
			if ds.GeoIndex != nil {
				if gIdx, err := vs.getGPUIndex(128); err == nil {
					ds.GeoIndex.SetGPUIndex(gIdx)
				}
			}
		})
	}
}

// SetAutoGPUConfig automatically detects and configures the best available GPU backend
// Metal on macOS, CUDA on Linux with NVIDIA, CPU fallback if no GPU
func (vs *VectorStore) SetAutoGPUConfig(deviceID int32) {
	backend := gpu.GetPreferredBackend()
	gpus := gpu.DetectAvailableGPUs()
	availableGPUs := len(gpus)

	vs.logger.Info().
		Str("backend", backend.String()).
		Int("available_gpus", availableGPUs).
		Bool("gpu_binary_available", availableGPUs > 0).
		Msg("Auto-detected GPU backend")

	if availableGPUs == 0 {
		vs.logger.Warn().
			Str("backend", backend.String()).
			Msg("GPU binary not available - operations will fall back to CPU. Build with appropriate tags for GPU acceleration (e.g., -tags metal,cuda)")
	}

	// Log GPU details
	for i, gpuInfo := range gpus {
		vs.logger.Debug().
			Int("index", i).
			Str("name", gpuInfo.Name).
			Str("backend", gpuInfo.Backend.String()).
			Int64("memory_mb", gpuInfo.MemoryMB).
			Msg("Available GPU detected")
	}

	vs.SetGPUConfig(backend, deviceID)
}

// SetTemporalIndex configures the temporal index for Part 22
func (vs *VectorStore) SetTemporalIndex(cfg TemporalConfig) {
	vs.temporalConfig = cfg

	// Apply to existing datasets
	vs.IterateDatasets(func(name string, ds *Dataset) {
		if ds.TemporalIndex == nil && cfg.Enabled {
			ds.TemporalIndex = NewTemporalIndex(0)
		}
	})
}

// GetTemporalIndex is deprecated
func (vs *VectorStore) GetTemporalIndex() *TemporalIndex {
	return nil
}
