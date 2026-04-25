package store

import (
	"context"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/rs/zerolog"
)

// QuantizationType represents the supported quantization levels.
type QuantizationType string

const (
	QuantizationFloat32 QuantizationType = "float32"
	QuantizationFloat16 QuantizationType = "float16"
	QuantizationInt8    QuantizationType = "int8"
)

// QuantizationTuner handles automatic selection of quantization levels.
type QuantizationTuner struct {
	logger zerolog.Logger
	store  *VectorStore

	mu              sync.RWMutex
	recallThreshold float64
	checkInterval   time.Duration
	
	// datasetState tracks the current tuning state for each dataset.
	datasetState map[string]*tuningState
}

type tuningState struct {
	currentType QuantizationType
	lastCheck   time.Time
}

// NewQuantizationTuner creates a new tuner instance.
func NewQuantizationTuner(logger zerolog.Logger, store *VectorStore) *QuantizationTuner {
	return &QuantizationTuner{
		logger:          logger.With().Str("component", "quantization-tuner").Logger(),
		store:           store,
		recallThreshold: 0.90, // Default: maintain at least 90% recall
		checkInterval:   5 * time.Minute,
		datasetState:    make(map[string]*tuningState),
	}
}

// Start runs the periodic tuning loop.
func (t *QuantizationTuner) Start(ctx context.Context) {
	ticker := time.NewTicker(t.checkInterval)
	defer ticker.Stop()

	t.logger.Info().Msg("Quantization auto-tuner started")

	for {
		select {
		case <-ctx.Done():
			t.logger.Info().Msg("Quantization auto-tuner stopped")
			return
		case <-ticker.C:
			t.TuneAll()
		}
	}
}

// TuneAll iterates through all datasets and performs tuning.
func (t *QuantizationTuner) TuneAll() {
	t.store.IterateDatasets(func(name string, ds *Dataset) {
		t.TuneDataset(name, ds)
	})
}

// TuneDataset evaluates and potentially adjusts quantization for a single dataset.
func (t *QuantizationTuner) TuneDataset(name string, ds *Dataset) {
	t.mu.Lock()
	state, exists := t.datasetState[name]
	if !exists {
		state = &tuningState{
			currentType: QuantizationFloat32,
			lastCheck:   time.Now(),
		}
		t.datasetState[name] = state
		// Initialize metrics
		metrics.QuantizationActiveType.WithLabelValues(name, string(QuantizationFloat32)).Set(1)
	}
	t.mu.Unlock()

	// 1. Gather Recall Metrics
	_, _, _, recall, _ := ds.queryStats.GetMetrics()
	if recall == 0 {
		return // Not enough search activity to tune
	}

	metrics.QuantizationRecallEstimate.WithLabelValues(name, string(state.currentType)).Observe(recall)

	// 2. Memory Pressure Check
	var memoryPressure float64
	if t.store.tuner != nil {
		memoryPressure = t.store.tuner.GetUtilizationRatio()
	}

	// 3. Decision Logic
	newType := state.currentType

	// If recall is too low, move to higher precision
	if recall < t.recallThreshold {
		switch state.currentType {
		case QuantizationInt8:
			newType = QuantizationFloat16
		case QuantizationFloat16:
			newType = QuantizationFloat32
		}
		t.applyTransition(name, ds, state, newType, "recall")
		return
	}

	// If memory pressure is high (> 85%), move to higher compression
	if memoryPressure > 0.85 {
		switch state.currentType {
		case QuantizationFloat32:
			newType = QuantizationFloat16
		case QuantizationFloat16:
			newType = QuantizationInt8
		}
		t.applyTransition(name, ds, state, newType, "memory")
		return
	}

	// Opportunistic compression: If recall is very high (> 98%) and we aren't at max compression
	if recall > 0.98 && memoryPressure > 0.5 {
		switch state.currentType {
		case QuantizationFloat32:
			newType = QuantizationFloat16
		case QuantizationFloat16:
			newType = QuantizationInt8
		}
		t.applyTransition(name, ds, state, newType, "opportunistic")
	}
}

func (t *QuantizationTuner) applyTransition(name string, ds *Dataset, state *tuningState, newType QuantizationType, reason string) {
	if newType == state.currentType {
		return
	}

	t.logger.Info().
		Str("dataset", name).
		Str("from", string(state.currentType)).
		Str("to", string(newType)).
		Str("reason", reason).
		Msg("Quantization transition triggered")

	// 1. Update Index Configuration
	ds.dataMu.Lock()
	idx := ds.Index
	if idx == nil {
		ds.dataMu.Unlock()
		return
	}
	
	// This requires the index to support live re-quantization or we'll trigger it on next maintenance
	// For now, we update the preferred type which signals the background workers.
	var dataType types.VectorDataType
	switch newType {
	case QuantizationFloat32:
		dataType = types.VectorTypeFloat32
	case QuantizationFloat16:
		dataType = types.VectorTypeFloat16
	case QuantizationInt8:
		dataType = types.VectorTypeInt8
	}
	ds.PreferredVectorType = dataType
	ds.dataMu.Unlock()

	// 2. Update Metrics
	metrics.QuantizationSwitchesTotal.WithLabelValues(name, string(state.currentType), string(newType), reason).Inc()
	metrics.QuantizationActiveType.WithLabelValues(name, string(state.currentType)).Set(0)
	metrics.QuantizationActiveType.WithLabelValues(name, string(newType)).Set(1)

	// 3. Update State
	state.currentType = newType
	state.lastCheck = time.Now()

	// 4. Calculate Savings (Estimated)
	t.updateSavingsMetric(name, ds, newType)
}

func (t *QuantizationTuner) updateSavingsMetric(name string, ds *Dataset, qType QuantizationType) {
	numVectors := int64(ds.IndexLen())
	// Use ds.Index.GetDims() if available or fall back to dataset PreferredVectorType logic
	dims := int64(0)
	if ds.Index != nil {
		dims = int64(ds.Index.GetDimension())
	}
	if dims == 0 {
		return
	}

	var bytesPerDim float64
	switch qType {
	case QuantizationFloat32:
		bytesPerDim = 4
	case QuantizationFloat16:
		bytesPerDim = 2
	case QuantizationInt8:
		bytesPerDim = 1
	}

	savings := float64(numVectors*dims) * (4.0 - bytesPerDim)
	metrics.QuantizationMemorySavingsBytes.WithLabelValues(name).Set(savings)
}
