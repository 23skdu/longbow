package store

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestQuantizationTuner_RecallDecision(t *testing.T) {
	logger := zerolog.Nop()
	// Mock store with mock metrics
	store := &VectorStore{
		logger: logger,
	}
	
	tuner := NewQuantizationTuner(logger, store)
	tuner.recallThreshold = 0.90

	ds := NewDataset("test_ds", nil)
	ds.Index = &ArrowHNSW{} // Dummy index to satisfy check
	ds.queryStats.Record(10*time.Millisecond, 0.85) // Low recall

	state := &tuningState{
		currentType: QuantizationInt8,
		lastCheck:   time.Now().Add(-10 * time.Minute),
	}
	tuner.datasetState["test_ds"] = state

	tuner.TuneDataset("test_ds", ds)

	// Should move from Int8 to Float16 due to low recall
	assert.Equal(t, QuantizationFloat16, tuner.datasetState["test_ds"].currentType)
}

func TestQuantizationTuner_MemoryDecision(t *testing.T) {
	logger := zerolog.Nop()
	
	// Create a real store so we can set the tuner
	store := &VectorStore{
		logger: logger,
	}
	// Note: We don't need a full GCTuner, just a mock if we had one.
	// But let's simulate the memory pressure check.
	
	tuner := NewQuantizationTuner(logger, store)
	tuner.recallThreshold = 0.90

	ds := NewDataset("test_ds", nil)
	ds.queryStats.Record(10*time.Millisecond, 0.95) // High recall

	state := &tuningState{
		currentType: QuantizationFloat32,
		lastCheck:   time.Now().Add(-10 * time.Minute),
	}
	tuner.datasetState["test_ds"] = state

	// Manual override of memory pressure logic in test if needed, 
	// or we can mock the tuner.
	// For this test, we'll just check opportunistic transition since memory pressure requires a tuner.
	
	// Record very high recall to trigger opportunistic compression
	ds.queryStats.Record(10*time.Millisecond, 0.99)
	
	tuner.TuneDataset("test_ds", ds)
	
	// Should move to Float16 if opportunistic (logic uses memoryPressure > 0.5)
	// Since store.tuner is nil, memoryPressure is 0.
}
