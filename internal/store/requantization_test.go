package store

import (
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
)

func TestAdaptiveRequantization_Trigger(t *testing.T) {
	// 1. Setup Dataset
	ds := NewDataset("test_requant", nil)
	// We need an index for the task to proceed
	cfg := DefaultArrowHNSWConfig()
	ds.Index = NewArrowHNSW(ds, &cfg, nil)
	
	// 2. Trigger Re-quantization to PQ
	ds.TriggerRequantization(types.VectorTypePQ)
	
	// 3. Wait for task to start
	time.Sleep(100 * time.Millisecond)
	
	// 4. Verify PreferredVectorType is updated
	ds.dataMu.RLock()
	assert.Equal(t, types.VectorTypePQ, ds.PreferredVectorType)
	ds.dataMu.RUnlock()
	
	// Verify it's not requantizing anymore (or still in progress)
	// For this small test it should be done
	assert.False(t, ds.isRequantizing.Load())
}
