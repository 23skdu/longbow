package store

import (
	"os"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestShouldAutoQuantize_Thresholds(t *testing.T) {
	constrainedRAM := int64(16) * 1024 * 1024 * 1024 // 16 GB RAM
	generousRAM := int64(128) * 1024 * 1024 * 1024  // 128 GB RAM

	// 1. Below threshold (100k < 500k default threshold) -> false
	assert.False(t, ShouldAutoQuantize(100000, 128, types.VectorTypeFloat32, constrainedRAM, 500000))

	// 2. High scale (500k) on constrained RAM (16 GB <= 32 GB) -> true
	assert.True(t, ShouldAutoQuantize(500000, 128, types.VectorTypeFloat32, constrainedRAM, 500000))

	// 3. High scale (500k) with high-dimension Complex128 (16 bytes/dim) on 128 GB RAM -> true if projected > 50%
	assert.True(t, ShouldAutoQuantize(1000000, 1536, types.VectorTypeComplex128, generousRAM, 500000))

	// 4. Already quantized formats must NOT re-quantize
	assert.False(t, ShouldAutoQuantize(500000, 128, types.VectorTypeTQ, constrainedRAM, 500000))
	assert.False(t, ShouldAutoQuantize(500000, 128, types.VectorTypePQ, constrainedRAM, 500000))
	assert.False(t, ShouldAutoQuantize(500000, 128, types.VectorTypeBQ, constrainedRAM, 500000))
}

func TestAutoQuantize_EnvFlags(t *testing.T) {
	defer os.Unsetenv("LONGBOW_AUTO_QUANTIZE")
	defer os.Unsetenv("LONGBOW_AUTO_QUANTIZE_THRESHOLD")
	defer os.Unsetenv("LONGBOW_AUTO_QUANTIZE_BITS")

	os.Setenv("LONGBOW_AUTO_QUANTIZE", "1")
	os.Setenv("LONGBOW_AUTO_QUANTIZE_THRESHOLD", "250000")
	os.Setenv("LONGBOW_AUTO_QUANTIZE_BITS", "8")

	cfg := types.DefaultArrowHNSWConfig()
	assert.True(t, cfg.AutoQuantize)
	assert.Equal(t, int64(250000), cfg.AutoQuantizeThreshold)
	assert.Equal(t, 8, cfg.AutoQuantizeBits)
}

func TestAutoQuantize_TuneDatasetIntegration(t *testing.T) {
	defer os.Unsetenv("LONGBOW_AUTO_QUANTIZE")
	os.Setenv("LONGBOW_AUTO_QUANTIZE", "1")

	alloc := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(alloc, logger, 1024*1024*1024, 1024*1024, time.Hour)

	tuner := NewQuantizationTuner(logger, vs)
	ds := &Dataset{
		Name:                "high_scale_ds",
		PreferredVectorType: types.VectorTypeFloat32,
		Logger:              logger,
	}

	state := &tuningState{
		currentType: QuantizationFloat32,
		lastCheck:   time.Now(),
	}
	tuner.datasetState[ds.Name] = state

	// Verify ShouldAutoQuantize triggers on 500k+
	assert.True(t, ShouldAutoQuantize(500000, 128, types.VectorTypeFloat32, 16*1024*1024*1024, 500000))
}

func FuzzAutoQuantizeEvaluation(f *testing.F) {
	f.Add(int64(10000), 128, int(types.VectorTypeFloat32), int64(16*1024*1024*1024), int64(500000))
	f.Add(int64(500000), 384, int(types.VectorTypeFloat64), int64(24*1024*1024*1024), int64(500000))
	f.Add(int64(1000000), 384, int(types.VectorTypeTQ), int64(32*1024*1024*1024), int64(500000))

	f.Fuzz(func(t *testing.T, numVectors int64, dim int, dtVal int, maxRAM int64, threshold int64) {
		if numVectors < 0 || numVectors > 100_000_000 {
			return
		}
		if dim < 0 || dim > 8192 {
			return
		}
		dt := types.VectorDataType(dtVal)
		if dt < 0 || dt > types.VectorTypeTQ {
			return
		}

		result := ShouldAutoQuantize(numVectors, dim, dt, maxRAM, threshold)
		if dt == types.VectorTypeTQ || dt == types.VectorTypePQ || dt == types.VectorTypeBQ {
			assert.False(t, result, "Quantized types should never trigger auto-quantize")
		}
	})
}
