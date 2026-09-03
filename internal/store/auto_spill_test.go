package store

import (
	"os"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestProjectVectorMemory_Calculations(t *testing.T) {
	// 1. Zero vectors or zero dims should return 0
	assert.Equal(t, int64(0), ProjectVectorMemory(0, 128, types.VectorTypeFloat32))
	assert.Equal(t, int64(0), ProjectVectorMemory(100, 0, types.VectorTypeFloat32))

	// 2. Float32 (4 bytes/dim) for 10k vectors, 128d
	f32Mem := ProjectVectorMemory(10000, 128, types.VectorTypeFloat32)
	assert.Greater(t, f32Mem, int64(10000*128*4))

	// 3. Complex128 (16 bytes/dim) for 500k vectors, 384d
	c128Mem := ProjectVectorMemory(500000, 384, types.VectorTypeComplex128)
	// Raw: 500k * 384 * 16 = 3.072 GB. Projected graph + node footprint is > 6 GB
	assert.Greater(t, c128Mem, int64(6)*1024*1024*1024)

	// 4. TurboQuant (0.5 byte/dim) for 500k vectors, 128d
	tqMem := ProjectVectorMemory(500000, 128, types.VectorTypeTQ)
	// TurboQuant should be drastically smaller than Float32
	f32HighMem := ProjectVectorMemory(500000, 128, types.VectorTypeFloat32)
	assert.Less(t, tqMem, f32HighMem/2, "TurboQuant memory should be less than half of Float32")
}

func TestShouldSpillToDisk_Thresholds(t *testing.T) {
	simulatedRAM := int64(24) * 1024 * 1024 * 1024 // 24 GB RAM

	// 10,000 vectors, 128d Float32 on 24 GB RAM -> Should NOT spill (<70%)
	assert.False(t, ShouldSpillToDisk(10000, 128, types.VectorTypeFloat32, simulatedRAM, 0.70))

	// 500,000 vectors, 384d Complex128 (16 bytes/dim) on 12 GB RAM -> MUST spill (>70%)
	// Raw: 500k * 384 * 16 = 3.072 GB. Projected = 10.598 GB >= 12 GB * 0.70 (8.4 GB)
	assert.True(t, ShouldSpillToDisk(500000, 384, types.VectorTypeComplex128, 12*1024*1024*1024, 0.70))

	// 500,000 vectors, 384d Float64 (8 bytes/dim) on 6 GB container -> MUST spill (>70%)
	// Raw: 500k * 384 * 8 = 1.536 GB. Projected = 4.812 GB >= 6 GB * 0.70 (4.2 GB)
	assert.True(t, ShouldSpillToDisk(500000, 384, types.VectorTypeFloat64, 6*1024*1024*1024, 0.70))

	// 500,000 vectors, 128d TurboQuant (4-bit) on 24 GB RAM -> Should NOT spill
	assert.False(t, ShouldSpillToDisk(500000, 128, types.VectorTypeTQ, simulatedRAM, 0.70))
}

func TestShouldSpillToDisk_EnvOverrides(t *testing.T) {
	defer os.Unsetenv("LONGBOW_AUTO_SPILL_DISK")
	defer os.Unsetenv("LONGBOW_SPILL_THRESHOLD_RATIO")

	os.Setenv("LONGBOW_AUTO_SPILL_DISK", "0")
	// If auto-spill is explicitly disabled in env, it should not trigger in ShouldSpillToDisk
	assert.False(t, (&VectorStore{}).ShouldSpillToDisk(nil, nil))
}

func TestAutoSpillToDisk_InitDiskStoreIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "autospill_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	alloc := memory.NewGoAllocator()
	logger := zerolog.Nop()
	// Set max memory small (50 MB) so uncompressed 64-bit projection exceeds 70% threshold
	vs := NewVectorStore(alloc, logger, 50*1024*1024, 1024*1024, time.Hour)
	vs.dataPath = tempDir

	// Build Arrow schema for Float64 vectors (dim = 384)
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(384, arrow.PrimitiveTypes.Float64)},
	}
	schema := arrow.NewSchema(fields, nil)

	ds := &Dataset{
		Name:                "spill_test_ds",
		PreferredVectorType: types.VectorTypeFloat64,
	}

	// ShouldSpillToDisk must return true because projected memory > 35 MB (70% of 50 MB)
	assert.True(t, vs.ShouldSpillToDisk(ds, schema))

	// initDiskStore should initialize ds.DiskStore
	vs.initDiskStore(ds, ds.Name, schema)
	assert.NotNil(t, ds.DiskStore, "DiskStore should have been initialized via auto-spill")
	if ds.DiskStore != nil {
		assert.Equal(t, 384, ds.DiskStore.dim)
	}
}

func FuzzMemoryProjectionThresholds(f *testing.F) {
	f.Add(int64(1000), 128, int(types.VectorTypeFloat32), int64(16*1024*1024*1024))
	f.Add(int64(500000), 384, int(types.VectorTypeComplex128), int64(24*1024*1024*1024))
	f.Add(int64(500000), 128, int(types.VectorTypeTQ), int64(8*1024*1024*1024))

	f.Fuzz(func(t *testing.T, numVectors int64, dim int, dtVal int, maxRAM int64) {
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

		proj := ProjectVectorMemory(numVectors, dim, dt)
		assert.GreaterOrEqual(t, proj, int64(0))

		// Spill evaluation must not panic
		spill := ShouldSpillToDisk(numVectors, dim, dt, maxRAM, 0.70)
		_ = spill
	})
}
