package engine

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestMemoryScaling validates memory projection scaling, cap enforcement,
// graceful degradation via auto-spill, and RSS bounds (roadmap E1).
func TestMemoryScaling(t *testing.T) {
	t.Run("ProjectionScalingUpTo1M", func(t *testing.T) {
		p10k := ProjectVectorMemory(10_000, 128, types.VectorTypeFloat32)
		p100k := ProjectVectorMemory(100_000, 128, types.VectorTypeFloat32)
		p1m := ProjectVectorMemory(1_000_000, 128, types.VectorTypeFloat32)

		require.Greater(t, p10k, int64(0))
		require.Greater(t, p100k, p10k)
		require.Greater(t, p1m, p100k)

		// Under a tight 256 MB cap, 1M float32 (128d) must trigger disk spill.
		require.True(t, ShouldSpillToDisk(1_000_000, 128, types.VectorTypeFloat32, 256*1024*1024, 0.70))
		// Under a generous 64 GB cap, 10k float32 (128d) stays in memory.
		require.False(t, ShouldSpillToDisk(10_000, 128, types.VectorTypeFloat32, 64*1024*1024*1024, 0.70))
	})

	t.Run("CapEnforcementAndGracefulDegradation", func(t *testing.T) {
		tmpDir := t.TempDir()
		alloc := memory.NewGoAllocator()
		logger := zerolog.Nop()

		// Configure a tight 10 KB cap to verify both strict rejection and graceful spill/degradation.
		vs := NewVectorStore(alloc, logger, 10*1024, 1024, time.Hour)
		vs.dataPath = tmpDir
		defer func() { _ = vs.Close() }()

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.FixedSizeListOf(64, arrow.PrimitiveTypes.Float32)},
		}, nil)
		ds, _ := vs.getOrCreateDataset("mem_scaling_ds", func() *Dataset {
			return NewDataset("mem_scaling_ds", schema)
		})

		vecs := make([][]float32, 16)
		for i := range vecs {
			v := make([]float32, 64)
			v[0] = float32(i)
			vecs[i] = v
		}
		rec := MakeBatchTestRecord(alloc, 64, vecs)
		defer rec.Release()

		// 1. Hard cap enforcement: when currentMemory exceeds maxMemory, writes return ResourceExhausted.
		vs.memoryConfig.RejectWrites = true
		vs.currentMemory.Store(vs.maxMemory.Load() + 1)
		err := vs.applyBatchToMemory(ds, rec, time.Now().UnixNano())
		require.Error(t, err, "expected ResourceExhausted when currentMemory exceeds maxMemory")
		require.Contains(t, err.Error(), "memory limit exceeded")

		// 2. Graceful degradation under high memory pressure (>70%): DiskStore initializes and batch ingests.
		vs.memoryConfig.RejectWrites = false
		vs.maxMemory.Store(64 * 1024)
		vs.currentMemory.Store(48 * 1024) // 75% pressure (>70% auto-spill/quantization threshold)
		vs.initDiskStore(ds, ds.Name, schema)
		require.NotNil(t, ds.DiskStore, "expected DiskStore to initialize for graceful degradation")

		err = vs.applyBatchToMemory(ds, rec, time.Now().UnixNano())
		require.NoError(t, err, "expected graceful ingestion under >70%% pressure when RejectWrites=false")
	})

	t.Run("RSSWithinLimit", func(t *testing.T) {
		rss := readProcessRSSBytes(t)
		const maxRSS = 20 * 1024 * 1024 * 1024
		require.Less(t, rss, uint64(maxRSS), "process RSS must stay below 20 GB")
	})
}

func readProcessRSSBytes(t *testing.T) uint64 {
	t.Helper()
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		t.Skipf("cannot read rss: %v", err)
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		t.Fatal("unexpected statm format")
	}
	rssPages, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatal(err)
	}
	return uint64(rssPages) * 4096 // #nosec G115
}
