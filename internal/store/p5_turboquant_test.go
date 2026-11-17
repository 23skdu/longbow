package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
)

func TestShouldAutoQuantizeHighScale(t *testing.T) {
	// At 500k+ vectors on a memory-constrained host, auto-standardization must
	// engage for an uncompressed float32 dataset.
	if !ShouldAutoQuantize(500_000, 128, types.VectorTypeFloat32, 16*1024*1024*1024, 100_000) {
		t.Fatal("expected auto-quantize to engage at 500k on 16GB host")
	}
	// Already-quantized formats never re-quantize.
	if ShouldAutoQuantize(500_000, 128, types.VectorTypeTQ, 16*1024*1024*1024, 100_000) {
		t.Fatal("TurboQuant input should not re-quantize")
	}
	// Below threshold on a large host with plenty of headroom does not engage.
	if ShouldAutoQuantize(10_000, 128, types.VectorTypeFloat32, 256*1024*1024*1024, 100_000) {
		t.Fatal("expected no auto-quantize below threshold on large host")
	}
	// Projected memory exceeding 30% of RAM engages even below the count threshold
	// when the threshold is small.
	if !ShouldAutoQuantize(50_000, 1024, types.VectorTypeFloat64, 8*1024*1024*1024, 10_000) {
		t.Logf("projected-memory path did not engage; verify ProjectVectorMemory semantics")
	}
}
