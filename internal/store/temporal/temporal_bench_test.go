package temporal

import (
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/memory"
)

// BenchmarkTemporalTree_GetRange runs the benchmark temporal tree_get range operation.
func BenchmarkTemporalTree_GetRange(b *testing.B) {
	arena := memory.NewSlabArena(1024 * 1024)
	tt := NewTemporalTree(arena)
	n := 100000
	startTs := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		tt.Insert(startTs+int64(i), uint64(i), 0.0)
	}

	b.ResetTimer()
	for b.Loop() {
		_ = tt.GetRange(startTs+10000, startTs+20000)
	}
}

// BenchmarkTemporalTree_GetLatest runs the benchmark temporal tree_get latest operation.
func BenchmarkTemporalTree_GetLatest(b *testing.B) {
	arena := memory.NewSlabArena(1024 * 1024)
	tt := NewTemporalTree(arena)
	n := 100000
	startTs := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		tt.Insert(startTs+int64(i), uint64(i), 0.0)
	}

	b.ResetTimer()
	for b.Loop() {
		_ = tt.GetLatest(10000)
	}
}
