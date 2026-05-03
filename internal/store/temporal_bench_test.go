package store

import (
	"testing"
	"time"
)

func BenchmarkTemporalTree_GetRange(b *testing.B) {
	tt := NewTemporalTree()
	n := 100000
	startTs := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		tt.Insert(startTs+int64(i), uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tt.GetRange(startTs+10000, startTs+20000)
	}
}

func BenchmarkTemporalTree_GetLatest(b *testing.B) {
	tt := NewTemporalTree()
	n := 100000
	startTs := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		tt.Insert(startTs+int64(i), uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tt.GetLatest(10000)
	}
}
