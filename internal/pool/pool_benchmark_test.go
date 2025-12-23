package pool

import (
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

func BenchmarkBitmapAllocation(b *testing.B) {
	b.Run("RoaringNew", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm := roaring.NewBitmap()
			bm.Add(1)
			_ = bm
		}
	})

	b.Run("PoolGet", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm := GetBitmap()
			bm.Add(1)
			PutBitmap(bm)
		}
	})
}
