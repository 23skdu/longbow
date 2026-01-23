package pool

import (
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
)

// BitmapPool manages a pool of *roaring.Bitmap objects to reduce GC pressure.
type BitmapPool struct {
	pool sync.Pool
}

var globalBitmapPool = &BitmapPool{
	pool: sync.Pool{
		New: func() any {
			return roaring.NewBitmap()
		},
	},
}

// GetBitmap retrieves a cleared bitmap from the global pool.
func GetBitmap() *roaring.Bitmap {
	return globalBitmapPool.Get()
}

// PutBitmap returns a bitmap to the global pool after clearing it.
func PutBitmap(bm *roaring.Bitmap) {
	globalBitmapPool.Put(bm)
}

// Get retrieves a cleared bitmap from the pool.
func (p *BitmapPool) Get() *roaring.Bitmap {
	return p.pool.Get().(*roaring.Bitmap)
}

// Put returns a bitmap to the pool.
func (p *BitmapPool) Put(bm *roaring.Bitmap) {
	if bm != nil {
		bm.Clear()
		p.pool.Put(bm)
	}
}
