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

// GetBitmap retrieves an empty bitmap from the global pool.
func GetBitmap() *roaring.Bitmap {
	return globalBitmapPool.Get()
}

// PutBitmap returns a bitmap to the global pool after clearing it.
func PutBitmap(bm *roaring.Bitmap) {
	globalBitmapPool.Put(bm)
}

// Get retrieves an empty bitmap from the pool. The clear is not redundant with
// the one in Put: it is what makes "every Get returns an empty bitmap" hold for
// the pool regardless of how an object entered it, so a caller that recycles a
// bitmap it does not exclusively own can never leak its contents to the next
// owner. roaring's Clear only resets the container index, so it costs the same
// whether or not there is anything to drop.
func (p *BitmapPool) Get() *roaring.Bitmap {
	bm := p.pool.Get().(*roaring.Bitmap)
	if !bm.IsEmpty() {
		bm.Clear()
	}
	return bm
}

// Put returns a bitmap to the pool after clearing it.
func (p *BitmapPool) Put(bm *roaring.Bitmap) {
	if bm == nil {
		return
	}
	if !bm.IsEmpty() {
		bm.Clear()
	}
	p.pool.Put(bm)
}
