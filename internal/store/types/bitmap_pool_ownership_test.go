package types

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/23skdu/longbow/internal/pool"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBitset_PoolNeverReturnsStaleContents pins the property the pool exists to
// provide: a bitmap handed back by Release comes back empty, so a new owner
// cannot observe the previous owner's contents.
func TestBitset_PoolNeverReturnsStaleContents(t *testing.T) {
	for i := 0; i < 2000; i++ {
		b := NewBitset()
		require.True(t, b.AsRoaring().IsEmpty(), "a bitset acquired from the pool starts empty")
		b.Set(i % 4096)
		b.Set(i%4096 + 1)
		require.Equal(t, uint64(2), b.Count())
		b.Release()

		next := NewBitset()
		require.True(t, next.AsRoaring().IsEmpty(),
			"a bitset acquired after a release must not observe the previous owner's contents")
		require.Equal(t, uint64(0), next.Count())
		require.False(t, next.Contains(i%4096))
		next.Release()
	}
}

// TestBitset_ReleasedSharedBitmapIsNotRecycled covers the aliasing case that
// used to put one bitmap into the pool twice: a bitset built from a caller's
// roaring.Bitmap must not hand that bitmap back, because the caller still owns
// it and may release its own owner afterwards.
func TestBitset_ReleasedSharedBitmapIsNotRecycled(t *testing.T) {
	bm := roaring.New()
	bm.Add(7)
	bm.Add(9)

	b := NewBitsetFromRoaring(bm)
	require.True(t, b.Contains(7))
	b.Release()

	assert.Equal(t, uint64(2), bm.GetCardinality(), "releasing a shared bitset must not clear the caller's bitmap")

	for i := 0; i < 32; i++ {
		got := pool.GetBitmap()
		assert.NotSame(t, bm, got, "a caller-owned bitmap must never reach the pool")
		pool.PutBitmap(got)
	}
}

// TestBitset_SharedReleaseDoesNotDuplicatePoolEntry reproduces the sequence of
// TestBitset_Roaring, where two bitsets end up pointing at the same pooled
// bitmap. Before the fix, releasing both put that one bitmap into the pool
// twice, and the pool then handed the same object to two owners: TestBitset_Slice
// saw the source bitset's ids appear inside its own slice.
func TestBitset_SharedReleaseDoesNotDuplicatePoolEntry(t *testing.T) {
	for i := 0; i < 200; i++ {
		b := NewBitset()
		b.Set(1)
		b.Set(5)
		b.Set(10)
		b.Set(15)

		alias := NewBitsetFromRoaring(b.AsRoaring())
		b.Release()
		alias.Release()

		first := NewBitset()
		second := NewBitset()
		require.NotSame(t, first.AsRoaring(), second.AsRoaring(),
			"the pool must not hand one bitmap to two owners")
		require.True(t, first.AsRoaring().IsEmpty())
		require.True(t, second.AsRoaring().IsEmpty())
		first.Release()
		second.Release()
	}
}

// TestBitset_SliceIsNotAliasedByThePooledSource is the regression for the
// reported failure: a slice built while the source is alive must not share the
// source's bitmap, so writing the slice cannot write into its source.
func TestBitset_SliceIsNotAliasedByThePooledSource(t *testing.T) {
	for i := 0; i < 500; i++ {
		b := NewBitset()
		b.Set(1)
		b.Set(5)
		b.Set(10)
		b.Set(15)

		slice := b.Slice(1, 10)
		require.NotSame(t, b.AsRoaring(), slice.AsRoaring(),
			"Slice must return a bitmap of its own, not the source's pooled bitmap")

		arr := slice.ToUint32Array()
		require.Equal(t, []uint32{0, 4, 9}, arr)

		assert.Equal(t, uint64(4), b.Count(), "the source must be unchanged by its slice")
		slice.Release()
		b.Release()
	}
}

// TestBitset_ConcurrentAcquireRelease drives the pool from many goroutines at
// once. Under -race this is what catches a bitmap handed to two owners: the
// second writer's Add is a data race, and the re-read sees a foreign marker.
func TestBitset_ConcurrentAcquireRelease(t *testing.T) {
	const (
		workers = 16
		rounds  = 400
	)

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				b := NewBitset()
				if got := b.Count(); got != 0 {
					errs <- staleError(worker, r, got)
					b.Release()
					return
				}
				marker := uint32(worker*rounds + r)
				b.Set(int(marker % 1_000_000))
				if got := b.Count(); got != 1 {
					errs <- aliasedError(worker, r, got)
					b.Release()
					return
				}
				runtime.Gosched()
				if !b.Contains(int(marker%1_000_000)) || b.Count() != 1 {
					errs <- aliasedError(worker, r, b.Count())
					b.Release()
					return
				}
				b.Release()
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestBitset_ConcurrentReleaseStress releases bitsets built from a caller's
// bitmap while other goroutines churn the pool, so a released bitset can never
// be observed with stale contents under -race.
func TestBitset_ConcurrentReleaseStress(t *testing.T) {
	const (
		workers = 16
		rounds  = 400
	)

	shared := roaring.New()
	for i := uint32(0); i < 64; i++ {
		shared.Add(i * 64)
	}

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				owned := NewBitset()
				owned.Set(r % 512)

				borrowed := NewBitsetFromRoaring(shared)
				borrowed.Set(int(worker%64) * 64)

				owned.Release()
				borrowed.Release()

				if shared.GetCardinality() != 64 {
					errs <- aliasedError(worker, r, shared.GetCardinality())
					return
				}
				fresh := NewBitset()
				if fresh.Count() != 0 {
					errs <- staleError(worker, r, fresh.Count())
					fresh.Release()
					return
				}
				fresh.Release()
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func staleError(worker, round int, got uint64) error {
	return fmt.Errorf("acquired a bitset that already holds contents: worker %d round %d saw %d set bits", worker, round, got)
}

func aliasedError(worker, round int, got uint64) error {
	return fmt.Errorf("bitset contents changed under its owner, two owners share one bitmap: worker %d round %d saw %d set bits", worker, round, got)
}
