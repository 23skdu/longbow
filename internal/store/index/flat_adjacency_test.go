package index

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)


type appendingApplier struct {
	val uint32
}

func (a *appendingApplier) ApplyNeighbors(old []uint32) []uint32 {
	res := make([]uint32, len(old)+1)
	copy(res, old)
	res[len(old)] = a.val
	return res
}

func TestFlatAdjacency_Basic(t *testing.T) {
	slab := memory.NewSlabArena(1024 * 1024)
	defer slab.Free()

	fa := NewFlatAdjacency(slab, 32, 128)
	defer fa.Release()

	// Initial node has no neighbors
	nbs, ok := fa.GetNeighbors(5)
	assert.False(t, ok)
	assert.Nil(t, nbs)

	// Set neighbors
	input := []uint32{10, 20, 30, 40}
	err := fa.SetNeighbors(5, input)
	require.NoError(t, err)

	nbs, ok = fa.GetNeighbors(5)
	assert.True(t, ok)
	assert.Equal(t, input, nbs)

	// Overwrite neighbors
	input2 := []uint32{100, 200}
	err = fa.SetNeighbors(5, input2)
	require.NoError(t, err)

	nbs, ok = fa.GetNeighbors(5)
	assert.True(t, ok)
	assert.Equal(t, input2, nbs)
}

func TestFlatAdjacency_LockFreeCAS(t *testing.T) {
	slab := memory.NewSlabArena(1024 * 1024)
	defer slab.Free()

	fa := NewFlatAdjacency(slab, 32, 128)
	defer fa.Release()

	// Initial packed ref
	packed0, ok := fa.GetPackedNeighbors(1)
	assert.True(t, ok)

	// Successful CAS
	first := []uint32{1, 2, 3}
	success := fa.CASNeighbors(1, packed0, first)
	assert.True(t, success)

	nbs, ok := fa.GetNeighbors(1)
	assert.True(t, ok)
	assert.Equal(t, first, nbs)

	// Stale CAS with packed0 must fail
	second := []uint32{4, 5, 6}
	failed := fa.CASNeighbors(1, packed0, second)
	assert.False(t, failed)

	// CAS with new packed ref succeeds
	packed1, ok := fa.GetPackedNeighbors(1)
	assert.True(t, ok)
	assert.NotEqual(t, packed0, packed1)

	success2 := fa.CASNeighbors(1, packed1, second)
	assert.True(t, success2)

	nbs, ok = fa.GetNeighbors(1)
	assert.True(t, ok)
	assert.Equal(t, second, nbs)
}

func TestFlatAdjacency_UpdateNeighbors(t *testing.T) {
	slab := memory.NewSlabArena(1024 * 1024)
	defer slab.Free()

	fa := NewFlatAdjacency(slab, 64, 128)
	defer fa.Release()

	err := fa.SetNeighbors(10, []uint32{1, 2})
	require.NoError(t, err)

	err = fa.UpdateNeighbors(10, &appendingApplier{val: 3})
	require.NoError(t, err)

	nbs, ok := fa.GetNeighbors(10)
	assert.True(t, ok)
	assert.Equal(t, []uint32{1, 2, 3}, nbs)
}

func TestFlatAdjacency_ConcurrentReadWrite(t *testing.T) {
	slab := memory.NewSlabArena(16 * 1024 * 1024)
	defer slab.Free()

	fa := NewFlatAdjacency(slab, 64, 1024)
	defer fa.Release()

	numNodes := 32
	numWriters := 8
	numReaders := 16
	numIters := 200

	var wg sync.WaitGroup
	var tornReads atomic.Int64

	// Concurrently write monotonic pattern to nodes:
	// a list of K identical values [val, val, ... val]
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for iter := 0; iter < numIters; iter++ {
				for n := 0; n < numNodes; n++ {
					nodeID := uint32(n)
					val := uint32(workerID*1000 + iter + 1)
					listLen := (iter % 16) + 1
					list := make([]uint32, listLen)
					for i := range list {
						list[i] = val
					}
					_ = fa.SetNeighbors(nodeID, list)
				}
			}
		}(w)
	}

	// Concurrently read nodes and verify all elements match (un-torn read)
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iter := 0; iter < numIters*2; iter++ {
				for n := 0; n < numNodes; n++ {
					nodeID := uint32(n)
					nbs, ok := fa.GetNeighbors(nodeID)
					if ok && len(nbs) > 0 {
						expected := nbs[0]
						for _, v := range nbs {
							if v != expected {
								tornReads.Add(1)
							}
						}
					}
				}
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(0), tornReads.Load(), "Zero torn reads under concurrent writes")
}
