package store

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSegmentTree_Basic(t *testing.T) {
	st := NewSegmentTree(0, 100)

	// Insert intervals
	st.Insert(10, 20, 1)
	st.Insert(15, 25, 2)
	st.Insert(5, 12, 3)
	st.Insert(30, 40, 4)

	// Query point 11 -> overlaps [10, 20] and [5, 12], so ids 1, 3
	bm := st.Query(11)
	assert.True(t, bm.Contains(1))
	assert.False(t, bm.Contains(2))
	assert.True(t, bm.Contains(3))
	assert.False(t, bm.Contains(4))

	// Query point 16 -> overlaps [10, 20] and [15, 25], so ids 1, 2
	bm = st.Query(16)
	assert.True(t, bm.Contains(1))
	assert.True(t, bm.Contains(2))
	assert.False(t, bm.Contains(3))
	assert.False(t, bm.Contains(4))

	// Query range [22, 35] -> overlaps [15, 25] and [30, 40], so ids 2, 4
	bm = st.QueryRange(22, 35)
	assert.False(t, bm.Contains(1))
	assert.True(t, bm.Contains(2))
	assert.False(t, bm.Contains(3))
	assert.True(t, bm.Contains(4))
}

func TestSegmentTree_Remove(t *testing.T) {
	st := NewSegmentTree(0, 100)

	st.Insert(10, 20, 1)
	st.Insert(15, 25, 2)

	bm := st.Query(16)
	assert.True(t, bm.Contains(1))
	assert.True(t, bm.Contains(2))

	st.Remove(10, 20, 1)

	bm = st.Query(16)
	assert.False(t, bm.Contains(1))
	assert.True(t, bm.Contains(2))

	// Remove non-existent
	st.Remove(0, 5, 999)
}

func TestSegmentTree_QueryEmpty(t *testing.T) {
	st := NewSegmentTree(0, 100)

	bm := st.Query(50)
	assert.True(t, bm.IsEmpty())

	bm = st.QueryRange(10, 20)
	assert.True(t, bm.IsEmpty())
}

func TestSegmentTree_LargeRange(t *testing.T) {
	st := NewSegmentTree(0, math.MaxInt64)

	now := time.Now().UnixNano()
	day := int64(24 * time.Hour)

	st.Insert(now, now+day, 1)
	st.Insert(now+day/2, now+day*2, 2)

	bm := st.Query(now + day/4)
	assert.True(t, bm.Contains(1))
	assert.False(t, bm.Contains(2))

	bm = st.Query(now + day)
	assert.True(t, bm.Contains(1))
	assert.True(t, bm.Contains(2))
}

func TestSegmentTree_Concurrency(t *testing.T) {
	st := NewSegmentTree(0, 1000)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id uint32) {
			defer wg.Done()
			st.Insert(int64(id), int64(id+10), id)
			bm := st.Query(int64(id + 5))
			assert.True(t, bm.Contains(id))
			st.Remove(int64(id), int64(id+10), id)
		}(uint32(i))
	}

	wg.Wait()
}

func TestSegmentTree_QueryRangeNilNode(t *testing.T) {
	st := NewSegmentTree(0, 100)
	st.Insert(10, 20, 1) // left branch
	
	// Querying range [80, 90] should traverse to the right branch, which is nil.
	bm := st.QueryRange(80, 90)
	assert.True(t, bm.IsEmpty())
}

func TestSegmentTree_QueryRangeEmptyNode(t *testing.T) {
	st := NewSegmentTree(0, 100)
	st.Insert(10, 20, 1)
	st.Remove(10, 20, 1) // node now has empty IDs
	
	bm := st.QueryRange(10, 20)
	assert.True(t, bm.IsEmpty())
}
