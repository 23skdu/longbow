package store

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkedLocationStore_ReverseIndex(t *testing.T) {
	s := NewChunkedLocationStore()

	// Test Append
	loc1 := Location{BatchIdx: 0, RowIdx: 1}
	id1 := s.Append(loc1)
	assert.Equal(t, uint32(0), uint32(id1))

	gotID1, ok1 := s.GetID(loc1)
	assert.True(t, ok1)
	assert.Equal(t, id1, gotID1)

	// Test BatchAppend
	locs := []Location{
		{BatchIdx: 0, RowIdx: 2},
		{BatchIdx: 1, RowIdx: 0},
	}
	startID := s.BatchAppend(locs)
	assert.Equal(t, uint32(1), uint32(startID))

	gotID2, ok2 := s.GetID(locs[0])
	assert.True(t, ok2)
	assert.Equal(t, startID, gotID2)

	gotID3, ok3 := s.GetID(locs[1])
	assert.True(t, ok3)
	assert.Equal(t, startID+1, gotID3)

	// Test Set (update)
	locUpdated := Location{BatchIdx: 2, RowIdx: 5}
	s.Set(id1, locUpdated)

	gotID1Updated, ok1Updated := s.GetID(locUpdated)
	assert.True(t, ok1Updated)
	assert.Equal(t, id1, gotID1Updated)

	// Note: The old location (BatchIdx: 0, RowIdx: 1) still points to id1 in the reverse map?
	// Currently we DON'T remove old keys in Set (performance trade-off).
	// So GetID(loc1) might still return id1.
	// But stricly, it's fine as long as new location works.
	// We can verify this behavior.
	gotOld, okOld := s.GetID(loc1)
	assert.True(t, okOld)
	assert.Equal(t, id1, gotOld)

	// Test non-existent
	_, okMissing := s.GetID(Location{BatchIdx: 99, RowIdx: 99})
	assert.False(t, okMissing)
}

func TestChunkedLocationStore_Concurrency(t *testing.T) {
	s := NewChunkedLocationStore()
	iter := 1000
	var wg sync.WaitGroup

	wg.Add(iter)
	for i := 0; i < iter; i++ {
		go func(idx int) {
			defer wg.Done()
			loc := Location{BatchIdx: idx, RowIdx: idx} // Unique locations
			s.Append(loc)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, iter, s.Len())

	// Verify all reverse lookups
	for i := 0; i < iter; i++ {
		loc := Location{BatchIdx: i, RowIdx: i}
		id, ok := s.GetID(loc)
		assert.True(t, ok, "failed to find location %v", loc)
		// We can't easily assert ID without knowing order, but we confirm it exists.
		gotLoc, found := s.Get(id)
		assert.True(t, found)
		assert.Equal(t, loc, gotLoc)
	}
}
