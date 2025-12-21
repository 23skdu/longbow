package store

import (
	"sync"
)

// TimestampMap tracks the latest LWW timestamp for each VectorID.
type TimestampMap struct {
	shards [16]*timestampShard
}

type timestampShard struct {
	mu   sync.RWMutex
	data map[VectorID]int64
}

func NewTimestampMap() *TimestampMap {
	tm := &TimestampMap{}
	for i := 0; i < 16; i++ {
		tm.shards[i] = &timestampShard{
			data: make(map[VectorID]int64),
		}
	}
	return tm
}

func (tm *TimestampMap) getShard(id VectorID) *timestampShard {
	return tm.shards[id%16]
}

// Get returns the timestamp for a given ID.
func (tm *TimestampMap) Get(id VectorID) int64 {
	s := tm.getShard(id)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data[id]
}

// Update updates the timestamp if the new one is greater (LWW).
// Returns true if the update was applied.
func (tm *TimestampMap) Update(id VectorID, ts int64) bool {
	s := tm.getShard(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	if ts > s.data[id] {
		s.data[id] = ts
		return true
	}
	return false
}

// Len returns the total number of entries tracked.
func (tm *TimestampMap) Len() int {
	total := 0
	for i := 0; i < 16; i++ {
		tm.shards[i].mu.RLock()
		total += len(tm.shards[i].data)
		tm.shards[i].mu.RUnlock()
	}
	return total
}
