package store

import ()

type TemporalPredicate struct {
	minTs  int64
	maxTs  int64
	shards *[TemporalShards]temporalShard
}

func (tp *TemporalPredicate) IsMatch(id uint32) bool {
	shard := &tp.shards[uint64(id)%uint64(TemporalShards)]
	shard.mu.RLock()
	vec, ok := shard.data[uint64(id)]
	shard.mu.RUnlock()
	if !ok || vec.Tombstone {
		return false
	}
	return vec.Timestamp >= tp.minTs && vec.Timestamp <= tp.maxTs
}

func (tp *TemporalPredicate) MatchBatch(ids []uint32, dst []byte) {
	for i, id := range ids {
		if tp.IsMatch(id) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

// SlidingWindowPredicate implements types.HNSWPredicate for sliding window filtering.
type SlidingWindowPredicate struct {
	validIDs map[uint64]struct{}
}

func (sp *SlidingWindowPredicate) IsMatch(id uint32) bool {
	_, ok := sp.validIDs[uint64(id)]
	return ok
}

func (sp *SlidingWindowPredicate) MatchBatch(ids []uint32, dst []byte) {
	for i, id := range ids {
		if sp.IsMatch(id) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}
