package temporal

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// AddBatchNorms inserts a batch of temporal entries using precomputed L2 norms
// without retaining duplicate per-row []float32 vector copies on the Go heap.
// Used by the ingestion path when vectors are already stored off-heap in the
// dataset's primary vector index.
func (ti *TemporalIndex) AddBatchNorms(ids []uint64, norms []float32, timestamps []int64, dim int) error {
	n := len(ids)
	if n == 0 {
		return nil
	}
	if len(norms) != n || len(timestamps) != n {
		return fmt.Errorf("mismatched batch lengths: ids=%d norms=%d timestamps=%d", n, len(norms), len(timestamps))
	}

	ti.mu.Lock()
	if ti.dimension == 0 && dim > 0 {
		ti.dimension = dim
	}
	for i := range ids {
		vec := &TemporalVector{
			ID:        ids[i],
			Norm:      norms[i],
			Timestamp: timestamps[i],
			Tombstone: false,
		}
		shard := ti.getShard(ids[i])
		shard.mu.Lock()
		shard.data[ids[i]] = vec
		shard.mu.Unlock()
		if ti.history != nil {
			ti.history.Add(ids[i], nil, norms[i], timestamps[i], nil)
		}
	}

	if ti.asyncIngest.Load() {
		task := temporalIngestTask{
			timestamps: timestamps,
			ids:        ids,
			norms:      norms,
		}
		select {
		case ti.ingestCh <- task:
		default:
			tree := ti.temporalTree.Load()
			if tree != nil {
				tree.InsertBatch(timestamps, ids, norms)
			}
			segmentTree := ti.segmentTree.Load()
			if segmentTree != nil {
				uids := make([]uint32, n)
				for i, id := range ids {
					uids[i] = uint32(id) // #nosec G115
				}
				segmentTree.InsertBatch(timestamps, timestamps, uids)
			}
		}
	} else {
		tree := ti.temporalTree.Load()
		if tree != nil {
			tree.InsertBatch(timestamps, ids, norms)
		}
		segmentTree := ti.segmentTree.Load()
		if segmentTree != nil {
			uids := make([]uint32, n)
			for i, id := range ids {
				uids[i] = uint32(id) // #nosec G115
			}
			segmentTree.InsertBatch(timestamps, timestamps, uids)
		}
	}
	ti.pointCount.Add(int64(n))
	ti.mutationSeq.Add(1)
	ti.mu.Unlock()

	return nil
}

// Delete marks a vector as deleted (tombstoned) in the temporal index.
func (ti *TemporalIndex) Delete(id uint64) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	shard := ti.getShard(id)
	shard.mu.RLock()
	vec, ok := shard.data[id]
	shard.mu.RUnlock()
	if !ok {
		return fmt.Errorf("vector id %d not found", id)
	}

	newVec := *vec
	newVec.Tombstone = true
	shard.mu.Lock()
	shard.data[id] = &newVec
	shard.mu.Unlock()

	segmentTree := ti.segmentTree.Load()
	if segmentTree != nil {
		segmentTree.Remove(vec.Timestamp, vec.Timestamp, uint32(id)) // #nosec G115
	}
	ti.mutationSeq.Add(1)
	// We don't decrement pointCount here because it's a tombstone
	return nil
}

// Update updates an existing vector and metadata at a new timestamp.
func (ti *TemporalIndex) Update(id uint64, vector []float32, timestamp int64, metadata []byte) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	shard := ti.getShard(id)
	shard.mu.RLock()
	oldVec, ok := shard.data[id]
	shard.mu.RUnlock()
	if !ok {
		return fmt.Errorf("vector id %d not found", id)
	}

	newOldVec := *oldVec
	newOldVec.Tombstone = true
	shard.mu.Lock()
	shard.data[id] = &newOldVec
	shard.mu.Unlock()

	norm := ti.computeNorm(vector)
	newVec := &TemporalVector{
		ID:        id,
		Vector:    vector,
		Norm:      norm,
		Timestamp: timestamp,
		Metadata:  metadata,
		Tombstone: false,
	}

	shard.mu.Lock()
	shard.data[id] = newVec
	shard.mu.Unlock()
	tree := ti.temporalTree.Load()
	if tree != nil {
		tree.Insert(timestamp, id, norm)
	}

	if ti.history != nil {
		ti.history.Add(id, vector, norm, timestamp, metadata)
	}

	return nil
}

// DeleteByTime removes or tombstones vectors with timestamps before the specified value.
func (ti *TemporalIndex) DeleteByTime(ctx context.Context, beforeTimestamp int64) (int, error) {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	tree := ti.temporalTree.Load()
	if tree == nil {
		return 0, nil
	}
	toDelete := tree.GetBefore(beforeTimestamp)
	deleted := 0

	for _, id := range toDelete {
		shard := ti.getShard(id)
		shard.mu.RLock()
		vec, ok := shard.data[id]
		shard.mu.RUnlock()
		if ok {
			newVec := *vec
			newVec.Tombstone = true
			shard.mu.Lock()
			shard.data[id] = &newVec
			shard.mu.Unlock()
			deleted++
		}
	}

	return deleted, nil
}

// GetVersion retrieves the vector data for a specific ID as it existed at a given timestamp.
func (ti *TemporalIndex) GetVersion(id uint64, timestamp int64) ([]float32, bool) {
	shard := ti.getShard(id)
	shard.mu.RLock()
	vec, ok := shard.data[id]
	shard.mu.RUnlock()
	if !ok {
		return nil, false
	}

	if vec.Timestamp > timestamp {
		return nil, false
	}

	return vec.Vector, true
}

// GetHistory returns the temporal version history for a specific vector ID.
func (ti *TemporalIndex) GetHistory(id uint64) []TemporalVector {
	shard := ti.getShard(id)
	shard.mu.RLock()
	vec, ok := shard.data[id]
	shard.mu.RUnlock()
	if !ok {
		return nil
	}

	if vec.Timestamp == 0 {
		return nil
	}

	return []TemporalVector{*vec}
}

type VectorTimestamp struct {
	ID        uint64    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Vector    []float32 `json:"vector,omitempty"`
	Metadata  []byte    `json:"metadata,omitempty"`
}

// GetVectorsInRange retrieves all vectors within a timestamp range as VectorTimestamp objects.
func (ti *TemporalIndex) GetVectorsInRange(startTime, endTime int64) []VectorTimestamp {
	tree := ti.temporalTree.Load()
	if tree == nil {
		return nil
	}
	ids := tree.GetRange(startTime, endTime)

	results := make([]VectorTimestamp, 0, len(ids))
	for _, id := range ids {
		shard := ti.getShard(id)
		shard.mu.RLock()
		vec, ok := shard.data[id]
		shard.mu.RUnlock()
		if !ok {
			continue
		}
		if vec.Tombstone {
			continue
		}
		results = append(results, VectorTimestamp{
			ID:        vec.ID,
			Timestamp: time.Unix(0, vec.Timestamp),
			Vector:    vec.Vector,
			Metadata:  vec.Metadata,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp.Before(results[j].Timestamp)
	})

	return results
}

// MarshalJSON provides custom JSON serialization for TemporalIndex.
func (ti *TemporalIndex) MarshalJSON() ([]byte, error) {
	type TempVec struct {
		ID        uint64    `json:"id"`
		Vector    []float32 `json:"vector"`
		Timestamp int64     `json:"timestamp"`
		Metadata  []byte    `json:"metadata,omitempty"`
		Tombstone bool      `json:"tombstone,omitempty"`
	}

	var vectors []TempVec
	for i := 0; i < TemporalShards; i++ {
		ti.shards[i].mu.RLock()
		for _, v := range ti.shards[i].data {
			vectors = append(vectors, TempVec{
				ID:        v.ID,
				Vector:    v.Vector,
				Timestamp: v.Timestamp,
				Metadata:  v.Metadata,
				Tombstone: v.Tombstone,
			})
		}
		ti.shards[i].mu.RUnlock()
	}

	return json.Marshal(struct {
		Dimension int       `json:"dimension"`
		Vectors   []TempVec `json:"vectors"`
	}{
		Dimension: ti.dimension,
		Vectors:   vectors,
	})
}
