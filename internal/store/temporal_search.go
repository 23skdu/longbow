package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

type TemporalConfig struct {
	Enabled            bool
	VersionHistory     bool
	MaxVersions        int
	RetentionPeriod    time.Duration
	TTLEnabled         bool
	DefaultTTL         time.Duration
	CleanupInterval    time.Duration
	AggregationEnabled bool
	MaxBuckets         int
}

func DefaultTemporalConfig() TemporalConfig {
	return TemporalConfig{
		Enabled:            false,
		VersionHistory:     false,
		MaxVersions:        10,
		RetentionPeriod:    7 * 24 * time.Hour,
		TTLEnabled:         false,
		DefaultTTL:         30 * 24 * time.Hour,
		CleanupInterval:    time.Hour,
		AggregationEnabled: false,
		MaxBuckets:         1000,
	}
}

type TemporalVector struct {
	ID        uint64
	Vector    []float32
	Timestamp int64
	Metadata  map[string]interface{}
	Tombstone bool
}

type TemporalIndex struct {
	mu           sync.RWMutex
	dimension    int
	vectors      map[uint64]*TemporalVector
	temporalTree *TemporalTree
	byTimestamp  map[int64][]uint64
}

type TemporalTree struct {
	mu     sync.RWMutex
	nodes  map[int64]*TemporalNode
	sorted []int64
}

type TemporalNode struct {
	Timestamp int64
	VectorIDs []uint64
}

func NewTemporalTree() *TemporalTree {
	return &TemporalTree{
		nodes:  make(map[int64]*TemporalNode),
		sorted: make([]int64, 0),
	}
}

func (tt *TemporalTree) Insert(timestamp int64, id uint64) {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	if node, ok := tt.nodes[timestamp]; ok {
		node.VectorIDs = append(node.VectorIDs, id)
	} else {
		tt.nodes[timestamp] = &TemporalNode{
			Timestamp: timestamp,
			VectorIDs: []uint64{id},
		}
		tt.sorted = append(tt.sorted, timestamp)
		sort.Slice(tt.sorted, func(i, j int) bool {
			return tt.sorted[i] < tt.sorted[j]
		})
	}
}

func (tt *TemporalTree) GetRange(start, end int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	var results []uint64
	for _, ts := range tt.sorted {
		if ts >= start && ts <= end {
			results = append(results, tt.nodes[ts].VectorIDs...)
		}
	}
	return results
}

func (tt *TemporalTree) GetRangeReversed(start, end int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	var results []uint64
	for i := len(tt.sorted) - 1; i >= 0; i-- {
		ts := tt.sorted[i]
		if ts >= start && ts <= end {
			results = append(results, tt.nodes[ts].VectorIDs...)
		}
	}
	return results
}

func (tt *TemporalTree) GetBefore(timestamp int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	var results []uint64
	for _, ts := range tt.sorted {
		if ts < timestamp {
			results = append(results, tt.nodes[ts].VectorIDs...)
		}
	}
	return results
}

func (tt *TemporalTree) GetAfter(timestamp int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	var results []uint64
	for _, ts := range tt.sorted {
		if ts > timestamp {
			results = append(results, tt.nodes[ts].VectorIDs...)
		}
	}
	return results
}

func (tt *TemporalTree) GetLatest(n int) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if n > len(tt.sorted) {
		n = len(tt.sorted)
	}

	var results []uint64
	for i := len(tt.sorted) - n; i < len(tt.sorted); i++ {
		results = append(results, tt.nodes[tt.sorted[i]].VectorIDs...)
	}
	return results
}

func (tt *TemporalTree) GetEarliest(n int) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if n > len(tt.sorted) {
		n = len(tt.sorted)
	}

	var results []uint64
	for i := 0; i < n; i++ {
		results = append(results, tt.nodes[tt.sorted[i]].VectorIDs...)
	}
	return results
}

func (tt *TemporalTree) Len() int {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	return len(tt.sorted)
}

func NewTemporalIndex(dimension int) *TemporalIndex {
	return &TemporalIndex{
		dimension:    dimension,
		vectors:      make(map[uint64]*TemporalVector),
		temporalTree: NewTemporalTree(),
		byTimestamp:  make(map[int64][]uint64),
	}
}

func (ti *TemporalIndex) Add(id uint64, vector []float32, timestamp int64, metadata map[string]interface{}) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	if len(vector) != ti.dimension {
		return fmt.Errorf("dimension mismatch: expected %d, got %d", ti.dimension, len(vector))
	}

	vec := &TemporalVector{
		ID:        id,
		Vector:    vector,
		Timestamp: timestamp,
		Metadata:  metadata,
		Tombstone: false,
	}

	ti.vectors[id] = vec
	ti.temporalTree.Insert(timestamp, id)
	ti.byTimestamp[timestamp] = append(ti.byTimestamp[timestamp], id)

	return nil
}

func (ti *TemporalIndex) Delete(id uint64) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	vec, ok := ti.vectors[id]
	if !ok {
		return fmt.Errorf("vector id %d not found", id)
	}

	vec.Tombstone = true
	return nil
}

func (ti *TemporalIndex) Update(id uint64, vector []float32, timestamp int64, metadata map[string]interface{}) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	oldVec, ok := ti.vectors[id]
	if !ok {
		return fmt.Errorf("vector id %d not found", id)
	}

	oldVec.Tombstone = true

	newVec := &TemporalVector{
		ID:        id,
		Vector:    vector,
		Timestamp: timestamp,
		Metadata:  metadata,
		Tombstone: false,
	}

	ti.vectors[id] = newVec
	ti.temporalTree.Insert(timestamp, id)
	ti.byTimestamp[timestamp] = append(ti.byTimestamp[timestamp], id)

	return nil
}

func (ti *TemporalIndex) SearchAsOf(ctx context.Context, timestamp int64, k int) ([]lbtypes.SearchResult, error) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	validIDs := ti.temporalTree.GetBefore(timestamp + 1)

	type scoredResult struct {
		id       uint64
		distance float64
	}

	var results []scoredResult
	for _, id := range validIDs {
		vec := ti.vectors[id]
		if vec.Tombstone {
			continue
		}
		dist := float64(ti.computeNorm(vec.Vector))
		results = append(results, scoredResult{id: id, distance: dist})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	searchResults := make([]lbtypes.SearchResult, 0, min(k, len(results)))
	for i := 0; i < min(k, len(results)); i++ {
		searchResults = append(searchResults, lbtypes.SearchResult{
			ID:       lbtypes.VectorID(results[i].id), // #nosec G115
			Distance: float32(results[i].distance),
			Score:    float32(1.0 / (1.0 + results[i].distance)),
		})
	}

	return searchResults, nil
}

func (ti *TemporalIndex) SearchRange(ctx context.Context, startTime, endTime int64, k int) ([]lbtypes.SearchResult, error) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	validIDs := ti.temporalTree.GetRange(startTime, endTime)

	type scoredResult struct {
		id       uint64
		distance float64
	}

	var results []scoredResult
	for _, id := range validIDs {
		vec := ti.vectors[id]
		if vec.Tombstone {
			continue
		}
		dist := float64(ti.computeNorm(vec.Vector))
		results = append(results, scoredResult{id: id, distance: dist})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	searchResults := make([]lbtypes.SearchResult, 0, min(k, len(results)))
	for i := 0; i < min(k, len(results)); i++ {
		searchResults = append(searchResults, lbtypes.SearchResult{
			ID:       lbtypes.VectorID(results[i].id), // #nosec G115
			Distance: float32(results[i].distance),
			Score:    float32(1.0 / (1.0 + results[i].distance)),
		})
	}

	return searchResults, nil
}

func (ti *TemporalIndex) SearchSlidingWindow(ctx context.Context, windowSize int, k int) ([]lbtypes.SearchResult, error) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	validIDs := ti.temporalTree.GetLatest(windowSize)

	type scoredResult struct {
		id       uint64
		distance float64
	}

	var results []scoredResult
	for _, id := range validIDs {
		vec := ti.vectors[id]
		if vec.Tombstone {
			continue
		}
		dist := float64(ti.computeNorm(vec.Vector))
		results = append(results, scoredResult{id: id, distance: dist})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	searchResults := make([]lbtypes.SearchResult, 0, min(k, len(results)))
	for i := 0; i < min(k, len(results)); i++ {
		searchResults = append(searchResults, lbtypes.SearchResult{
			ID:       lbtypes.VectorID(results[i].id), // #nosec G115
			Distance: float32(results[i].distance),
			Score:    float32(1.0 / (1.0 + results[i].distance)),
		})
	}

	return searchResults, nil
}

func (ti *TemporalIndex) SearchSlidingWindowByTime(ctx context.Context, duration time.Duration, k int) ([]lbtypes.SearchResult, error) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	now := time.Now().UnixNano()
	windowStart := now - duration.Nanoseconds()

	validIDs := ti.temporalTree.GetRange(windowStart, now)

	type scoredResult struct {
		id       uint64
		distance float64
	}

	var results []scoredResult
	for _, id := range validIDs {
		vec := ti.vectors[id]
		if vec.Tombstone {
			continue
		}
		dist := float64(ti.computeNorm(vec.Vector))
		results = append(results, scoredResult{id: id, distance: dist})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	searchResults := make([]lbtypes.SearchResult, 0, min(k, len(results)))
	for i := 0; i < min(k, len(results)); i++ {
		searchResults = append(searchResults, lbtypes.SearchResult{
			ID:       lbtypes.VectorID(results[i].id), // #nosec G115
			Distance: float32(results[i].distance),
			Score:    float32(1.0 / (1.0 + results[i].distance)),
		})
	}

	return searchResults, nil
}

func (ti *TemporalIndex) DeleteByTime(ctx context.Context, beforeTimestamp int64) (int, error) {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	toDelete := ti.temporalTree.GetBefore(beforeTimestamp)
	deleted := 0

	for _, id := range toDelete {
		vec := ti.vectors[id]
		vec.Tombstone = true
		deleted++
	}

	return deleted, nil
}

func (ti *TemporalIndex) GetVersion(id uint64, timestamp int64) ([]float32, bool) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	vec, ok := ti.vectors[id]
	if !ok {
		return nil, false
	}

	if vec.Timestamp > timestamp {
		return nil, false
	}

	return vec.Vector, true
}

func (ti *TemporalIndex) GetHistory(id uint64) []TemporalVector {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	vec, ok := ti.vectors[id]
	if !ok {
		return nil
	}

	if vec.Timestamp == 0 {
		return nil
	}

	return []TemporalVector{*vec}
}

func (ti *TemporalIndex) Size() int {
	ti.mu.RLock()
	defer ti.mu.RUnlock()
	return len(ti.vectors)
}

func (ti *TemporalIndex) ActiveCount() int {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	count := 0
	for _, v := range ti.vectors {
		if !v.Tombstone {
			count++
		}
	}
	return count
}

func (ti *TemporalIndex) computeNorm(vector []float32) float32 {
	var sum float32
	for _, v := range vector {
		sum += v * v
	}
	return sum
}

type TemporalSearchRequest struct {
	SearchType string        `json:"search_type"` // "as_of", "range", "sliding_window", "sliding_window_time"
	K          int           `json:"k"`
	Timestamp  int64         `json:"timestamp,omitempty"`
	StartTime  int64         `json:"start_time,omitempty"`
	EndTime    int64         `json:"end_time,omitempty"`
	WindowSize int           `json:"window_size,omitempty"`
	Duration   time.Duration `json:"duration,omitempty"`
}

func (req *TemporalSearchRequest) Validate() error {
	if req.K <= 0 {
		req.K = 10
	}

	switch req.SearchType {
	case "as_of":
		if req.Timestamp <= 0 {
			return fmt.Errorf("timestamp required for as_of search")
		}
	case "range":
		if req.StartTime <= 0 || req.EndTime <= 0 {
			return fmt.Errorf("start_time and end_time required for range search")
		}
	case "sliding_window":
		if req.WindowSize <= 0 {
			req.WindowSize = 100
		}
	case "sliding_window_time":
		if req.Duration <= 0 {
			req.Duration = time.Hour
		}
	default:
		req.SearchType = "as_of"
	}

	return nil
}

type VectorTimestamp struct {
	ID        uint64                 `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	Vector    []float32              `json:"vector,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

func (ti *TemporalIndex) GetVectorsInRange(startTime, endTime int64) []VectorTimestamp {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	ids := ti.temporalTree.GetRange(startTime, endTime)

	results := make([]VectorTimestamp, 0, len(ids))
	for _, id := range ids {
		vec := ti.vectors[id]
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

func (ti *TemporalIndex) MarshalJSON() ([]byte, error) {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	type TempVec struct {
		ID        uint64                 `json:"id"`
		Vector    []float32              `json:"vector"`
		Timestamp int64                  `json:"timestamp"`
		Metadata  map[string]interface{} `json:"metadata"`
		Tombstone bool                   `json:"tombstone"`
	}

	vectors := make([]TempVec, 0, len(ti.vectors))
	for _, v := range ti.vectors {
		vectors = append(vectors, TempVec{
			ID:        v.ID,
			Vector:    v.Vector,
			Timestamp: v.Timestamp,
			Metadata:  v.Metadata,
			Tombstone: v.Tombstone,
		})
	}

	return json.Marshal(struct {
		Dimension int       `json:"dimension"`
		Vectors   []TempVec `json:"vectors"`
	}{
		Dimension: ti.dimension,
		Vectors:   vectors,
	})
}
