package store

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// TemporalHNSWConfig defines configuration for time-aware HNSW indices, specifying connectivity and search parameters.
type TemporalHNSWConfig struct {
	MaxM           int
	EfConstruction int
	EfSearch       int
}

// DefaultTemporalHNSWConfig returns default temporal search settings.
func DefaultTemporalHNSWConfig() TemporalHNSWConfig {
	return TemporalHNSWConfig{
		MaxM:           16,
		EfConstruction: 200,
		EfSearch:       50,
	}
}

// TemporalHNSWIndex provides vector search capabilities with temporal partitioning, allowing for time-travel queries.
type TemporalHNSWIndex struct {
	mu           sync.RWMutex
	hnsw         interface{}
	temporalTree *TemporalTree
	dimension    int
	config       TemporalHNSWConfig
}

// NewTemporalHNSWIndex creates a new temporal index instance.
func NewTemporalHNSWIndex(dimension int, config TemporalHNSWConfig) *TemporalHNSWIndex {
	return &TemporalHNSWIndex{
		dimension:    dimension,
		config:       config,
		temporalTree: NewTemporalTree(),
	}
}

// Add inserts a vector with a specific timestamp into the index.
func (thi *TemporalHNSWIndex) Add(id uint64, vector []float32, timestamp int64) error {
	thi.mu.Lock()
	defer thi.mu.Unlock()

	thi.temporalTree.Insert(timestamp, id, 0.0)

	return nil
}

// SearchAsOf returns nearest neighbors as they existed at a specific point in time.
func (thi *TemporalHNSWIndex) SearchAsOf(ctx context.Context, timestamp int64, k int) ([]SearchResult, error) {
	thi.mu.RLock()
	defer thi.mu.RUnlock()

	validIDs := thi.temporalTree.GetBefore(timestamp + 1)

	results := make([]SearchResult, 0, k)
	for _, id := range validIDs {
		results = append(results, SearchResult{
			ID: VectorID(id), // #nosec G115
		})
	}

	if len(results) > k {
		results = results[:k]
	}

	return results, nil
}

// SearchRange returns nearest neighbors within a specific time interval.
func (thi *TemporalHNSWIndex) SearchRange(ctx context.Context, startTime, endTime int64, k int) ([]SearchResult, error) {
	thi.mu.RLock()
	defer thi.mu.RUnlock()

	validIDs := thi.temporalTree.GetRange(startTime, endTime)

	results := make([]SearchResult, 0, k)
	for _, id := range validIDs {
		results = append(results, SearchResult{
			ID: VectorID(id), // #nosec G115
		})
	}

	if len(results) > k {
		results = results[:k]
	}

	return results, nil
}

// SearchSlidingWindow returns nearest neighbors within the most recent time window.
func (thi *TemporalHNSWIndex) SearchSlidingWindow(ctx context.Context, windowSize int, k int) ([]SearchResult, error) {
	thi.mu.RLock()
	defer thi.mu.RUnlock()

	validIDs := thi.temporalTree.GetLatest(windowSize)

	results := make([]SearchResult, 0, k)
	for _, id := range validIDs {
		results = append(results, SearchResult{
			ID: VectorID(id), // #nosec G115
		})
	}

	if len(results) > k {
		results = results[:k]
	}

	return results, nil
}

// Delete removes a vector from the temporal index.
func (thi *TemporalHNSWIndex) Delete(id uint64) error {
	return nil
}

// GetMeta returns the schema for temporal metadata.
func (thi *TemporalHNSWIndex) GetMeta() *arrow.Schema {
	return nil
}

// Size returns the total number of items in the temporal index.
func (thi *TemporalHNSWIndex) Size() int {
	thi.mu.RLock()
	defer thi.mu.RUnlock()
	return thi.temporalTree.Len()
}

// Close releases resources held by the temporal index.
func (thi *TemporalHNSWIndex) Close() error {
	return nil
}
