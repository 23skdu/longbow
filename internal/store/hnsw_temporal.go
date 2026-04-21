package store

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

type TemporalHNSWConfig struct {
	MaxM           int
	EfConstruction int
	EfSearch       int
}

func DefaultTemporalHNSWConfig() TemporalHNSWConfig {
	return TemporalHNSWConfig{
		MaxM:           16,
		EfConstruction: 200,
		EfSearch:       50,
	}
}

type TemporalHNSWIndex struct {
	mu           sync.RWMutex
	hnsw         interface{}
	temporalTree *TemporalTree
	dimension    int
	config       TemporalHNSWConfig
}

func NewTemporalHNSWIndex(dimension int, config TemporalHNSWConfig) *TemporalHNSWIndex {
	return &TemporalHNSWIndex{
		dimension:    dimension,
		config:       config,
		temporalTree: NewTemporalTree(),
	}
}

func (thi *TemporalHNSWIndex) Add(id uint64, vector []float32, timestamp int64) error {
	thi.mu.Lock()
	defer thi.mu.Unlock()

	thi.temporalTree.Insert(timestamp, id)

	return nil
}

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

func (thi *TemporalHNSWIndex) Delete(id uint64) error {
	return nil
}

func (thi *TemporalHNSWIndex) GetMeta() *arrow.Schema {
	return nil
}

func (thi *TemporalHNSWIndex) Size() int {
	thi.mu.RLock()
	defer thi.mu.RUnlock()
	return thi.temporalTree.Len()
}

func (thi *TemporalHNSWIndex) Close() error {
	return nil
}
