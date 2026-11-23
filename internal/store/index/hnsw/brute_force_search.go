package hnsw

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

// SearchVectors runs the search vectors operation.
func (b *BruteForceIndex) SearchVectors(ctx context.Context, q any, k int, filters []core.Filter, options any) ([]SearchResult, error) {
	qF32, ok := q.([]float32)
	if !ok {
		return nil, errors.New("BruteForceIndex only supports []float32 queries")
	}
	start := time.Now()
	b.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.GetName(), "read").Observe(time.Since(start).Seconds())
	defer b.mu.RUnlock()

	if len(b.locations) == 0 {
		return nil, nil
	}

	h := &bfSearchHeap{}
	heap.Init(h)

	dims := len(qF32)
	if b.distFunc == nil || b.dims != dims {
		b.distFunc = simd.GetKernel[float32](simd.MetricEuclidean, dims)
		if b.distFunc == nil {
			b.distFunc = simd.EuclideanDistance
		}
		b.dims = dims
	}
	kernel := b.distFunc

	for i, loc := range b.locations {
		if i%1000 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		vec, release := b.getVectorUnsafe(loc)
		if vec == nil || release == nil {
			continue
		}

		dist, err := kernel(qF32, vec)
		release()

		if err != nil {
			dist = math.MaxFloat32
		}

		if h.Len() < k {
			heap.Push(h, bfHeapItem{
				id:    VectorID(i),
				score: dist,
			})
		} else if dist < (*h)[0].score {
			heap.Pop(h)
			heap.Push(h, bfHeapItem{
				id:    VectorID(i),
				score: dist,
			})
		}
	}

	results := make([]SearchResult, h.Len())
	for i := len(results) - 1; i >= 0; i-- {
		item := heap.Pop(h).(bfHeapItem)
		results[i] = SearchResult{
			ID:    types.VectorID(item.id),
			Score: item.score,
		}
	}

	return results, nil
}

// SearchVectorsInRange returns nearest neighbors within a distance threshold.
func (b *BruteForceIndex) SearchVectorsInRange(ctx context.Context, q any, threshold float32, filters []core.Filter, options any) ([]SearchResult, error) {
	qF32, ok := q.([]float32)
	if !ok {
		return nil, errors.New("BruteForceIndex only supports []float32 queries")
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.locations) == 0 {
		return nil, nil
	}

	dims := len(qF32)
	if b.distFunc == nil || b.dims != dims {
		b.distFunc = simd.GetKernel[float32](simd.MetricEuclidean, dims)
		if b.distFunc == nil {
			b.distFunc = simd.EuclideanDistance
		}
		b.dims = dims
	}
	kernel := b.distFunc

	var results []SearchResult
	for i, loc := range b.locations {
		if i%1000 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		vec, release := b.getVectorUnsafe(loc)
		if vec == nil || release == nil {
			continue
		}

		dist, _ := kernel(qF32, vec)
		release()

		if dist <= threshold {
			results = append(results, SearchResult{
				ID:       VectorID(i),
				Distance: dist,
				Score:    1.0 / (1.0 + dist),
			})
		}
	}

	return results, nil
}

// GetIndexType returns "brute_force".
func (b *BruteForceIndex) GetIndexType() string {
	return "brute_force"
}

// Len returns the number of vectors in the brute-force index.
func (b *BruteForceIndex) Len() int {
	start := time.Now()
	b.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.GetName(), "read").Observe(time.Since(start).Seconds())
	defer b.mu.RUnlock()
	return len(b.locations)
}

// getVector retrieves a vector from the dataset.
func (b *BruteForceIndex) getVector(loc Location) []float32 {
	if b.dataset == nil || loc.BatchIdx >= len(b.dataset.GetRecords()) {
		return nil
	}

	record := b.dataset.GetRecords()[loc.BatchIdx]
	fieldIndices := record.Schema().FieldIndices("vector")
	if len(fieldIndices) == 0 {
		return nil
	}
	vecCol := record.Column(fieldIndices[0])

	list, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		return nil
	}

	values := list.ListValues().(*array.Float32).Float32Values()
	listSize := int(list.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * listSize

	if start+listSize > len(values) {
		return nil
	}

	// Return a copy to avoid data races
	result := make([]float32, listSize)
	copy(result, values[start:start+listSize])

	// Track metrics
	metrics.VectorAccessCopyTotal.WithLabelValues(b.dataset.GetName(), "brute_force").Inc()
	metrics.VectorAccessBytesAllocated.WithLabelValues(b.dataset.GetName(), "brute_force").Add(float64(listSize * 4))

	return result
}

func (b *BruteForceIndex) getVectorUnsafe(loc Location) (vec []float32, release func()) {
	b.enterEpoch()

	if b.dataset == nil || loc.BatchIdx >= len(b.dataset.GetRecords()) {
		b.exitEpoch()
		return nil, nil
	}

	record := b.dataset.GetRecords()[loc.BatchIdx]
	fieldIndices := record.Schema().FieldIndices("vector")
	if len(fieldIndices) == 0 {
		b.exitEpoch()
		return nil, nil
	}

	vecCol := record.Column(fieldIndices[0])
	list, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		b.exitEpoch()
		return nil, nil
	}

	values := list.ListValues().(*array.Float32).Float32Values()
	listSize := int(list.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * listSize

	if start+listSize > len(values) {
		b.exitEpoch()
		return nil, nil
	}

	vec = values[start : start+listSize]
	release = b.exitEpoch

	metrics.VectorAccessZeroCopyTotal.WithLabelValues(b.dataset.GetName(), "brute_force").Inc()

	return vec, release
}

func (b *BruteForceIndex) enterEpoch() {
	b.activeReaders.Add(1)
}

func (b *BruteForceIndex) exitEpoch() {
	b.activeReaders.Add(-1)
}

type bfHeapItem struct {
	id    VectorID
	score float32
}

type bfSearchHeap []bfHeapItem

// Len runs the len operation.
func (h bfSearchHeap) Len() int { return len(h) }

// Less runs the less operation.
func (h bfSearchHeap) Less(i, j int) bool { return h[i].score > h[j].score }

// Swap runs the swap operation.
func (h bfSearchHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push runs the push operation.
func (h *bfSearchHeap) Push(x any) {
	*h = append(*h, x.(bfHeapItem))
}

// Pop runs the pop operation.
func (h *bfSearchHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
