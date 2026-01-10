package memory

import (
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TrackingAllocator wraps a base memory.Allocator and updates Prometheus metrics
type TrackingAllocator struct {
	memory.Allocator
	// Exposed for testing validity, but main purpose is metrics
	BytesAllocated atomic.Int64
	BytesFreed     atomic.Int64
}

// NewTrackingAllocator creates a new allocator that wraps the given base allocator.
// If base is nil, it uses memory.DefaultAllocator.
func NewTrackingAllocator(base memory.Allocator) *TrackingAllocator {
	if base == nil {
		base = memory.DefaultAllocator
	}
	return &TrackingAllocator{Allocator: base}
}

func (a *TrackingAllocator) Allocate(size int) []byte {
	a.BytesAllocated.Add(int64(size))
	metrics.AllocatorBytesAllocatedTotal.Add(float64(size))
	metrics.AllocatorAllocationsActive.Inc()
	return a.Allocator.Allocate(size)
}

func (a *TrackingAllocator) Reallocate(size int, b []byte) []byte {
	// Reallocate is hard to track accurately without knowing old size.
	// We count it as new allocation for "AllocatedTotal" metrics to show churn/activity.
	// Active count remains same (1 obj -> 1 obj).
	a.BytesAllocated.Add(int64(size))
	metrics.AllocatorBytesAllocatedTotal.Add(float64(size))
	return a.Allocator.Reallocate(size, b)
}

func (a *TrackingAllocator) Free(b []byte) {
	a.BytesFreed.Add(int64(len(b)))
	metrics.AllocatorBytesFreedTotal.Add(float64(len(b)))
	metrics.AllocatorAllocationsActive.Dec()
	a.Allocator.Free(b)
}

// Ensure interface satisfaction
var _ memory.Allocator = (*TrackingAllocator)(nil)
