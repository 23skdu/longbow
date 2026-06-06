package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ArenaAllocatedBytes tracks total bytes allocated in SlabArenas
	ArenaAllocatedBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_arena_allocated_bytes",
			Help: "Total bytes allocated in SlabArenas",
		},
		[]string{"type"}, // e.g. "slab", "typed"
	)

	// ArenaSlabsTotal tracks total number of slabs created
	ArenaSlabsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_arena_slabs_total",
			Help: "Total number of slabs allocated",
		},
	)

	// ArenaFastPathTotal tracks allocations using the lock-free fast path
	ArenaFastPathTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_arena_fast_path_total",
			Help: "Total number of allocations using the lock-free fast path",
		},
	)

	// ArenaSlowPathTotal tracks allocations using the mutex-based slow path
	ArenaSlowPathTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_arena_slow_path_total",
			Help: "Total number of allocations using the mutex-based slow path",
		},
	)

	// ArenaFastPathFailedTotal tracks fast path failures (CAS contention)
	ArenaFastPathFailedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_arena_fast_path_failed_total",
			Help: "Total number of fast path allocations that failed and fell back to slow path",
		},
	)

	// ArenaOffHeapBytes tracks total bytes allocated in off-heap SlabArenas
	ArenaOffHeapBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_arena_off_heap_bytes",
			Help: "Total bytes allocated in off-heap SlabArenas (mmap/C.malloc)",
		},
	)

	// ArenaNilErrorTotal counts occurrences of `"arena is nil"` errors
	// returned by the TypedArena allocator family (AllocSlice, AllocSliceDirty,
	// AllocSliceAligned). Should remain at 0 in healthy operation. Non-zero
	// values indicate a regression of the reader-pin contract introduced in
	// commit a2f535ef — a goroutine is calling a typed-arena allocator
	// after the underlying Slab has been released, or the parent GraphData
	// pointer was replaced by a concurrent compareAndSwapData before this
	// goroutine acquired a reader pin.
	//
	// Implemented as a Counter (not a Gauge) because the value is
	// monotonically increasing. The `method` label distinguishes the
	// allocator that fired, which makes it possible to attribute the
	// regression to the right call site from Grafana alone.
	ArenaNilErrorTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_arena_nil_error_total",
			Help: "Total number of 'arena is nil' errors from TypedArena allocators",
		},
		[]string{"method"},
	)
)
