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
)
