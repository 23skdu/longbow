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
)
