package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Memory Management Metrics
var (
	// MemoryLimitRejects counts writes rejected due to memory limit
	MemoryLimitRejects = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_memory_limit_rejects_total",
			Help: "Total number of writes rejected due to memory limit",
		},
	)

	// MemoryEvictionsTriggered counts evictions triggered by writes
	MemoryEvictionsTriggered = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_memory_evictions_triggered_total",
			Help: "Total number of evictions triggered by write operations",
		},
	)

	// MemoryCurrentBytes tracks current memory usage
	MemoryCurrentBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_memory_current_bytes",
			Help: "Current memory usage in bytes",
		},
	)

	// MemoryLimitBytes tracks configured memory limit
	MemoryLimitBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_memory_limit_bytes",
			Help: "Configured memory limit in bytes",
		},
	)

	// MemoryUtilization tracks memory utilization percentage
	MemoryUtilization = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_memory_utilization",
			Help: "Memory utilization (currentMemory / maxMemory)",
		},
	)

	// StoreVectorsManagedCount tracks number of vectors stored in managed arenas
	StoreVectorsManagedCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_store_vectors_managed_count",
			Help: "Total number of vectors stored in managed arenas (SlabArena)",
		},
	)

	// ArenaAllocationTotal counts total vector allocations from arena
	ArenaAllocationTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_arena_allocations_total",
			Help: "Total number of vector allocations from arena",
		},
	)

	// ArenaHitRate tracks arena fast-path utilization
	ArenaHitRate = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_arena_hit_rate",
			Help: "Arena fast-path hit rate (0-1, higher is better)",
		},
	)

	// ArenaBytesAllocated tracks total bytes allocated from arena
	ArenaBytesAllocated = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_arena_bytes_allocated",
			Help: "Total bytes allocated from arena",
		},
	)

	// ArenaSlabAllocations counts slab allocations
	ArenaSlabAllocations = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_arena_slab_allocations_total",
			Help: "Total number of slab allocations in arena",
		},
	)

	// AdjacencyPaddingBytes tracks bytes used for alignment padding
	AdjacencyPaddingBytes = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_adjacency_padding_bytes_total",
			Help: "Total bytes used for alignment padding in graph adjacency",
		},
	)
)
