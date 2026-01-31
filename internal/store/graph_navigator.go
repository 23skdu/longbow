package store

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/prometheus/client_golang/prometheus"
)

// Navigator defines the interface for graph navigation operations.
type Navigator interface {
	Initialize() error
	FindPath(ctx context.Context, query NavigatorQuery) (*NavigatorPath, error)
	GetMetrics() *NavigatorMetrics
	IsInitialized() bool
}

type GraphNavigator struct {
	mu            sync.RWMutex
	graphProvider func() *types.GraphData
	metrics       *NavigatorMetrics
	searchConfig  NavigatorConfig
	isInitialized atomic.Bool
	cache         map[string]cachedResult
	planner       *QueryPlanner
	cacheMu       sync.RWMutex
}

type cachedResult struct {
	path      *NavigatorPath
	version   uint64
	timestamp time.Time
}

type NavigatorConfig struct {
	MaxHops           int
	SearchRadius      float32
	Concurrency       int
	EarlyTerminate    bool
	DistanceThreshold float32
	EnableCaching     bool
	CacheTTL          time.Duration
}

type NavigatorPath struct {
	StartID   uint32
	EndID     uint32
	Path      []uint32
	Distances []float32
	Hops      int
	Found     bool
}

type NavigatorQuery struct {
	StartID     uint32
	TargetID    uint32
	MaxHops     int
	Constraints []PathConstraint
}

type PathConstraint struct {
	Type      ConstraintType
	Threshold float32
}

type ConstraintType int

const (
	NoConstraint ConstraintType = iota
	MaxDistanceConstraint
	MinSimilarityConstraint
	AvoidNodesConstraint
)

type NavigatorMetrics struct {
	QueriesTotal      prometheus.Counter
	QueriesDuration   prometheus.Histogram
	HopsPerQuery      prometheus.Histogram
	PathsFound        prometheus.Counter
	EarlyTerminations prometheus.Counter
	ConcurrentQueries prometheus.Gauge
	CacheHits         prometheus.Counter
	CacheMisses       prometheus.Counter
}

func NewNavigatorMetrics(reg prometheus.Registerer) *NavigatorMetrics {
	m := &NavigatorMetrics{
		QueriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "graph_navigator_queries_total",
			Help: "Total number of graph navigation queries",
		}),
		QueriesDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "graph_navigator_query_duration_seconds",
			Help:    "Duration of graph navigation queries",
			Buckets: prometheus.DefBuckets,
		}),
		HopsPerQuery: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "graph_navigator_hops_per_query",
			Help:    "Number of hops per navigation query",
			Buckets: prometheus.LinearBuckets(1, 1, 50),
		}),
		PathsFound: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "graph_navigator_paths_found_total",
			Help: "Total number of paths found successfully",
		}),
		EarlyTerminations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "graph_navigator_early_terminations_total",
			Help: "Total number of early terminations due to sparse graph structure",
		}),
		ConcurrentQueries: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "graph_navigator_concurrent_queries",
			Help: "Current number of concurrent navigation queries",
		}),
		CacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "graph_navigator_cache_hits_total",
			Help: "Total number of cache hits during navigation",
		}),
		CacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "graph_navigator_cache_misses_total",
			Help: "Total number of cache misses during navigation",
		}),
	}

	if reg != nil {
		reg.MustRegister(
			m.QueriesTotal,
			m.QueriesDuration,
			m.HopsPerQuery,
			m.PathsFound,
			m.EarlyTerminations,
			m.ConcurrentQueries,
			m.CacheHits,
			m.CacheMisses,
		)
	}

	return m
}

func NewGraphNavigator(graphProvider func() *types.GraphData, config NavigatorConfig, reg prometheus.Registerer) *GraphNavigator {
	return &GraphNavigator{
		graphProvider: graphProvider,
		searchConfig:  config,
		metrics:       NewNavigatorMetrics(reg),
		cache:         make(map[string]cachedResult),
		planner:       NewQueryPlanner(),
	}
}

func (gn *GraphNavigator) Initialize() error {
	gn.mu.Lock()
	defer gn.mu.Unlock()

	if gn.graphProvider == nil {
		return fmt.Errorf("graph provider cannot be nil")
	}
	if gn.graphProvider() == nil {
		return fmt.Errorf("graph data cannot be nil")
	}

	gn.isInitialized.Store(true)
	return nil
}

func (gn *GraphNavigator) FindPath(ctx context.Context, query NavigatorQuery) (*NavigatorPath, error) {
	if !gn.isInitialized.Load() {
		return nil, fmt.Errorf("navigator not initialized")
	}

	start := time.Now()
	defer func() {
		gn.metrics.QueriesDuration.Observe(time.Since(start).Seconds())
		gn.metrics.QueriesTotal.Inc()
	}()

	gn.metrics.ConcurrentQueries.Inc()
	defer gn.metrics.ConcurrentQueries.Dec()

	// Check cache if enabled
	var cacheKey string
	if gn.searchConfig.EnableCaching {
		cacheKey = fmt.Sprintf("%d:%d:%d:%v", query.StartID, query.TargetID, query.MaxHops, query.Constraints)
		gn.cacheMu.RLock()
		if cached, ok := gn.cache[cacheKey]; ok {
			// Validate version
			currentGraph := gn.graphProvider()
			if currentGraph != nil {
				currentVersion := atomic.LoadUint64(&currentGraph.GlobalVersion)
				if cached.version == currentVersion && (gn.searchConfig.CacheTTL == 0 || time.Since(cached.timestamp) < gn.searchConfig.CacheTTL) {
					gn.cacheMu.RUnlock()
					gn.metrics.CacheHits.Inc()
					return cached.path, nil
				}
			}
		}
		gn.cacheMu.RUnlock()
		gn.metrics.CacheMisses.Inc()
	}

	// Use Planner to determine strategy
	strategy := gn.planner.Plan(query)

	// Execute strategy
	path, err := strategy.FindPath(ctx, gn, query)
	if err != nil {
		return nil, err
	}

	if path.Found {
		gn.metrics.PathsFound.Inc()
		gn.metrics.HopsPerQuery.Observe(float64(path.Hops))
	}

	// Update cache
	if gn.searchConfig.EnableCaching && path.Found {
		currentGraph := gn.graphProvider()
		if currentGraph != nil {
			currentVersion := atomic.LoadUint64(&currentGraph.GlobalVersion)
			gn.cacheMu.Lock()
			gn.cache[cacheKey] = cachedResult{
				path:      path,
				version:   currentVersion,
				timestamp: time.Now(),
			}
			gn.cacheMu.Unlock()
		}
	}

	return path, nil
}

func (gn *GraphNavigator) getNeighbors(nodeID uint32) ([]uint32, bool) {
	gn.mu.RLock()
	defer gn.mu.RUnlock()

	graph := gn.graphProvider()
	if graph == nil {
		return nil, false
	}

	for layer := 0; layer < types.ArrowMaxLayers; layer++ {
		neighbors := graph.GetNeighbors(layer, nodeID, []uint32{})
		if len(neighbors) > 0 {
			return neighbors, true
		}
	}

	return nil, false
}

func (gn *GraphNavigator) shouldTerminateEarly(currentID, targetID uint32, hops int) bool {
	if hops < 2 {
		return false
	}

	if gn.searchConfig.DistanceThreshold > 0 {
		if distance := gn.calculateDistance(currentID, targetID); distance > gn.searchConfig.DistanceThreshold {
			return true
		}
	}

	if gn.searchConfig.SearchRadius > 0 {
		neighbors, _ := gn.getNeighbors(currentID)
		neighborCount := float32(len(neighbors))
		if neighborCount < gn.searchConfig.SearchRadius {
			return true
		}
	}

	return false
}

func (gn *GraphNavigator) calculateDistance(node1, node2 uint32) float32 {
	graph := gn.graphProvider()
	if graph == nil {
		return math.MaxFloat32
	}

	vec1, err := graph.GetVector(node1)
	if err != nil {
		return math.MaxFloat32
	}

	vec2, err := graph.GetVector(node2)
	if err != nil {
		return math.MaxFloat32
	}

	v1, ok1 := vec1.([]float32)
	if !ok1 {
		return math.MaxFloat32
	}

	v2, ok2 := vec2.([]float32)
	if !ok2 {
		return math.MaxFloat32
	}

	if len(v1) != len(v2) {
		return math.MaxFloat32
	}

	// Use SIMD optimization
	dist, err := simd.DistFunc(v1, v2)
	if err != nil {
		return math.MaxFloat32
	}
	return dist
}

func (gn *GraphNavigator) GetMetrics() *NavigatorMetrics {
	return gn.metrics
}

func (gn *GraphNavigator) IsInitialized() bool {
	return gn.isInitialized.Load()
}
