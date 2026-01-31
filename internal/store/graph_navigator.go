package store

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

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
}

type NavigatorConfig struct {
	MaxHops           int
	SearchRadius      float32
	Concurrency       int
	EarlyTerminate    bool
	DistanceThreshold float32
	EnableCaching     bool
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

	path, err := gn.findPathInternal(ctx, query.StartID, query.TargetID, query.MaxHops)
	if err != nil {
		return nil, err
	}

	if path.Found {
		gn.metrics.PathsFound.Inc()
		gn.metrics.HopsPerQuery.Observe(float64(path.Hops))
	}

	return path, nil
}

func (gn *GraphNavigator) findPathInternal(ctx context.Context, startID, targetID uint32, maxHops int) (*NavigatorPath, error) {
	type queueItem struct {
		id   uint32
		hops int
		path []uint32
	}

	queue := []queueItem{{id: startID, hops: 0, path: []uint32{startID}}}
	visited := make(map[uint32]bool)

	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		current := queue[0]
		queue = queue[1:]

		if current.id == targetID {
			return &NavigatorPath{
				StartID: startID,
				EndID:   targetID,
				Path:    current.path,
				Hops:    current.hops,
				Found:   true,
			}, nil
		}

		if current.hops >= maxHops {
			continue
		}

		if gn.searchConfig.EarlyTerminate && gn.shouldTerminateEarly(current.id, targetID, current.hops) {
			gn.metrics.EarlyTerminations.Inc()
			continue
		}

		if visited[current.id] {
			continue
		}
		visited[current.id] = true

		neighbors, ok := gn.getNeighbors(current.id)
		if !ok {
			continue
		}

		for _, neighbor := range neighbors {
			if !visited[neighbor] {
				newPath := make([]uint32, len(current.path)+1)
				copy(newPath, current.path)
				newPath[len(current.path)] = neighbor

				queue = append(queue, queueItem{
					id:   neighbor,
					hops: current.hops + 1,
					path: newPath,
				})
			}
		}
	}

	return &NavigatorPath{
		StartID: startID,
		EndID:   targetID,
		Path:    []uint32{},
		Hops:    0,
		Found:   false,
	}, nil
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

	var sum float32
	for i := 0; i < len(v1); i++ {
		diff := v1[i] - v2[i]
		sum += diff * diff
	}

	return float32(math.Sqrt(float64(sum)))
}

func (gn *GraphNavigator) GetMetrics() *NavigatorMetrics {
	return gn.metrics
}

func (gn *GraphNavigator) IsInitialized() bool {
	return gn.isInitialized.Load()
}
