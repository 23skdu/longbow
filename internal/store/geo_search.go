package store

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"time"

	"github.com/23skdu/longbow/internal/metrics"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

type GeoPoint = lbtypes.GeoPoint

type GeoBoundingBox = lbtypes.GeoBoundingBox

type GeoPolygon []GeoPoint

type GeoDistanceType string

const (
	GeoDistanceHaversine   GeoDistanceType = "haversine"
	GeoDistanceEuclidean   GeoDistanceType = "euclidean"
	GeoDistanceApproximate GeoDistanceType = "approximate"
)

type GeoSearchConfig struct {
	DistanceType GeoDistanceType
	EarthRadius  float64
	IndexType    string
}

type GeoIndexedVector struct {
	ID        uint64
	Vector    []float32
	GeoPoint  GeoPoint
	Timestamp int64
	Metadata  map[string]interface{}
}

type GeoIndex struct {
	mu           sync.RWMutex
	dimension    int
	vectors      map[uint64]*GeoIndexedVector
	pointIndex   *Quadtree
	nearestCache map[uint64][]lbtypes.SearchResult
	config       *GeoSearchConfig
	datasetName  string // For metrics
}

type Quadtree struct {
	mu          sync.RWMutex
	bounds      GeoBoundingBox
	capacity    int
	vectors     []*GeoIndexedVector
	divided     bool
	northwest   *Quadtree
	northeast   *Quadtree
	southwest   *Quadtree
	southeast   *Quadtree
	datasetName string
}

func NewQuadtree(bounds GeoBoundingBox, capacity int, datasetName string) *Quadtree {
	return &Quadtree{
		bounds:      bounds,
		capacity:    capacity,
		datasetName: datasetName,
	}
}

func (q *Quadtree) Insert(vec *GeoIndexedVector) bool {
	if !q.Contains(vec.GeoPoint) {
		return false
	}

	if len(q.vectors) < q.capacity && !q.divided {
		q.vectors = append(q.vectors, vec)
		return true
	}

	if !q.divided {
		q.subdivide()
	}

	return q.northwest.Insert(vec) || q.northeast.Insert(vec) ||
		q.southwest.Insert(vec) || q.southeast.Insert(vec)
}

func (q *Quadtree) Contains(point GeoPoint) bool {
	return point.Lat >= q.bounds.MinLat && point.Lat <= q.bounds.MaxLat &&
		point.Lon >= q.bounds.MinLon && point.Lon <= q.bounds.MaxLon
}

func (q *Quadtree) subdivide() {
	metrics.QuadtreeSubdivisionsTotal.WithLabelValues(q.datasetName).Inc()
	midLat := (q.bounds.MinLat + q.bounds.MaxLat) / 2
	midLon := (q.bounds.MinLon + q.bounds.MaxLon) / 2

	q.northwest = &Quadtree{
		bounds:      GeoBoundingBox{MinLat: midLat, MaxLat: q.bounds.MaxLat, MinLon: q.bounds.MinLon, MaxLon: midLon},
		capacity:    q.capacity,
		datasetName: q.datasetName,
	}
	q.northeast = &Quadtree{
		bounds:      GeoBoundingBox{MinLat: midLat, MaxLat: q.bounds.MaxLat, MinLon: midLon, MaxLon: q.bounds.MaxLon},
		capacity:    q.capacity,
		datasetName: q.datasetName,
	}
	q.southwest = &Quadtree{
		bounds:      GeoBoundingBox{MinLat: q.bounds.MinLat, MaxLat: midLat, MinLon: q.bounds.MinLon, MaxLon: midLon},
		capacity:    q.capacity,
		datasetName: q.datasetName,
	}
	q.southeast = &Quadtree{
		bounds:      GeoBoundingBox{MinLat: q.bounds.MinLat, MaxLat: midLat, MinLon: midLon, MaxLon: q.bounds.MaxLon},
		capacity:    q.capacity,
		datasetName: q.datasetName,
	}

	for _, v := range q.vectors {
		q.northwest.Insert(v)
		q.northeast.Insert(v)
		q.southwest.Insert(v)
		q.southeast.Insert(v)
	}
	q.vectors = nil
	q.divided = true
}

func (q *Quadtree) QueryRadius(center GeoPoint, radiusKm float64) []*GeoIndexedVector {
	results := q.queryRadiusRecursive(center, radiusKm)
	return results
}

func (q *Quadtree) queryRadiusRecursive(center GeoPoint, radiusKm float64) []*GeoIndexedVector {
	var results []*GeoIndexedVector

	box := GeoBoundingBox{
		MinLat: center.Lat - radiusKm/111.0,
		MaxLat: center.Lat + radiusKm/111.0,
		MinLon: center.Lon - radiusKm/(111.0*math.Cos(center.Lat*math.Pi/180)),
		MaxLon: center.Lon + radiusKm/(111.0*math.Cos(center.Lat*math.Pi/180)),
	}

	if !q.intersects(box) {
		return results
	}

	if !q.divided {
		for _, v := range q.vectors {
			dist := HaversineDistance(center, v.GeoPoint, 6371.0)
			if dist <= radiusKm {
				results = append(results, v)
			}
		}
		return results
	}

	results = append(results, q.northwest.queryRadiusRecursive(center, radiusKm)...)
	results = append(results, q.northeast.queryRadiusRecursive(center, radiusKm)...)
	results = append(results, q.southwest.queryRadiusRecursive(center, radiusKm)...)
	results = append(results, q.southeast.queryRadiusRecursive(center, radiusKm)...)

	return results
}

func (q *Quadtree) intersects(box GeoBoundingBox) bool {
	return !(box.MaxLat < q.bounds.MinLat || box.MinLat > q.bounds.MaxLat ||
		box.MaxLon < q.bounds.MinLon || box.MinLon > q.bounds.MaxLon)
}

func (q *Quadtree) QueryBox(box GeoBoundingBox) []*GeoIndexedVector {
	var results []*GeoIndexedVector

	if !q.intersects(box) {
		return results
	}

	if !q.divided {
		for _, v := range q.vectors {
			if v.GeoPoint.Lat >= box.MinLat && v.GeoPoint.Lat <= box.MaxLat &&
				v.GeoPoint.Lon >= box.MinLon && v.GeoPoint.Lon <= box.MaxLon {
				results = append(results, v)
			}
		}
		return results
	}

	results = append(results, q.northwest.QueryBox(box)...)
	results = append(results, q.northeast.QueryBox(box)...)
	results = append(results, q.southwest.QueryBox(box)...)
	results = append(results, q.southeast.QueryBox(box)...)

	return results
}

func NewGeoIndex(datasetName string, dimension int, config *GeoSearchConfig) *GeoIndex {
	if config == nil {
		config = &GeoSearchConfig{
			DistanceType: GeoDistanceHaversine,
			EarthRadius:  6371.0,
			IndexType:    "quadtree",
		}
	}

	return &GeoIndex{
		datasetName:  datasetName,
		dimension:    dimension,
		vectors:      make(map[uint64]*GeoIndexedVector),
		pointIndex:   NewQuadtree(GeoBoundingBox{MinLat: -90, MaxLat: 90, MinLon: -180, MaxLon: 180}, 4, datasetName),
		nearestCache: make(map[uint64][]lbtypes.SearchResult),
		config:       config,
	}
}

func (gi *GeoIndex) Add(id uint64, vector []float32, point GeoPoint, metadata map[string]interface{}) error {
	gi.mu.Lock()
	defer gi.mu.Unlock()

	geoVec := &GeoIndexedVector{
		ID:        id,
		Vector:    vector,
		GeoPoint:  point,
		Timestamp: 0,
		Metadata:  metadata,
	}

	gi.vectors[id] = geoVec
	gi.pointIndex.Insert(geoVec)
	metrics.GeoIndexPointsTotal.WithLabelValues(gi.datasetName).Set(float64(len(gi.vectors)))

	for k := range gi.nearestCache {
		delete(gi.nearestCache, k)
	}

	return nil
}

func (gi *GeoIndex) SearchRadius(ctx context.Context, center GeoPoint, radiusKm float64, k int) ([]lbtypes.SearchResult, error) {
	start := time.Now()
	defer func() {
		metrics.GeoSearchOpsTotal.WithLabelValues(gi.datasetName, "radius").Inc()
		metrics.GeoSearchDurationSeconds.WithLabelValues(gi.datasetName, "radius").Observe(time.Since(start).Seconds())
	}()

	gi.mu.RLock()
	defer gi.mu.RUnlock()

	candidates := gi.pointIndex.QueryRadius(center, radiusKm)

	if len(candidates) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	type scoredResult struct {
		id       uint64
		distance float64
		vector   []float32
	}

	results := make([]scoredResult, 0, len(candidates))
	for _, c := range candidates {
		var dist float64
		switch gi.config.DistanceType {
		case GeoDistanceHaversine:
			dist = HaversineDistance(center, c.GeoPoint, gi.config.EarthRadius)
		case GeoDistanceEuclidean:
			dist = EuclideanDistanceGeo(center, c.GeoPoint)
		default:
			dist = HaversineDistance(center, c.GeoPoint, gi.config.EarthRadius)
		}
		results = append(results, scoredResult{id: c.ID, distance: dist, vector: c.Vector})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	searchResults := make([]lbtypes.SearchResult, 0, min(k, len(results)))
	for i := 0; i < min(k, len(results)); i++ {
		searchResults = append(searchResults, lbtypes.SearchResult{
			ID:       lbtypes.VectorID(results[i].id), // #nosec G115
			Distance: float32(results[i].distance),
			Score:    float32(1.0 / (1.0 + results[i].distance)),
		})
	}

	return searchResults, nil
}

func (gi *GeoIndex) SearchBox(ctx context.Context, box GeoBoundingBox, k int) ([]lbtypes.SearchResult, error) {
	start := time.Now()
	defer func() {
		metrics.GeoSearchOpsTotal.WithLabelValues(gi.datasetName, "box").Inc()
		metrics.GeoSearchDurationSeconds.WithLabelValues(gi.datasetName, "box").Observe(time.Since(start).Seconds())
	}()

	gi.mu.RLock()
	defer gi.mu.RUnlock()

	candidates := gi.pointIndex.QueryBox(box)

	searchResults := make([]lbtypes.SearchResult, 0, min(k, len(candidates)))
	for i := 0; i < min(k, len(candidates)); i++ {
		c := candidates[i]
		searchResults = append(searchResults, lbtypes.SearchResult{
			ID:       lbtypes.VectorID(c.ID), // #nosec G115
			Distance: 0,
			Score:    1.0,
		})
	}

	return searchResults, nil
}

func (gi *GeoIndex) HybridSearch(ctx context.Context, queryVector []float32, center GeoPoint, radiusKm float64, k int) ([]lbtypes.SearchResult, error) {
	start := time.Now()
	defer func() {
		metrics.GeoSearchOpsTotal.WithLabelValues(gi.datasetName, "hybrid").Inc()
		metrics.GeoSearchDurationSeconds.WithLabelValues(gi.datasetName, "hybrid").Observe(time.Since(start).Seconds())
	}()

	gi.mu.RLock()
	defer gi.mu.RUnlock()

	candidates := gi.pointIndex.QueryRadius(center, radiusKm)

	type scoredResult struct {
		id       uint64
		distance float64
		geoScore float64
		vector   []float32
	}

	results := make([]scoredResult, 0, len(candidates))
	for _, c := range candidates {
		geoDist := HaversineDistance(center, c.GeoPoint, gi.config.EarthRadius)
		geoScore := 1.0 / (1.0 + geoDist)

		vectorDist := VectorDistance(queryVector, c.Vector)
		vectorScore := 1.0 / (1.0 + vectorDist)

		combinedScore := 0.5*geoScore + 0.5*vectorScore

		results = append(results, scoredResult{
			id:       c.ID,
			distance: combinedScore,
			vector:   c.Vector,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance > results[j].distance
	})

	searchResults := make([]lbtypes.SearchResult, 0, min(k, len(results)))
	for i := 0; i < min(k, len(results)); i++ {
		searchResults = append(searchResults, lbtypes.SearchResult{
			ID:       lbtypes.VectorID(results[i].id), // #nosec G115
			Distance: float32(results[i].distance),
			Score:    float32(results[i].distance),
		})
	}

	return searchResults, nil
}

func (gi *GeoIndex) Size() int {
	gi.mu.RLock()
	defer gi.mu.RUnlock()
	return len(gi.vectors)
}

func (gi *GeoIndex) Get(id uint64) (*GeoIndexedVector, bool) {
	gi.mu.RLock()
	defer gi.mu.RUnlock()
	v, ok := gi.vectors[id]
	return v, ok
}

func (gi *GeoIndex) Delete(id uint64) {
	gi.mu.Lock()
	defer gi.mu.Unlock()
	delete(gi.vectors, id)
	for k := range gi.nearestCache {
		delete(gi.nearestCache, k)
	}
}

func HaversineDistance(p1, p2 GeoPoint, earthRadius float64) float64 {
	lat1 := p1.Lat * math.Pi / 180
	lat2 := p2.Lat * math.Pi / 180
	dLat := (p2.Lat - p1.Lat) * math.Pi / 180
	dLon := (p2.Lon - p1.Lon) * math.Pi / 180

	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadius * c
}

func EuclideanDistanceGeo(p1, p2 GeoPoint) float64 {
	dLat := p2.Lat - p1.Lat
	dLon := p2.Lon - p1.Lon
	return math.Sqrt(dLat*dLat + dLon*dLon)
}

type GeoSearchRequest = lbtypes.GeoSearchRequest

func ValidateGeoSearchRequest(req *GeoSearchRequest) error {
	if req.K <= 0 {
		req.K = 10
	}

	switch req.SearchType {
	case "radius":
		if req.RadiusKm <= 0 {
			return fmt.Errorf("radius_km must be positive for radius search")
		}
	case "box":
		if req.Box == nil {
			return fmt.Errorf("box must be specified for box search")
		}
	case "hybrid":
		if req.RadiusKm <= 0 {
			return fmt.Errorf("radius_km must be positive for hybrid search")
		}
	default:
		req.SearchType = "radius"
		req.RadiusKm = 10
	}

	return nil
}


func VectorDistance(v1, v2 []float32) float64 {
	if len(v1) != len(v2) {
		return -1
	}
	var sum float64
	for i := range v1 {
		d := float64(v1[i] - v2[i])
		sum += d * d
	}
	return math.Sqrt(sum)
}
