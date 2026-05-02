package store

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	internalcore "github.com/23skdu/longbow/internal/store/internal/core"
	lbcore "github.com/23skdu/longbow/internal/core"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

// GeoPoint represents a geographic location with latitude and longitude.
type GeoPoint = lbcore.GeoPoint

// BoundingBox calculates a bounding box around a point with the given radius.
func BoundingBox(p GeoPoint, radiusKm float64) GeoBoundingBox {
	latDelta := radiusKm / 111.0
	lonDelta := radiusKm / (111.0 * math.Cos(p.Lat*math.Pi/180.0))

	return GeoBoundingBox{
		MinLat: p.Lat - latDelta,
		MaxLat: p.Lat + latDelta,
		MinLon: p.Lon - lonDelta,
		MaxLon: p.Lon + lonDelta,
	}
}

// GeoBoundingBox defines a rectangular geographic area.
type GeoBoundingBox = lbtypes.GeoBoundingBox

// GeoPolygon represents a sequence of GeoPoints forming a closed area.
type GeoPolygon []GeoPoint

// GeoDistanceType defines the method used for distance calculations.
type GeoDistanceType string

const (
	// GeoDistanceHaversine uses the spherical law of cosines for great-circle distance.
	GeoDistanceHaversine   GeoDistanceType = "haversine"
	// GeoDistanceEuclidean uses planar distance (suitable for small areas).
	GeoDistanceEuclidean   GeoDistanceType = "euclidean"
	// GeoDistanceApproximate uses a faster, less accurate distance calculation.
	GeoDistanceApproximate GeoDistanceType = "approximate"
)

// GeoSearchConfig holds configuration for geospatial indexing and search.
type GeoSearchConfig struct {
	DistanceType GeoDistanceType
	EarthRadius  float64
	IndexType    string
}

// GeoIndexedVector pairs a vector with its geographic location.
type GeoIndexedVector struct {
	ID        uint64
	Vector    []float32
	GeoPoint  GeoPoint
	Timestamp int64
	Metadata  []byte
}

// GeoIndex provides efficient spatial indexing for vectors.
type GeoIndex struct {
	mu           sync.Mutex
	dimension    int
	vectors      sync.Map
	pointIndex   atomic.Pointer[Quadtree]
	nearestCache sync.Map
	config       *GeoSearchConfig
	datasetName  string // For metrics
	pointCount   atomic.Int64
	gpuIndex     atomic.Value // holds gputypes.Index
}

// Quadtree implements a recursive spatial partitioning structure.
type Quadtree struct {
	bounds      GeoBoundingBox
	capacity    int
	vectors     []*GeoIndexedVector
	divided     bool
	northwest   *Quadtree
	northeast   *Quadtree
	southwest   *Quadtree
	southeast   *Quadtree
	datasetName string
	depth       int
}

// NewQuadtree creates a new Quadtree instance with the given bounds and capacity.
func NewQuadtree(bounds GeoBoundingBox, capacity int, datasetName string) *Quadtree {
	return &Quadtree{
		bounds:      bounds,
		capacity:    capacity,
		datasetName: datasetName,
		depth:       0,
	}
}

// Insert adds a vector to the quadtree, subdividing if necessary.
func (q *Quadtree) Insert(vec *GeoIndexedVector) bool {
	if !q.Contains(vec.GeoPoint) {
		return false
	}

	if len(q.vectors) < q.capacity && !q.divided {
		q.vectors = append(q.vectors, vec)
		return true
	}

	if !q.divided {
		if q.depth >= 20 {
			q.vectors = append(q.vectors, vec)
			return true
		}
		q.subdivide()
	}

	return q.northwest.Insert(vec) || q.northeast.Insert(vec) ||
		q.southwest.Insert(vec) || q.southeast.Insert(vec)
}

// Contains checks if a GeoPoint is within the quadtree bounds.
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
		depth:       q.depth + 1,
	}
	q.northeast = &Quadtree{
		bounds:      GeoBoundingBox{MinLat: midLat, MaxLat: q.bounds.MaxLat, MinLon: midLon, MaxLon: q.bounds.MaxLon},
		capacity:    q.capacity,
		datasetName: q.datasetName,
		depth:       q.depth + 1,
	}
	q.southwest = &Quadtree{
		bounds:      GeoBoundingBox{MinLat: q.bounds.MinLat, MaxLat: midLat, MinLon: q.bounds.MinLon, MaxLon: midLon},
		capacity:    q.capacity,
		datasetName: q.datasetName,
		depth:       q.depth + 1,
	}
	q.southeast = &Quadtree{
		bounds:      GeoBoundingBox{MinLat: q.bounds.MinLat, MaxLat: midLat, MinLon: midLon, MaxLon: q.bounds.MaxLon},
		capacity:    q.capacity,
		datasetName: q.datasetName,
		depth:       q.depth + 1,
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

// QueryRadius returns all vectors within a given radius from a point.
func (q *Quadtree) QueryRadius(center GeoPoint, radiusKm float64) []*GeoIndexedVector {
	var results []*GeoIndexedVector
	q.queryRadiusRecursive(center, radiusKm, &results)
	return results
}

func (q *Quadtree) queryRadiusRecursive(center GeoPoint, radiusKm float64, results *[]*GeoIndexedVector) {
	box := GeoBoundingBox{
		MinLat: center.Lat - radiusKm/111.0,
		MaxLat: center.Lat + radiusKm/111.0,
		MinLon: center.Lon - radiusKm/(111.0*math.Cos(center.Lat*math.Pi/180)),
		MaxLon: center.Lon + radiusKm/(111.0*math.Cos(center.Lat*math.Pi/180)),
	}

	if !q.intersects(box) {
		return
	}

	if !q.divided {
		for _, v := range q.vectors {
			dist := HaversineDistance(center, v.GeoPoint, 6371.0)
			if dist <= radiusKm {
				*results = append(*results, v)
			}
		}
		return
	}

	q.northwest.queryRadiusRecursive(center, radiusKm, results)
	q.northeast.queryRadiusRecursive(center, radiusKm, results)
	q.southwest.queryRadiusRecursive(center, radiusKm, results)
	q.southeast.queryRadiusRecursive(center, radiusKm, results)
}

func (q *Quadtree) intersects(box GeoBoundingBox) bool {
	return !(box.MaxLat < q.bounds.MinLat || box.MinLat > q.bounds.MaxLat ||
		box.MaxLon < q.bounds.MinLon || box.MinLon > q.bounds.MaxLon)
}

// QueryBox returns all vectors within a bounding box.
func (q *Quadtree) QueryBox(box GeoBoundingBox) []*GeoIndexedVector {
	var results []*GeoIndexedVector
	q.queryBoxRecursive(box, &results)
	return results
}

func (q *Quadtree) queryBoxRecursive(box GeoBoundingBox, results *[]*GeoIndexedVector) {
	if !q.intersects(box) {
		return
	}

	if !q.divided {
		for _, v := range q.vectors {
			if v.GeoPoint.Lat >= box.MinLat && v.GeoPoint.Lat <= box.MaxLat &&
				v.GeoPoint.Lon >= box.MinLon && v.GeoPoint.Lon <= box.MaxLon {
				*results = append(*results, v)
			}
		}
		return
	}

	q.northwest.queryBoxRecursive(box, results)
	q.northeast.queryBoxRecursive(box, results)
	q.southwest.queryBoxRecursive(box, results)
	q.southeast.queryBoxRecursive(box, results)
}

// NewGeoIndex creates a new GeoIndex with the specified configuration.
func NewGeoIndex(datasetName string, dimension int, config *GeoSearchConfig) *GeoIndex {
	if config == nil {
		config = &GeoSearchConfig{
			DistanceType: GeoDistanceHaversine,
			EarthRadius:  6371.0,
			IndexType:    "quadtree",
		}
	}

	gi := &GeoIndex{
		datasetName: datasetName,
		dimension:   dimension,
		config:      config,
	}
	gi.pointIndex.Store(NewQuadtree(GeoBoundingBox{MinLat: -90, MaxLat: 90, MinLon: -180, MaxLon: 180}, 4, datasetName))
	return gi
}

// SetGPUIndex sets the GPU acceleration index for this GeoIndex.
func (gi *GeoIndex) SetGPUIndex(idx gputypes.Index) {
	gi.gpuIndex.Store(idx)
}

// Add inserts a vector and its location into the index.
func (gi *GeoIndex) Add(id uint64, vector []float32, point GeoPoint, metadata []byte) error {
	gi.mu.Lock()
	defer gi.mu.Unlock()

	geoVec := &GeoIndexedVector{
		ID:        id,
		Vector:    vector,
		GeoPoint:  point,
		Timestamp: 0,
		Metadata:  metadata,
	}

	gi.vectors.Store(id, geoVec)
	index := gi.pointIndex.Load()
	if index != nil {
		index.Insert(geoVec)
	}
	gi.pointCount.Add(1)

	// Reset nearestCache by creating a new sync.Map
	gi.nearestCache = sync.Map{}

	return nil
}

// AddBatch inserts multiple vectors into the GeoIndex.
func (gi *GeoIndex) AddBatch(ids []uint64, vectors [][]float32, points []GeoPoint, metadata [][]byte) error {
	gi.mu.Lock()
	defer gi.mu.Unlock()

	index := gi.pointIndex.Load()
	for i := range ids {
		var m []byte
		if i < len(metadata) {
			m = metadata[i]
		}
		geoVec := &GeoIndexedVector{
			ID:        ids[i],
			Vector:    vectors[i],
			GeoPoint:  points[i],
			Timestamp: 0,
			Metadata:  m,
		}

		gi.vectors.Store(ids[i], geoVec)
		if index != nil {
			index.Insert(geoVec)
		}
	}
	gi.pointCount.Add(int64(len(ids)))

	// Reset nearestCache
	gi.nearestCache = sync.Map{}

	return nil
}

// SearchRadius finds the k-nearest neighbors within a radius.
func (gi *GeoIndex) SearchRadius(ctx context.Context, center GeoPoint, radiusKm float64, k int) ([]lbtypes.SearchResult, error) {
	start := time.Now()
	defer func() {
		metrics.GeoSearchOpsTotal.WithLabelValues(gi.datasetName, "radius").Inc()
		metrics.GeoSearchDurationSeconds.WithLabelValues(gi.datasetName, "radius").Observe(time.Since(start).Seconds())
	}()

	index := gi.pointIndex.Load()
	if index == nil {
		return []lbtypes.SearchResult{}, nil
	}

	// Get candidate bounding box
	box := BoundingBox(center, radiusKm)
	candidates := index.QueryBox(box)

	if len(candidates) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	type scoredResult struct {
		id       uint64
		distance float64
		vector   []float32
	}

	results := make([]scoredResult, len(candidates))
	pool := internalcore.GetSharedPool()

	if gi.config.DistanceType == GeoDistanceHaversine {
		distances := make([]float32, len(candidates))
		pts := make([]lbcore.GeoPoint, len(candidates))

		// Parallel point preparation
		pool.ParallelFor(len(candidates), 2048, func(start, end int) {
			for i := start; i < end; i++ {
				pts[i] = candidates[i].GeoPoint
			}
		})

		// Use GPU if available
		var gpuIdx gputypes.Index
		if val := gi.gpuIndex.Load(); val != nil {
			gpuIdx = val.(gputypes.Index)
		}

		if gpuIdx != nil {
			// GPU acceleration path
			pointsF32 := make([]float32, len(candidates)*2)
			pool.ParallelFor(len(candidates), 2048, func(start, end int) {
				for i := start; i < end; i++ {
					pointsF32[i*2] = float32(candidates[i].GeoPoint.Lat)
					pointsF32[i*2+1] = float32(candidates[i].GeoPoint.Lon)
				}
			})
			gpuRes, err := gpuIdx.HaversineSearch(float32(center.Lat), float32(center.Lon), pointsF32, float32(gi.config.EarthRadius))
			if err == nil {
				distances = gpuRes
			} else {
				// Fallback to Parallel CPU
				pool.ParallelFor(len(candidates), 1024, func(start, end int) {
					simd.HaversineBatch(center.Lat, center.Lon, pts[start:end], gi.config.EarthRadius, distances[start:end])
				})
			}
		} else {
			// CPU parallel path
			pool.ParallelFor(len(candidates), 1024, func(start, end int) {
				simd.HaversineBatch(center.Lat, center.Lon, pts[start:end], gi.config.EarthRadius, distances[start:end])
			})
		}

		pool.ParallelFor(len(candidates), 2048, func(start, end int) {
			for i := start; i < end; i++ {
				c := candidates[i]
				results[i] = scoredResult{id: c.ID, distance: float64(distances[i]), vector: c.Vector}
			}
		})
	} else {
		pool.ParallelFor(len(candidates), 1024, func(start, end int) {
			for i := start; i < end; i++ {
				c := candidates[i]
				var dist float64
				switch gi.config.DistanceType {
				case GeoDistanceEuclidean:
					dist = EuclideanDistanceGeo(center, c.GeoPoint)
				default:
					dist = HaversineDistance(center, c.GeoPoint, gi.config.EarthRadius)
				}
				results[i] = scoredResult{id: c.ID, distance: dist, vector: c.Vector}
			}
		})
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

// SearchBox finds all vectors within a rectangular area.
func (gi *GeoIndex) SearchBox(ctx context.Context, box GeoBoundingBox, k int) ([]lbtypes.SearchResult, error) {
	start := time.Now()
	defer func() {
		metrics.GeoSearchOpsTotal.WithLabelValues(gi.datasetName, "box").Inc()
		metrics.GeoSearchDurationSeconds.WithLabelValues(gi.datasetName, "box").Observe(time.Since(start).Seconds())
	}()

	index := gi.pointIndex.Load()
	if index == nil {
		return []lbtypes.SearchResult{}, nil
	}

	candidates := index.QueryBox(box)

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

// HybridSearch combines vector similarity and geographic proximity.
func (gi *GeoIndex) HybridSearch(ctx context.Context, queryVector []float32, center GeoPoint, radiusKm float64, k int) ([]lbtypes.SearchResult, error) {
	start := time.Now()
	defer func() {
		metrics.GeoSearchOpsTotal.WithLabelValues(gi.datasetName, "hybrid").Inc()
		metrics.GeoSearchDurationSeconds.WithLabelValues(gi.datasetName, "hybrid").Observe(time.Since(start).Seconds())
	}()

	index := gi.pointIndex.Load()
	if index == nil {
		return []lbtypes.SearchResult{}, nil
	}

	candidates := index.QueryBox(BoundingBox(center, radiusKm))
	if len(candidates) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	type scoredResult struct {
		id       uint64
		distance float64
		geoScore float64
		vector   []float32
	}

	// Batch compute geo distances
	geoDistances := make([]float32, len(candidates))
	points := make([]float32, len(candidates)*2)
	pool := internalcore.GetSharedPool()

	pool.ParallelFor(len(candidates), 2048, func(start, end int) {
		for i := start; i < end; i++ {
			points[i*2] = float32(candidates[i].GeoPoint.Lat)
			points[i*2+1] = float32(candidates[i].GeoPoint.Lon)
		}
	})

	var gpuIdx gputypes.Index
	if val := gi.gpuIndex.Load(); val != nil {
		gpuIdx = val.(gputypes.Index)
	}

	if gpuIdx != nil {
		gpuRes, err := gpuIdx.HaversineSearch(float32(center.Lat), float32(center.Lon), points, float32(gi.config.EarthRadius))
		if err == nil {
			geoDistances = gpuRes
		} else {
			pts := make([]lbcore.GeoPoint, len(candidates))
			pool.ParallelFor(len(candidates), 2048, func(start, end int) {
				for i := start; i < end; i++ {
					pts[i] = candidates[i].GeoPoint
				}
			})
			pool.ParallelFor(len(candidates), 1024, func(start, end int) {
				simd.HaversineBatch(center.Lat, center.Lon, pts[start:end], gi.config.EarthRadius, geoDistances[start:end])
			})
		}
	} else {
		pts := make([]lbcore.GeoPoint, len(candidates))
		pool.ParallelFor(len(candidates), 2048, func(start, end int) {
			for i := start; i < end; i++ {
				pts[i] = candidates[i].GeoPoint
			}
		})
		pool.ParallelFor(len(candidates), 1024, func(start, end int) {
			simd.HaversineBatch(center.Lat, center.Lon, pts[start:end], gi.config.EarthRadius, geoDistances[start:end])
		})
	}

	results := make([]scoredResult, len(candidates))
	pool = internalcore.GetSharedPool()

	pool.ParallelFor(len(candidates), 512, func(start, end int) {
		for i := start; i < end; i++ {
			c := candidates[i]
			geoDist := float64(geoDistances[i])
			geoScore := 1.0 / (1.0 + geoDist)

			vectorDist := VectorDistance(queryVector, c.Vector)
			vectorScore := 1.0 / (1.0 + vectorDist)

			combinedScore := 0.5*geoScore + 0.5*vectorScore

			results[i] = scoredResult{
				id:       c.ID,
				distance: combinedScore,
				vector:   c.Vector,
			}
		}
	})

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance > results[j].distance
	})

	searchResults := make([]lbtypes.SearchResult, 0, min(k, len(results)))
	for i := 0; i < min(k, len(results)); i++ {
		searchResults = append(searchResults, lbtypes.SearchResult{
			ID:       lbtypes.VectorID(results[i].id), // #nosec G115
			Distance: float32(results[i].distance),
			Score:    float32(1.0 / (1.0 + results[i].distance)), // Fixed score calculation
		})
	}

	return searchResults, nil
}

// Size returns the total number of vectors in the geo index.
func (gi *GeoIndex) Size() int {
	return int(gi.pointCount.Load())
}

// Get retrieves a vector by its ID.
func (gi *GeoIndex) Get(id uint64) (*GeoIndexedVector, bool) {
	val, ok := gi.vectors.Load(id)
	if !ok {
		return nil, false
	}
	return val.(*GeoIndexedVector), true
}

// Delete removes a vector from the geo index.
func (gi *GeoIndex) Delete(id uint64) {
	gi.mu.Lock()
	defer gi.mu.Unlock()
	
	if _, ok := gi.vectors.Load(id); ok {
		gi.vectors.Delete(id)
		gi.pointCount.Add(-1)
	}
	// Note: We don't remove from pointIndex (Quadtree) for performance.
	// It will be filtered out during Search if not in gi.vectors or marked.
	// But current Quadtree doesn't support easy deletion.
	
	gi.nearestCache = sync.Map{}
}

// HaversineDistance calculates the great-circle distance between two points.
func HaversineDistance(p1, p2 GeoPoint, earthRadius float64) float64 {
	lat1 := p1.Lat * math.Pi / 180
	lat2 := p2.Lat * math.Pi / 180
	dLat := (p2.Lat - p1.Lat) * math.Pi / 180
	dLon := (p2.Lon - p1.Lon) * math.Pi / 180

	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadius * c
}

// EuclideanDistanceGeo calculates the planar distance between two points.
func EuclideanDistanceGeo(p1, p2 GeoPoint) float64 {
	dLat := p2.Lat - p1.Lat
	dLon := p2.Lon - p1.Lon
	return math.Sqrt(dLat*dLat + dLon*dLon)
}

// GeoSearchRequest encapsulates parameters for various geographic search types.
type GeoSearchRequest = lbtypes.GeoSearchRequest

// ValidateGeoSearchRequest ensures a GeoSearchRequest is well-formed.
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


// VectorDistance calculates the Euclidean distance between two vectors.
func VectorDistance(v1, v2 []float32) float64 {
	d, err := simd.EuclideanDistance(v1, v2)
	if err != nil {
		return -1
	}
	return float64(d)
}
