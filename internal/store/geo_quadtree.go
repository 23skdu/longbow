package store

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/RoaringBitmap/roaring/v2"
)

// Quadtree implements a spatial partitioning structure using iterative stack traversal.
type Quadtree struct {
	bounds      GeoBoundingBox
	capacity    int
	vectors     []*GeoIndexedVector
	divided     atomic.Bool
	northwest   *Quadtree
	northeast   *Quadtree
	southwest   *Quadtree
	southeast   *Quadtree
	datasetName string
	depth       int
	mu          sync.RWMutex
}

// NewQuadtree creates a new Quadtree instance with the given bounds and capacity.
func NewQuadtree(bounds GeoBoundingBox, capacity int, datasetName string) *Quadtree {
	if capacity <= 0 {
		capacity = 64 // Increased default capacity for better scaling
	}
	return &Quadtree{
		bounds:      bounds,
		capacity:    capacity,
		datasetName: datasetName,
		depth:       0,
	}
}

// Insert adds a geographic vector to the quadtree.
func (q *Quadtree) Insert(vec *GeoIndexedVector) bool {
	if !q.Contains(vec.GeoPoint) {
		return false
	}

	q.mu.Lock()
	if !q.divided.Load() {
		if len(q.vectors) < q.capacity || q.depth >= 24 {
			q.vectors = append(q.vectors, vec)
			q.mu.Unlock()
			return true
		}
		q.subdivide()
	}
	q.mu.Unlock()

	// Try children - no lock needed here as children are immutable once published
	if q.northwest.Insert(vec) {
		return true
	}
	if q.northeast.Insert(vec) {
		return true
	}
	if q.southwest.Insert(vec) {
		return true
	}
	return q.southeast.Insert(vec)
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

	// Pre-create children
	nw := &Quadtree{
		bounds:      GeoBoundingBox{MinLat: midLat, MaxLat: q.bounds.MaxLat, MinLon: q.bounds.MinLon, MaxLon: midLon},
		capacity:    q.capacity,
		datasetName: q.datasetName,
		depth:       q.depth + 1,
	}
	ne := &Quadtree{
		bounds:      GeoBoundingBox{MinLat: midLat, MaxLat: q.bounds.MaxLat, MinLon: midLon, MaxLon: q.bounds.MaxLon},
		capacity:    q.capacity,
		datasetName: q.datasetName,
		depth:       q.depth + 1,
	}
	sw := &Quadtree{
		bounds:      GeoBoundingBox{MinLat: q.bounds.MinLat, MaxLat: midLat, MinLon: q.bounds.MinLon, MaxLon: midLon},
		capacity:    q.capacity,
		datasetName: q.datasetName,
		depth:       q.depth + 1,
	}
	se := &Quadtree{
		bounds:      GeoBoundingBox{MinLat: q.bounds.MinLat, MaxLat: midLat, MinLon: midLon, MaxLon: q.bounds.MaxLon},
		capacity:    q.capacity,
		datasetName: q.datasetName,
		depth:       q.depth + 1,
	}

	// Direct quadrant assignment for existing vectors
	for _, v := range q.vectors {
		if v.GeoPoint.Lat >= midLat {
			if v.GeoPoint.Lon < midLon {
				nw.vectors = append(nw.vectors, v)
			} else {
				ne.vectors = append(ne.vectors, v)
			}
		} else {
			if v.GeoPoint.Lon < midLon {
				sw.vectors = append(sw.vectors, v)
			} else {
				se.vectors = append(se.vectors, v)
			}
		}
	}

	// Atomic publication of children to ensure Search consistency
	q.northwest = nw
	q.northeast = ne
	q.southwest = sw
	q.southeast = se

	// Finalize subdivision flag
	q.divided.Store(true)

	// Clear local vectors only after children are visible
	q.vectors = nil
}

func (q *Quadtree) intersects(box GeoBoundingBox) bool {
	return !(box.MaxLat < q.bounds.MinLat || box.MinLat > q.bounds.MaxLat ||
		box.MaxLon < q.bounds.MinLon || box.MinLon > q.bounds.MaxLon)
}

// QueryRadius returns all vectors within a given radius from a point.
func (q *Quadtree) QueryRadius(center GeoPoint, radiusKm float64, results *[]*GeoIndexedVector) {
	cosLat := math.Cos(center.Lat * math.Pi / 180)
	lonDelta := radiusKm / (111.0 * cosLat)
	box := GeoBoundingBox{
		MinLat: center.Lat - radiusKm/111.0,
		MaxLat: center.Lat + radiusKm/111.0,
		MinLon: center.Lon - lonDelta,
		MaxLon: center.Lon + lonDelta,
	}
	q.queryRadiusIterative(box, results)
}

func (q *Quadtree) queryRadiusIterative(box GeoBoundingBox, results *[]*GeoIndexedVector) {
	if !q.intersects(box) {
		return
	}

	var stackBuf [128]*Quadtree
	stack := stackBuf[:0]
	stack = append(stack, q)

	for len(stack) > 0 {
		idx := len(stack) - 1
		curr := stack[idx]
		stack = stack[:idx]

		if !curr.intersects(box) {
			continue
		}

		curr.mu.RLock()
		if !curr.divided.Load() {
			*results = append(*results, curr.vectors...)
			curr.mu.RUnlock()
			continue
		}
		curr.mu.RUnlock()

		// Push in reverse order so northwest is popped first
		if curr.southeast != nil {
			stack = append(stack, curr.southeast)
		}
		if curr.southwest != nil {
			stack = append(stack, curr.southwest)
		}
		if curr.northeast != nil {
			stack = append(stack, curr.northeast)
		}
		if curr.northwest != nil {
			stack = append(stack, curr.northwest)
		}
	}
}

// QueryBox returns all vectors within a bounding box.
func (q *Quadtree) QueryBox(box GeoBoundingBox) []*GeoIndexedVector {
	results := make([]*GeoIndexedVector, 0, 128)
	q.queryBoxIterative(box, &results)
	return results
}

// QueryBoxAppend appends all vectors within box to the target results slice without extra allocations.
func (q *Quadtree) QueryBoxAppend(box GeoBoundingBox, results *[]*GeoIndexedVector) {
	q.queryBoxIterative(box, results)
}

func (q *Quadtree) queryBoxIterative(box GeoBoundingBox, results *[]*GeoIndexedVector) {
	if !q.intersects(box) {
		return
	}

	var stackBuf [128]*Quadtree
	stack := stackBuf[:0]
	stack = append(stack, q)

	for len(stack) > 0 {
		idx := len(stack) - 1
		curr := stack[idx]
		stack = stack[:idx]

		if !curr.intersects(box) {
			continue
		}

		curr.mu.RLock()
		if !curr.divided.Load() {
			for _, v := range curr.vectors {
				if v.GeoPoint.Lat >= box.MinLat && v.GeoPoint.Lat <= box.MaxLat &&
					v.GeoPoint.Lon >= box.MinLon && v.GeoPoint.Lon <= box.MaxLon {
					*results = append(*results, v)
				}
			}
			curr.mu.RUnlock()
			continue
		}
		curr.mu.RUnlock()

		// Push in reverse order so northwest is popped first
		if curr.southeast != nil {
			stack = append(stack, curr.southeast)
		}
		if curr.southwest != nil {
			stack = append(stack, curr.southwest)
		}
		if curr.northeast != nil {
			stack = append(stack, curr.northeast)
		}
		if curr.northwest != nil {
			stack = append(stack, curr.northwest)
		}
	}
}

// QueryBoxBitmap pre-computes a Roaring Bitmap of valid geospatial candidates via Quadtree bounding boxes.
func (q *Quadtree) QueryBoxBitmap(box GeoBoundingBox, bm *roaring.Bitmap) {
	if !q.intersects(box) {
		return
	}

	var stackBuf [128]*Quadtree
	stack := stackBuf[:0]
	stack = append(stack, q)

	for len(stack) > 0 {
		idx := len(stack) - 1
		curr := stack[idx]
		stack = stack[:idx]

		if !curr.intersects(box) {
			continue
		}

		curr.mu.RLock()
		if !curr.divided.Load() {
			for _, v := range curr.vectors {
				if v.GeoPoint.Lat >= box.MinLat && v.GeoPoint.Lat <= box.MaxLat &&
					v.GeoPoint.Lon >= box.MinLon && v.GeoPoint.Lon <= box.MaxLon {
					bm.Add(uint32(v.ID)) // #nosec G115
				}
			}
			curr.mu.RUnlock()
			continue
		}
		curr.mu.RUnlock()

		// Push in reverse order so northwest is popped first
		if curr.southeast != nil {
			stack = append(stack, curr.southeast)
		}
		if curr.southwest != nil {
			stack = append(stack, curr.southwest)
		}
		if curr.northeast != nil {
			stack = append(stack, curr.northeast)
		}
		if curr.northwest != nil {
			stack = append(stack, curr.northwest)
		}
	}
}
