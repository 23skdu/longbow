package store

import (
	"container/heap"
	"math"
	"strconv"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

const (
	// defaultGridCellSize is the edge length in degrees of one spatial hash
	// grid cell. P1-4b: 0.1 degree cells cut the number of false-positive
	// candidates of a radius query compared to the original 0.5 degree cells.
	defaultGridCellSize = 0.1

	// kmPerDegree is the mean length of one degree of latitude. The geo code
	// uses the same flat-earth conversion for every bounding box so that
	// boxes stay consistent with each other.
	kmPerDegree = 111.0

	// maxSpatialGridCellsPerQuery bounds the number of cells a single radius
	// query may visit. Near a pole the longitude span of the search box can
	// cover the whole globe (or blow up when the box crosses the pole), which
	// would otherwise make the grid scan far more expensive than the point
	// index fallback.
	maxSpatialGridCellsPerQuery = 1 << 14

	// degToRad converts degrees to radians.
	degToRad = math.Pi / 180
)

// geoGridCell holds every point of one spatial hash grid cell in a
// structure-of-arrays layout.
//
// Latitude and longitude are kept in contiguous float32 slices so a radius
// scan walks sequential memory instead of dereferencing one heap object per
// candidate. The parallel vec slice keeps the owning vector so the few
// candidates that need an exact distance do not need a second lookup.
//
// The float32 coordinates are only used to rank candidates; the exact
// Haversine distance of a retained hit is always recomputed from the
// float64 coordinates of the vector.
type geoGridCell struct {
	lats []float32
	lons []float32
	vecs []*GeoIndexedVector
}

// len returns the number of points in the cell.
func (c *geoGridCell) len() int { return len(c.lats) }

// addToSpatialGrid inserts a vector into the spatial hash grid.
func (gi *GeoIndex) addToSpatialGrid(vec *GeoIndexedVector) {
	if gi.gridCellSize <= 0 {
		gi.gridCellSize = defaultGridCellSize
	}
	gi.gridMu.Lock()
	if gi.spatialGrid == nil {
		gi.spatialGrid = make(map[string]*geoGridCell)
	}
	key := gi.gridKey(vec.GeoPoint.Lat, vec.GeoPoint.Lon)
	cell := gi.spatialGrid[key]
	if cell == nil {
		cell = &geoGridCell{}
		gi.spatialGrid[key] = cell
	}
	cell.lats = append(cell.lats, float32(vec.GeoPoint.Lat))
	cell.lons = append(cell.lons, float32(vec.GeoPoint.Lon))
	cell.vecs = append(cell.vecs, vec)
	gi.gridMu.Unlock()
}

// gridKey returns the spatial hash grid key of a coordinate.
func (gi *GeoIndex) gridKey(lat, lon float64) string {
	cs := gi.gridCellSize
	if cs <= 0 {
		cs = defaultGridCellSize
	}
	latCell := int(math.Floor(lat / cs))
	lonCell := int(math.Floor(lon / cs))
	return strconv.Itoa(latCell) + ":" + strconv.Itoa(lonCell)
}

// geoSearchBox returns the flat-earth bounding box that fully contains the
// circle of radiusKm around center.
//
// The longitude extent uses the cosine of the latitude furthest from the
// equator within the latitude band, so the box never clips the circle near
// the poles. Using the cosine of the centre latitude instead would shrink
// the box and silently drop hits on the north/south side of the query.
func geoSearchBox(center GeoPoint, radiusKm float64) GeoBoundingBox {
	latDelta := radiusKm / kmPerDegree
	cosLat := math.Cos(center.Lat * degToRad)
	if math.Abs(cosLat) < 1e-12 {
		cosLat = 1e-12
	}
	// Latitude furthest from the equator within [center.Lat-latDelta,
	// center.Lat+latDelta]; its cosine is the smallest of the band.
	edgeLat := math.Max(math.Abs(center.Lat-latDelta), math.Abs(center.Lat+latDelta))
	cosEdge := math.Cos(edgeLat * degToRad)
	if math.Abs(cosEdge) < 1e-12 {
		cosEdge = 1e-12
	}
	lonDelta := radiusKm / (kmPerDegree * cosEdge)

	return GeoBoundingBox{
		MinLat: center.Lat - latDelta,
		MaxLat: center.Lat + latDelta,
		MinLon: center.Lon - lonDelta,
		MaxLon: center.Lon + lonDelta,
	}
}

// spatialGridCells returns the grid cells that can hold a point within
// radiusKm of center.
//
// P1-2: cells whose bounding box lies entirely outside the search circle are
// skipped (annulus pruning), so only cells that can actually contain a hit
// are scanned.
func (gi *GeoIndex) spatialGridCells(center GeoPoint, radiusKm float64) []*geoGridCell {
	cs := gi.gridCellSize
	if cs <= 0 {
		cs = defaultGridCellSize
	}
	box := geoSearchBox(center, radiusKm)

	minLatCell := int(math.Floor(box.MinLat / cs))
	maxLatCell := int(math.Floor(box.MaxLat / cs))
	minLonCell := int(math.Floor(box.MinLon / cs))
	maxLonCell := int(math.Floor(box.MaxLon / cs))

	// A box that crosses a pole (or a degenerate cosine) has an inverted or
	// huge longitude span. Hand those queries to the point index instead of
	// iterating a pathological number of cells.
	latCells := maxLatCell - minLatCell + 1
	lonCells := maxLonCell - minLonCell + 1
	if latCells <= 0 || lonCells <= 0 || latCells > maxSpatialGridCellsPerQuery/lonCells {
		return nil
	}

	gi.gridMu.RLock()
	defer gi.gridMu.RUnlock()
	if len(gi.spatialGrid) == 0 {
		return nil
	}

	cells := make([]*geoGridCell, 0, min(latCells*lonCells, 64))
	for latCell := minLatCell; latCell <= maxLatCell; latCell++ {
		for lonCell := minLonCell; lonCell <= maxLonCell; lonCell++ {
			cellBox := GeoBoundingBox{
				MinLat: float64(latCell) * cs,
				MaxLat: float64(latCell+1) * cs,
				MinLon: float64(lonCell) * cs,
				MaxLon: float64(lonCell+1) * cs,
			}
			// Annulus pruning: skip cells entirely outside the radius.
			if !boxIntersectsCircleR(cellBox, center, radiusKm, 6371.0) {
				continue
			}
			cell, ok := gi.spatialGrid[strconv.Itoa(latCell)+":"+strconv.Itoa(lonCell)]
			if !ok || cell.len() == 0 {
				continue
			}
			cells = append(cells, cell)
		}
	}
	return cells
}

// geoRadiusHit is a candidate retained by a radius scan.
type geoRadiusHit struct {
	id   uint64
	dist float64
}

// geoRadiusHeap keeps the best hits seen by a radius scan. It is a max-heap
// ordered by distance, so the root always holds the worst retained hit and
// can be used directly as a pruning threshold.
type geoRadiusHeap struct {
	items []geoRadiusHit
}

func (h *geoRadiusHeap) Len() int           { return len(h.items) }
func (h *geoRadiusHeap) Less(i, j int) bool { return h.items[i].dist > h.items[j].dist }
func (h *geoRadiusHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *geoRadiusHeap) Push(x any)         { h.items = append(h.items, x.(geoRadiusHit)) }
func (h *geoRadiusHeap) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	return x
}

// searchRadiusGrid finds the k nearest points inside radiusKm of center
// using the spatial hash grid.
//
// The scan is a single pass over the cells overlapping the search circle.
// Every candidate is scored with a cheap proxy whose square is a lower bound
// on the exact distance (see geoRadiusScan). That bound lets the scan drop a
// candidate without computing an exact distance as soon as the bound exceeds
// either the search radius or the distance of the current k-th best hit, so
// only the handful of surviving candidates pay for a full distance
// evaluation.
//
// It reports false when the grid holds no candidate for the query, so the
// caller can fall back to the point index.
func (gi *GeoIndex) searchRadiusGrid(center GeoPoint, radiusKm float64, k int) ([]lbtypes.SearchResult, bool) {
	cells := gi.spatialGridCells(center, radiusKm)
	if len(cells) == 0 {
		return nil, false
	}

	earthRadius := gi.config.EarthRadius
	if earthRadius <= 0 {
		earthRadius = 6371.0
	}

	scan, ok := geoRadiusScanFor(center, radiusKm, earthRadius, gi.config.DistanceType)
	if !ok {
		return nil, false
	}
	radius2 := radiusKm * radiusKm

	h := &geoRadiusHeap{}
	limit := k
	if limit < 0 {
		limit = 0
	}
	// Worst retained exact distance, used as the pruning threshold.
	worst := math.Inf(1)

	centerLat := center.Lat
	centerLon := center.Lon

	for _, cell := range cells {
		lats := cell.lats
		lons := cell.lons
		vecs := cell.vecs
		for i := range lats {
			dLat := float64(lats[i]) - centerLat
			dLon := float64(lons[i]) - centerLon

			// Squared lower bound on the exact distance.
			lb2 := scan.phiCoef*dLat*dLat + scan.lonCoef*dLon*dLon

			// Definitely outside the search radius.
			if lb2 > radius2 {
				continue
			}
			// Definitely not among the k best hits seen so far.
			if h.Len() == limit && lb2 > worst*worst+scan.slack {
				continue
			}

			vec := vecs[i]
			var dist float64
			if scan.euclidean {
				dist = EuclideanDistanceGeo(center, vec.GeoPoint)
			} else {
				dist = HaversineDistance(center, vec.GeoPoint, earthRadius)
			}
			if dist > radiusKm {
				continue
			}
			if limit == 0 {
				h.items = append(h.items, geoRadiusHit{id: vec.ID, dist: dist})
				continue
			}
			if h.Len() < limit {
				heap.Push(h, geoRadiusHit{id: vec.ID, dist: dist})
				worst = h.items[0].dist
				continue
			}
			if dist < worst {
				h.items[0] = geoRadiusHit{id: vec.ID, dist: dist}
				heap.Fix(h, 0)
				worst = h.items[0].dist
			}
		}
	}

	return geoRadiusResults(h), true
}

// geoRadiusResults converts the retained hits into search results, best hit
// first.
func geoRadiusResults(h *geoRadiusHeap) []lbtypes.SearchResult {
	if h.Len() == 0 {
		return []lbtypes.SearchResult{}
	}
	results := make([]lbtypes.SearchResult, h.Len())
	// The heap pops the worst hit first, so filling from the back keeps the
	// best hit at the front, matching the ordering of the other search modes.
	for i := h.Len() - 1; i >= 0; i-- {
		hit := heap.Pop(h).(geoRadiusHit)
		results[i] = lbtypes.SearchResult{
			ID:       lbtypes.VectorID(hit.id), // #nosec G115
			Distance: float32(hit.dist),
			Score:    float32(1.0 / (1.0 + hit.dist)),
		}
	}
	return results
}

// geoRadiusScan holds the per-query constants that turn the degree deltas of
// a candidate into the squared distance bound used to prune candidates.
type geoRadiusScan struct {
	// phiCoef and lonCoef are the coefficients of the squared latitude and
	// longitude deltas.
	phiCoef float64
	lonCoef float64
	// euclidean selects the planar degree distance instead of the
	// great-circle distance.
	euclidean bool
	// slack absorbs the rounding error of the float32 cell coordinates.
	slack float64
}

// geoRadiusScanFor computes the pruning constants of a radius query.
//
// For two points separated by dPhi (latitude) and dLam (longitude) in
// radians with mid-latitude phiM, the haversine formula gives
//
//	a = sin^2(dPhi/2)cos^2(dLam/2) + cos^2(phiM)sin^2(dLam/2)
//
// and the great-circle distance is 2R*asin(sqrt(a)). Using sin x >= x-x^3/6
// and cos x >= 1-x^2/2 one obtains the lower bound
//
//	d >= dEq * sqrt(1 - dPhi^2/12 - dLam^2/4)
//
// where dEq is the equirectangular distance computed with cos^2(phiM). Both
// cos^2(phiM) and the sqrt factor are bounded over the whole search box, so
// a single pair of constants is valid for every candidate of the query.
//
// It reports false when the bound degenerates (a search box so large that
// no useful lower bound exists), in which case the caller must fall back to
// an exhaustive scan.
func geoRadiusScanFor(center GeoPoint, radiusKm, earthRadius float64, distType GeoDistanceType) (geoRadiusScan, bool) {
	if distType == GeoDistanceEuclidean {
		// The planar degree distance is computed exactly, so the proxy is
		// the distance itself; only the float32 rounding needs a slack.
		return geoRadiusScan{phiCoef: 1, lonCoef: 1, euclidean: true, slack: 1e-6}, true
	}

	box := geoSearchBox(center, radiusKm)

	latSpan := (box.MaxLat - box.MinLat) * degToRad
	lonSpan := (box.MaxLon - box.MinLon) * degToRad

	// The sqrt factor of the bound, minimised over the search box.
	kappa2 := 1 - latSpan*latSpan/12 - lonSpan*lonSpan/4
	if kappa2 <= 0 {
		return geoRadiusScan{}, false
	}

	// Smallest cos^2 of the mid-latitude over the search box. The
	// mid-latitude lies between the centre and the candidate, so it stays
	// inside the latitude band of the box.
	edgeLat := math.Max(math.Abs(box.MinLat), math.Abs(box.MaxLat))
	cosEdge := math.Cos(edgeLat * degToRad)
	c2min := cosEdge * cosEdge

	// The bound is expressed in square kilometres while the candidate
	// deltas are in degrees.
	factor := 4 * earthRadius * earthRadius * kappa2 * degToRad * degToRad * 0.25
	return geoRadiusScan{
		phiCoef: factor,
		lonCoef: factor * c2min,
	}, true
}

// boxIntersectsCircleR reports whether a bounding box intersects a circle of
// the given radius around center, using earthRadius for the great-circle
// distance.
func boxIntersectsCircleR(box GeoBoundingBox, center GeoPoint, radiusKm, earthRadius float64) bool {
	lat := center.Lat
	if lat < box.MinLat {
		lat = box.MinLat
	} else if lat > box.MaxLat {
		lat = box.MaxLat
	}
	lon := center.Lon
	if lon < box.MinLon {
		lon = box.MinLon
	} else if lon > box.MaxLon {
		lon = box.MaxLon
	}
	return HaversineDistance(center, GeoPoint{Lat: lat, Lon: lon}, earthRadius) <= radiusKm
}
