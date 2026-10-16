package store

import (
	"fmt"
	"math"

	"github.com/23skdu/longbow/internal/simd"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

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

// VectorDistance calculates the Euclidean distance between two vectors.
func VectorDistance(v1, v2 []float32) float64 {
	d, err := simd.EuclideanDistance(v1, v2)
	if err != nil {
		return -1
	}
	return float64(d)
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

// geoResultHeap implements heap.Interface for SearchResult (Max-Heap by Score).
type geoResultHeap []lbtypes.SearchResult

func (h geoResultHeap) Len() int           { return len(h) }
func (h geoResultHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h geoResultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *geoResultHeap) Push(x any) {
	*h = append(*h, x.(lbtypes.SearchResult))
}

func (h *geoResultHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
