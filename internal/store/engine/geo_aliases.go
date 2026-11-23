package engine

import "github.com/23skdu/longbow/internal/store/geo"

// GeoPoint represents a geo point value.
type GeoPoint = geo.GeoPoint

// GeoBoundingBox represents a geo bounding box value.
type GeoBoundingBox = geo.GeoBoundingBox

// GeoPolygon represents a geo polygon value.
type GeoPolygon = geo.GeoPolygon

// GeoDistanceType represents a geo distance type value.
type GeoDistanceType = geo.GeoDistanceType

// GeoSearchConfig represents a geo search config value.
type GeoSearchConfig = geo.GeoSearchConfig

// GeoPointIndex represents a geo point index value.
type GeoPointIndex = geo.GeoPointIndex

// GeoIndexedVector represents a geo indexed vector value.
type GeoIndexedVector = geo.GeoIndexedVector

// GeoIndex represents a geo index value.
type GeoIndex = geo.GeoIndex

// GeoPredicate represents a geo predicate value.
type GeoPredicate = geo.GeoPredicate

// Quadtree represents a quadtree value.
type Quadtree = geo.Quadtree

// MortonGrid represents a morton grid value.
type MortonGrid = geo.MortonGrid

// GeoSearchRequest represents a geo search request value.
type GeoSearchRequest = geo.GeoSearchRequest

const (
	// GeoDistanceHaversine is a geo distance haversine constant.
	GeoDistanceHaversine = geo.GeoDistanceHaversine
	// GeoDistanceEuclidean is a geo distance euclidean constant.
	GeoDistanceEuclidean = geo.GeoDistanceEuclidean
	// GeoDistanceApproximate is a geo distance approximate constant.
	GeoDistanceApproximate = geo.GeoDistanceApproximate
	// GeoIndexTypeMorton is a geo index type morton constant.
	GeoIndexTypeMorton = geo.GeoIndexTypeMorton
	// GeoIndexTypeQuadtree is a geo index type quadtree constant.
	GeoIndexTypeQuadtree = geo.GeoIndexTypeQuadtree
)

// NewGeoIndex constructs a new GeoIndex.
func NewGeoIndex(datasetName string, dimension int, config *GeoSearchConfig) *GeoIndex {
	return geo.NewGeoIndex(datasetName, dimension, config)
}

// NewQuadtree constructs a new Quadtree.
func NewQuadtree(bounds GeoBoundingBox, capacity int, datasetName string) *Quadtree {
	return geo.NewQuadtree(bounds, capacity, datasetName)
}

// NewMortonGrid constructs a new MortonGrid.
func NewMortonGrid(bounds GeoBoundingBox, expectedPoints int, datasetName string) *MortonGrid {
	return geo.NewMortonGrid(bounds, expectedPoints, datasetName)
}

// NewMortonGridWithResolution constructs a new MortonGridWithResolution.
func NewMortonGridWithResolution(bounds GeoBoundingBox, bits, expectedPoints int, datasetName string) *MortonGrid {
	return geo.NewMortonGridWithResolution(bounds, bits, expectedPoints, datasetName)
}

// BoundingBox runs the bounding box operation.
func BoundingBox(p GeoPoint, radiusKm float64) GeoBoundingBox {
	return geo.BoundingBox(p, radiusKm)
}

// HaversineDistance runs the haversine distance operation.
func HaversineDistance(p1, p2 GeoPoint, earthRadius float64) float64 {
	return geo.HaversineDistance(p1, p2, earthRadius)
}

// EuclideanDistanceGeo runs the euclidean distance geo operation.
func EuclideanDistanceGeo(p1, p2 GeoPoint) float64 {
	return geo.EuclideanDistanceGeo(p1, p2)
}

// VectorDistance runs the vector distance operation.
func VectorDistance(v1, v2 []float32) float64 {
	return geo.VectorDistance(v1, v2)
}

// ValidateGeoSearchRequest runs the validate geo search request operation.
func ValidateGeoSearchRequest(req *GeoSearchRequest) error {
	return geo.ValidateGeoSearchRequest(req)
}
