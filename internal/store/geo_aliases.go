package store

import "github.com/23skdu/longbow/internal/store/geo"

type GeoPoint = geo.GeoPoint
type GeoBoundingBox = geo.GeoBoundingBox
type GeoPolygon = geo.GeoPolygon
type GeoDistanceType = geo.GeoDistanceType
type GeoSearchConfig = geo.GeoSearchConfig
type GeoPointIndex = geo.GeoPointIndex
type GeoIndexedVector = geo.GeoIndexedVector
type GeoIndex = geo.GeoIndex
type GeoPredicate = geo.GeoPredicate
type Quadtree = geo.Quadtree
type MortonGrid = geo.MortonGrid
type GeoSearchRequest = geo.GeoSearchRequest

const (
	GeoDistanceHaversine   = geo.GeoDistanceHaversine
	GeoDistanceEuclidean   = geo.GeoDistanceEuclidean
	GeoDistanceApproximate = geo.GeoDistanceApproximate
	GeoIndexTypeMorton     = geo.GeoIndexTypeMorton
	GeoIndexTypeQuadtree   = geo.GeoIndexTypeQuadtree
)

func NewGeoIndex(datasetName string, dimension int, config *GeoSearchConfig) *GeoIndex {
	return geo.NewGeoIndex(datasetName, dimension, config)
}

func NewQuadtree(bounds GeoBoundingBox, capacity int, datasetName string) *Quadtree {
	return geo.NewQuadtree(bounds, capacity, datasetName)
}

func NewMortonGrid(bounds GeoBoundingBox, expectedPoints int, datasetName string) *MortonGrid {
	return geo.NewMortonGrid(bounds, expectedPoints, datasetName)
}

func NewMortonGridWithResolution(bounds GeoBoundingBox, bits, expectedPoints int, datasetName string) *MortonGrid {
	return geo.NewMortonGridWithResolution(bounds, bits, expectedPoints, datasetName)
}

func BoundingBox(p GeoPoint, radiusKm float64) GeoBoundingBox {
	return geo.BoundingBox(p, radiusKm)
}

func HaversineDistance(p1, p2 GeoPoint, earthRadius float64) float64 {
	return geo.HaversineDistance(p1, p2, earthRadius)
}

func EuclideanDistanceGeo(p1, p2 GeoPoint) float64 {
	return geo.EuclideanDistanceGeo(p1, p2)
}

func VectorDistance(v1, v2 []float32) float64 {
	return geo.VectorDistance(v1, v2)
}

func ValidateGeoSearchRequest(req *GeoSearchRequest) error {
	return geo.ValidateGeoSearchRequest(req)
}
