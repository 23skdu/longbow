package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHaversineDistance(t *testing.T) {
	p1 := GeoPoint{Lat: 40.7128, Lon: -74.0060}
	p2 := GeoPoint{Lat: 34.0522, Lon: -118.2437}

	dist := HaversineDistance(p1, p2, 6371.0)

	assert.Greater(t, dist, 3900000.0)
	assert.Less(t, dist, 4000000.0)
}

func TestEuclideanDistanceGeo(t *testing.T) {
	p1 := GeoPoint{Lat: 40.7128, Lon: -74.0060}
	p2 := GeoPoint{Lat: 34.0522, Lon: -118.2437}

	dist := EuclideanDistanceGeo(p1, p2)

	assert.Greater(t, dist, 0.0)
}

func TestQuadtree_Insert(t *testing.T) {
	bounds := GeoBoundingBox{
		MinLat: -90,
		MaxLat: 90,
		MinLon: -180,
		MaxLon: 180,
	}
	q := NewQuadtree(bounds, 4)

	vec := &GeoIndexedVector{
		ID:       1,
		Vector:   []float32{1.0, 2.0},
		GeoPoint: GeoPoint{Lat: 40.7128, Lon: -74.0060},
	}

	inserted := q.Insert(vec)
	assert.True(t, inserted)
}

func TestQuadtree_Contains(t *testing.T) {
	bounds := GeoBoundingBox{
		MinLat: 40.0,
		MaxLat: 41.0,
		MinLon: -75.0,
		MaxLon: -74.0,
	}
	q := NewQuadtree(bounds, 4)

	inside := GeoPoint{Lat: 40.5, Lon: -74.5}
	outside := GeoPoint{Lat: 35.0, Lon: -80.0}

	assert.True(t, q.Contains(inside))
	assert.False(t, q.Contains(outside))
}

func TestGeoIndex_New(t *testing.T) {
	config := &GeoSearchConfig{
		DistanceType: GeoDistanceHaversine,
		EarthRadius:  6371.0,
	}

	idx := NewGeoIndex(128, config)

	assert.NotNil(t, idx)
	assert.Equal(t, 128, idx.dimension)
	assert.NotNil(t, idx.pointIndex)
}

func TestGeoIndex_AddAndSearchRadius(t *testing.T) {
	config := &GeoSearchConfig{
		DistanceType: GeoDistanceHaversine,
		EarthRadius:  6371.0,
	}

	idx := NewGeoIndex(128, config)

	vectors := []struct {
		id  uint64
		vec []float32
		lat float64
		lon float64
	}{
		{1, []float32{1.0, 2.0}, 40.7128, -74.0060},
		{2, []float32{1.1, 2.1}, 34.0522, -118.2437},
		{3, []float32{1.2, 2.2}, 51.5074, -0.1278},
	}

	for _, v := range vectors {
		err := idx.Add(v.id, v.vec, GeoPoint{Lat: v.lat, Lon: v.lon}, nil)
		assert.NoError(t, err)
	}

	results, err := idx.SearchRadius(context.Background(), GeoPoint{Lat: 40.7128, Lon: -74.0060}, 5000, 2)
	assert.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestGeoIndex_SearchBox(t *testing.T) {
	config := &GeoSearchConfig{
		DistanceType: GeoDistanceHaversine,
		EarthRadius:  6371.0,
	}

	idx := NewGeoIndex(128, config)

	idx.Add(1, []float32{1.0, 2.0}, GeoPoint{Lat: 40.7128, Lon: -74.0060}, nil)
	idx.Add(2, []float32{1.1, 2.1}, GeoPoint{Lat: 34.0522, Lon: -118.2437}, nil)
	idx.Add(3, []float32{1.2, 2.2}, GeoPoint{Lat: 51.5074, Lon: -0.1278}, nil)

	box := GeoBoundingBox{
		MinLat: 30.0,
		MaxLat: 50.0,
		MinLon: -130.0,
		MaxLon: -50.0,
	}

	results, err := idx.SearchBox(context.Background(), box, 10)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(results), 1)
}

func TestGeoSearchRequest_Validate(t *testing.T) {
	req := &GeoSearchRequest{
		Center:     GeoPoint{Lat: 40.7128, Lon: -74.0060},
		RadiusKm:   100,
		K:          10,
		SearchType: "radius",
	}

	err := req.Validate()
	assert.NoError(t, err)
}

func TestGeoSearchRequest_ValidateDefault(t *testing.T) {
	req := &GeoSearchRequest{
		SearchType: "unknown",
	}

	err := req.Validate()
	assert.NoError(t, err)
	assert.Equal(t, "radius", req.SearchType)
}

func TestGeoSearchRequest_ValidateErrors(t *testing.T) {
	req := &GeoSearchRequest{
		SearchType: "radius",
		RadiusKm:   0,
	}

	err := req.Validate()
	assert.Error(t, err)
}

func TestGeoPoint_JSON(t *testing.T) {
	p := GeoPoint{
		Lat:  40.7128,
		Lon:  -74.0060,
		Name: "NYC",
	}

	lat := p.Lat
	lon := p.Lon
	name := p.Name

	assert.Equal(t, 40.7128, lat)
	assert.Equal(t, -74.0060, lon)
	assert.Equal(t, "NYC", name)
}

func TestGeoBoundingBox_Validation(t *testing.T) {
	box := GeoBoundingBox{
		MinLat: 40.0,
		MaxLat: 41.0,
		MinLon: -75.0,
		MaxLon: -74.0,
	}

	assert.Equal(t, 40.0, box.MinLat)
	assert.Equal(t, 41.0, box.MaxLat)
}

func TestQuadtree_QueryRadius(t *testing.T) {
	bounds := GeoBoundingBox{
		MinLat: -90,
		MaxLat: 90,
		MinLon: -180,
		MaxLon: 180,
	}
	q := NewQuadtree(bounds, 4)

	q.Insert(&GeoIndexedVector{ID: 1, GeoPoint: GeoPoint{Lat: 40.7128, Lon: -74.0060}})
	q.Insert(&GeoIndexedVector{ID: 2, GeoPoint: GeoPoint{Lat: 34.0522, Lon: -118.2437}})

	center := GeoPoint{Lat: 40.7128, Lon: -74.0060}
	results := q.QueryRadius(center, 1000)

	assert.NotEmpty(t, results)
}

func TestQuadtree_QueryBox(t *testing.T) {
	bounds := GeoBoundingBox{
		MinLat: -90,
		MaxLat: 90,
		MinLon: -180,
		MaxLon: 180,
	}
	q := NewQuadtree(bounds, 4)

	q.Insert(&GeoIndexedVector{ID: 1, GeoPoint: GeoPoint{Lat: 40.7128, Lon: -74.0060}})
	q.Insert(&GeoIndexedVector{ID: 2, GeoPoint: GeoPoint{Lat: 34.0522, Lon: -118.2437}})

	box := GeoBoundingBox{
		MinLat: 30.0,
		MaxLat: 50.0,
		MinLon: -130.0,
		MaxLon: -50.0,
	}

	results := q.QueryBox(box)

	assert.GreaterOrEqual(t, len(results), 1)
}
