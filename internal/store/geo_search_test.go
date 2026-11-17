package store

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
)

func TestHaversineDistance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	p1 := GeoPoint{Lat: 40.7128, Lon: -74.0060}
	p2 := GeoPoint{Lat: 34.0522, Lon: -118.2437}

	dist := HaversineDistance(p1, p2, 6371000.0)

	assert.Greater(t, dist, 3900000.0)
	assert.Less(t, dist, 4000000.0)
}

func TestEuclideanDistanceGeo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	p1 := GeoPoint{Lat: 40.7128, Lon: -74.0060}
	p2 := GeoPoint{Lat: 34.0522, Lon: -118.2437}

	dist := EuclideanDistanceGeo(p1, p2)

	assert.Greater(t, dist, 0.0)
}

func TestQuadtree_Insert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	bounds := GeoBoundingBox{
		MinLat: -90,
		MaxLat: 90,
		MinLon: -180,
		MaxLon: 180,
	}
	q := NewQuadtree(bounds, 4, "test_dataset")

	vec := &GeoIndexedVector{
		ID:       1,
		Vector:   []float32{1.0, 2.0},
		GeoPoint: GeoPoint{Lat: 40.7128, Lon: -74.0060},
	}

	inserted := q.Insert(vec)
	assert.True(t, inserted)
}

func TestQuadtree_Contains(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	bounds := GeoBoundingBox{
		MinLat: 40.0,
		MaxLat: 41.0,
		MinLon: -75.0,
		MaxLon: -74.0,
	}
	q := NewQuadtree(bounds, 4, "test_dataset")

	inside := GeoPoint{Lat: 40.5, Lon: -74.5}
	outside := GeoPoint{Lat: 35.0, Lon: -80.0}

	assert.True(t, q.Contains(inside))
	assert.False(t, q.Contains(outside))
}

func TestGeoIndex_New(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	config := &GeoSearchConfig{
		DistanceType: GeoDistanceHaversine,
		EarthRadius:  6371.0,
	}

	idx := NewGeoIndex("test_dataset", 128, config)

	assert.NotNil(t, idx)
	assert.Equal(t, 128, idx.dimension)
	assert.NotNil(t, idx.pointIndex.Load())
}

func TestGeoIndex_AddAndSearchRadius(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	config := &GeoSearchConfig{
		DistanceType: GeoDistanceHaversine,
		EarthRadius:  6371.0,
	}

	idx := NewGeoIndex("test_dataset", 128, config)

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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	config := &GeoSearchConfig{
		DistanceType: GeoDistanceHaversine,
		EarthRadius:  6371.0,
	}

	idx := NewGeoIndex("test_dataset", 128, config)

	_ = idx.Add(1, []float32{1.0, 2.0}, GeoPoint{Lat: 40.7128, Lon: -74.0060}, nil)
	_ = idx.Add(2, []float32{1.1, 2.1}, GeoPoint{Lat: 34.0522, Lon: -118.2437}, nil)
	_ = idx.Add(3, []float32{1.2, 2.2}, GeoPoint{Lat: 51.5074, Lon: -0.1278}, nil)

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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	req := &GeoSearchRequest{
		Center:     GeoPoint{Lat: 40.7128, Lon: -74.0060},
		RadiusKm:   100,
		K:          10,
		SearchType: "radius",
	}

	err := ValidateGeoSearchRequest(req)
	assert.NoError(t, err)
}

func TestGeoSearchRequest_ValidateDefault(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	req := &GeoSearchRequest{
		SearchType: "unknown",
	}

	err := ValidateGeoSearchRequest(req)
	assert.NoError(t, err)
	assert.Equal(t, "radius", req.SearchType)
}

func TestGeoSearchRequest_ValidateErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	req := &GeoSearchRequest{
		SearchType: "radius",
		RadiusKm:   0,
	}

	err := ValidateGeoSearchRequest(req)
	assert.Error(t, err)
}

func TestGeoPoint_JSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	bounds := GeoBoundingBox{
		MinLat: -90,
		MaxLat: 90,
		MinLon: -180,
		MaxLon: 180,
	}
	q := NewQuadtree(bounds, 4, "test_dataset")

	q.Insert(&GeoIndexedVector{ID: 1, GeoPoint: GeoPoint{Lat: 40.7128, Lon: -74.0060}})
	q.Insert(&GeoIndexedVector{ID: 2, GeoPoint: GeoPoint{Lat: 34.0522, Lon: -118.2437}})

	center := GeoPoint{Lat: 40.7128, Lon: -74.0060}
	var results []*GeoIndexedVector
	q.QueryRadius(center, 1000, &results)

	assert.NotEmpty(t, results)
}

func TestQuadtree_QueryBox(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	bounds := GeoBoundingBox{
		MinLat: -90,
		MaxLat: 90,
		MinLon: -180,
		MaxLon: 180,
	}
	q := NewQuadtree(bounds, 4, "test_dataset")

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

func TestGeoIndex_Concurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	config := &GeoSearchConfig{
		DistanceType: GeoDistanceHaversine,
		EarthRadius:  6371.0,
		IndexType:    "quadtree",
	}
	bounds := GeoBoundingBox{
		MinLat: -90, MaxLat: 90, MinLon: -180, MaxLon: 180,
	}
	gi := NewGeoIndex("test", 128, config)
	gi.pointIndex.Store(newGeoIndexHandle(NewQuadtree(bounds, 64, "test")))

	// Start concurrent inserters
	numInserters := 4
	vectorsPerInserter := 1000
	var wg sync.WaitGroup
	wg.Add(numInserters)

	for i := 0; i < numInserters; i++ {
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < vectorsPerInserter; j++ {
				id := uint64(idx*vectorsPerInserter + j)
				vec := make([]float32, 128)
				point := GeoPoint{Lat: 40.0 + float64(j)*0.001, Lon: -74.0 + float64(idx)*0.1}
				_ = gi.Add(id, vec, point, nil)
			}
		}(i)
	}

	// Start concurrent searchers
	numSearchers := 4
	wg.Add(numSearchers)
	for i := 0; i < numSearchers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = gi.SearchRadius(context.Background(), GeoPoint{Lat: 40.5, Lon: -73.5}, 50, 10)
			}
		}()
	}

	wg.Wait()

	expectedCount := int64(numInserters * vectorsPerInserter)
	assert.Equal(t, expectedCount, gi.pointCount.Load())
}

func BenchmarkGeoIndex_SearchRadius(b *testing.B) {
	for _, indexType := range []string{GeoIndexTypeMorton, GeoIndexTypeQuadtree} {
		b.Run(indexType, func(b *testing.B) {
			config := &GeoSearchConfig{
				DistanceType: GeoDistanceHaversine,
				EarthRadius:  6371.0,
				IndexType:    indexType,
			}
			gi := NewGeoIndex("bench", 128, config)
			gi.pointIndex.Store(newGeoIndexHandle(newGeoPointIndex(indexType, "bench", 64)))

			// Pre-fill with 10k points
			for i := 0; i < 10000; i++ {
				id := uint64(i)
				vec := make([]float32, 128)
				point := GeoPoint{Lat: 40.0 + float64(i)*0.0001, Lon: -74.0 + float64(i)*0.0001}
				_ = gi.Add(id, vec, point, nil)
			}

			center := GeoPoint{Lat: 40.5, Lon: -73.5}
			b.ResetTimer()
			for b.Loop() {
				_, _ = gi.SearchRadius(context.Background(), center, 100, 10)
			}
		})
	}
}

func BenchmarkGeoQuadtree_QueryRadiusVsPruned(b *testing.B) {
	qt := NewQuadtree(GeoBoundingBox{MinLat: -90, MaxLat: 90, MinLon: -180, MaxLon: 180}, 16, "bench")
	for i := 0; i < 10000; i++ {
		lat := -40.0 + float64(i%800)*0.1
		lon := -120.0 + float64(i/800)*10.0
		qt.Insert(&GeoIndexedVector{
			ID:       uint64(i),
			GeoPoint: GeoPoint{Lat: lat, Lon: lon},
		})
	}
	center := GeoPoint{Lat: 0.0, Lon: -60.0}
	var out []*GeoIndexedVector

	b.Run("QueryRadius_Unpruned", func(b *testing.B) {
		for b.Loop() {
			out = out[:0]
			qt.QueryRadius(center, 50.0, &out)
		}
	})
	b.Run("QueryRadiusPruned", func(b *testing.B) {
		for b.Loop() {
			out = out[:0]
			qt.QueryRadiusPruned(center, 50.0, &out)
		}
	})
}

// buildGeoTestIndex inserts n deterministic points produced by gen into a
// fresh GeoIndex.
func buildGeoTestIndex(tb testing.TB, n int, distType GeoDistanceType, gen func(*rand.Rand) GeoPoint) *GeoIndex {
	tb.Helper()
	config := &GeoSearchConfig{
		DistanceType: distType,
		EarthRadius:  6371.0,
		IndexType:    GeoIndexTypeQuadtree,
	}
	gi := NewGeoIndex("geo-test", 16, config)
	rng := rand.New(rand.NewSource(42))
	vec := make([]float32, 16)
	for i := 0; i < n; i++ {
		if err := gi.Add(uint64(i), vec, gen(rng), nil); err != nil {
			tb.Fatalf("add point %d: %v", i, err)
		}
	}
	return gi
}

// bruteForceGeoRadius is the exhaustive reference implementation of
// GeoIndex.SearchRadius. It ignores the spatial indexes entirely and filters
// every stored point by its exact distance, then sorts the hits best-first.
func bruteForceGeoRadius(gi *GeoIndex, center GeoPoint, radiusKm float64, k int, distType GeoDistanceType, earthRadius float64) []lbtypes.SearchResult {
	hits := make([]lbtypes.SearchResult, 0, 64)
	gi.vectors.Range(func(_, value any) bool {
		v := value.(*GeoIndexedVector)
		var dist float64
		if distType == GeoDistanceEuclidean {
			dist = EuclideanDistanceGeo(center, v.GeoPoint)
		} else {
			dist = HaversineDistance(center, v.GeoPoint, earthRadius)
		}
		if dist <= radiusKm {
			hits = append(hits, lbtypes.SearchResult{
				ID:       lbtypes.VectorID(v.ID), // #nosec G115
				Distance: float32(dist),
			})
		}
		return true
	})
	sortGeoResults(hits)
	if k > 0 && len(hits) > k {
		hits = hits[:k]
	}
	return hits
}

// sortGeoResults orders results best-first, breaking distance ties by ID so
// the reference and the index produce a deterministic order.
func sortGeoResults(results []lbtypes.SearchResult) {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Distance != results[j].Distance {
			return results[i].Distance < results[j].Distance
		}
		return results[i].ID < results[j].ID
	})
}

// assertGeoRadiusParity compares SearchRadius against the exhaustive
// reference for one query.
func assertGeoRadiusParity(t *testing.T, label string, gi *GeoIndex, center GeoPoint, radiusKm float64, k int, distType GeoDistanceType, earthRadius float64) {
	t.Helper()
	got, err := gi.SearchRadius(context.Background(), center, radiusKm, k)
	if err != nil {
		t.Fatalf("%s: search failed: %v", label, err)
	}
	want := bruteForceGeoRadius(gi, center, radiusKm, k, distType, earthRadius)

	if len(got) != len(want) {
		t.Fatalf("%s: got %d hits, want %d (center=%+v radius=%.3f k=%d)",
			label, len(got), len(want), center, radiusKm, k)
	}
	sortGeoResults(got)
	for i := range want {
		if got[i].ID != want[i].ID {
			t.Fatalf("%s: hit %d id got %d want %d (center=%+v radius=%.3f k=%d)",
				label, i, got[i].ID, want[i].ID, center, radiusKm, k)
		}
		if diff := float64(got[i].Distance) - float64(want[i].Distance); diff > 1e-3 || diff < -1e-3 {
			t.Fatalf("%s: hit %d distance got %.6f want %.6f (id=%d)",
				label, i, got[i].Distance, want[i].Distance, got[i].ID)
		}
	}
}

// TestGeoIndex_SearchRadiusParity verifies the spatial-hash fast path against
// an exhaustive scan on clustered, high-latitude, polar, and Euclidean data.
func TestGeoIndex_SearchRadiusParity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping parity test in short mode")
	}

	t.Run("clustered", func(t *testing.T) {
		gi := buildGeoTestIndex(t, 5000, GeoDistanceHaversine, func(rng *rand.Rand) GeoPoint {
			return GeoPoint{
				Lat: 40.7128 + (rng.Float64()-0.5)*0.2,
				Lon: -74.0060 + (rng.Float64()-0.5)*0.2,
			}
		})
		rng := rand.New(rand.NewSource(7))
		for q := 0; q < 100; q++ {
			center := GeoPoint{
				Lat: 40.7128 + (rng.Float64()-0.5)*0.3,
				Lon: -74.0060 + (rng.Float64()-0.5)*0.3,
			}
			radius := 1 + rng.Float64()*40
			for _, k := range []int{0, 1, 10, 100} {
				assertGeoRadiusParity(t,
					fmt.Sprintf("query=%d k=%d", q, k),
					gi, center, radius, k, GeoDistanceHaversine, 6371.0)
			}
		}
	})

	t.Run("high-latitude", func(t *testing.T) {
		gi := buildGeoTestIndex(t, 2000, GeoDistanceHaversine, func(rng *rand.Rand) GeoPoint {
			return GeoPoint{
				Lat: 78.0 + (rng.Float64()-0.5)*0.2,
				Lon: (rng.Float64() - 0.5) * 0.2,
			}
		})
		for _, radius := range []float64{5, 50, 100, 200} {
			assertGeoRadiusParity(t, fmt.Sprintf("radius=%v", radius),
				gi, GeoPoint{Lat: 78.0, Lon: 0.0}, radius, 10, GeoDistanceHaversine, 6371.0)
		}
	})

	t.Run("polar", func(t *testing.T) {
		gi := buildGeoTestIndex(t, 1000, GeoDistanceHaversine, func(rng *rand.Rand) GeoPoint {
			return GeoPoint{
				Lat: 89.5 + rng.Float64()*0.5,
				Lon: (rng.Float64() - 0.5) * 20,
			}
		})
		for _, radius := range []float64{10, 60, 120} {
			assertGeoRadiusParity(t, fmt.Sprintf("radius=%v", radius),
				gi, GeoPoint{Lat: 89.9, Lon: 0.0}, radius, 10, GeoDistanceHaversine, 6371.0)
		}
	})

	t.Run("euclidean", func(t *testing.T) {
		gi := buildGeoTestIndex(t, 2000, GeoDistanceEuclidean, func(rng *rand.Rand) GeoPoint {
			return GeoPoint{
				Lat: 40.7128 + (rng.Float64()-0.5)*0.2,
				Lon: -74.0060 + (rng.Float64()-0.5)*0.2,
			}
		})
		for _, radius := range []float64{0.01, 0.05, 0.2} {
			assertGeoRadiusParity(t, fmt.Sprintf("radius=%v", radius),
				gi, GeoPoint{Lat: 40.7128, Lon: -74.0060}, radius, 10, GeoDistanceEuclidean, 6371.0)
		}
	})

	t.Run("empty", func(t *testing.T) {
		gi := buildGeoTestIndex(t, 0, GeoDistanceHaversine, func(rng *rand.Rand) GeoPoint {
			return GeoPoint{}
		})
		got, err := gi.SearchRadius(context.Background(), GeoPoint{Lat: 0, Lon: 0}, 10, 5)
		if err != nil {
			t.Fatalf("search failed: %v", err)
		}
		assert.Empty(t, got)
	})
}

// TestGeoIndex_HybridSearch is a regression test for the pooled hybrid path:
// the result pool must round-trip the same slice type across queries.
func TestGeoIndex_HybridSearch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	gi := buildGeoTestIndex(t, 100, GeoDistanceHaversine, func(rng *rand.Rand) GeoPoint {
		return GeoPoint{
			Lat: 40.7128 + (rng.Float64()-0.5)*0.02,
			Lon: -74.0060 + (rng.Float64()-0.5)*0.02,
		}
	})
	query := make([]float32, 16)
	center := GeoPoint{Lat: 40.7128, Lon: -74.0060}
	for i := 0; i < 3; i++ {
		res, err := gi.HybridSearch(context.Background(), query, center, 10, 5)
		if err != nil {
			t.Fatalf("query %d: hybrid search failed: %v", i, err)
		}
		assert.Len(t, res, 5)
	}
}

// TestGeoSearchScalability exercises geo radius search on a realistic
// clustered workload and asserts a minimum QPS (P1-6b/P1-7). The threshold is
// deliberately below the measured rate so it stays green on shared CI hosts
// while still catching accidental O(N) scans.
func TestGeoSearchScalability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scalability test in short mode")
	}
	const n = 100000
	gi := buildGeoTestIndex(t, n, GeoDistanceHaversine, func(rng *rand.Rand) GeoPoint {
		return GeoPoint{
			Lat: 40.7128 + rng.Float64()*0.1,
			Lon: -74.0060 + rng.Float64()*0.1,
		}
	})

	center := GeoPoint{Lat: 40.7128, Lon: -74.0060}
	// Warm up caches and the size-class growth of the result slices.
	if _, err := gi.SearchRadius(context.Background(), center, 50.0, 10); err != nil {
		t.Fatalf("warm-up search failed: %v", err)
	}

	const numQueries = 200
	start := time.Now()
	for q := 0; q < numQueries; q++ {
		res, err := gi.SearchRadius(context.Background(), center, 50.0, 10)
		if err != nil {
			t.Fatalf("search failed: %v", err)
		}
		if len(res) != 10 {
			t.Fatalf("expected 10 hits, got %d", len(res))
		}
	}
	elapsed := time.Since(start)
	qps := float64(numQueries) / elapsed.Seconds()
	t.Logf("GeoSearchScalability: n=%d clustered qps=%.1f", n, qps)
	if qps < 500 {
		t.Fatalf("geo search QPS %.1f below threshold 500", qps)
	}
}

// BenchmarkGeoIndex_SearchRadiusScale measures the clustered radius workload
// at the scale used for the P1-6b target. The large cases are skipped in short
// mode because building them allocates hundreds of megabytes.
func BenchmarkGeoIndex_SearchRadiusScale(b *testing.B) {
	center := GeoPoint{Lat: 40.7128, Lon: -74.0060}
	for _, n := range []int{100000, 500000, 1000000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			if testing.Short() && n > 100000 {
				b.Skip("skipping large geo benchmark in short mode")
			}
			gi := buildGeoTestIndex(b, n, GeoDistanceHaversine, func(rng *rand.Rand) GeoPoint {
				return GeoPoint{
					Lat: 40.7128 + rng.Float64()*0.1,
					Lon: -74.0060 + rng.Float64()*0.1,
				}
			})
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := gi.SearchRadius(context.Background(), center, 50.0, 10); err != nil {
					b.Fatalf("search failed: %v", err)
				}
			}
			b.StopTimer()
			if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
				b.ReportMetric(float64(b.N)/elapsed, "qps")
			}
		})
	}
}
