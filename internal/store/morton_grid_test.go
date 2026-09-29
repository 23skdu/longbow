package store

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	mortonQuerySink  []*GeoIndexedVector
	mortonInsertSink bool
)

// randomWorldPoint returns a point uniformly distributed over the globe.
func randomWorldPoint(rng *rand.Rand) GeoPoint {
	return GeoPoint{
		Lat: rng.Float64()*180 - 90,
		Lon: rng.Float64()*360 - 180,
	}
}

// clusteredPoint returns a point scattered around one of three city centres, so
// that many points share a grid cell.
func clusteredPoint(rng *rand.Rand, i int) GeoPoint {
	centers := [...]GeoPoint{
		{Lat: 40.7128, Lon: -74.0060},
		{Lat: 34.0522, Lon: -118.2437},
		{Lat: 51.5074, Lon: -0.1278},
	}
	c := centers[i%len(centers)]
	return GeoPoint{
		Lat: c.Lat + (rng.Float64()-0.5)*0.5,
		Lon: c.Lon + (rng.Float64()-0.5)*0.5,
	}
}

// randomQueryBox returns a random box, occasionally degenerate or fully outside
// the globe.
func randomQueryBox(rng *rand.Rand) GeoBoundingBox {
	center := randomWorldPoint(rng)
	switch rng.Intn(16) {
	case 0:
		// Degenerate box: a single point.
		return GeoBoundingBox{MinLat: center.Lat, MaxLat: center.Lat, MinLon: center.Lon, MaxLon: center.Lon}
	case 1:
		// Whole globe.
		return geoWorldBounds()
	case 2:
		// Entirely north of the globe.
		return GeoBoundingBox{MinLat: 91, MaxLat: 120, MinLon: -10, MaxLon: 10}
	default:
		dLat := rng.Float64() * 20
		dLon := rng.Float64() * 20
		return GeoBoundingBox{
			MinLat: center.Lat - dLat,
			MaxLat: center.Lat + dLat,
			MinLon: center.Lon - dLon,
			MaxLon: center.Lon + dLon,
		}
	}
}

// sortedGeoIDs extracts the sorted ID set of a candidate slice.
func sortedGeoIDs(vectors []*GeoIndexedVector) []uint64 {
	ids := make([]uint64, len(vectors))
	for i, v := range vectors {
		ids[i] = v.ID
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// assertSameIDs asserts that both indexes returned the exact same ID set.
func assertSameIDs(t *testing.T, context string, want, got []*GeoIndexedVector) {
	t.Helper()
	wantIDs := sortedGeoIDs(want)
	gotIDs := sortedGeoIDs(got)
	require.Equal(t, len(wantIDs), len(gotIDs), "%s: result count mismatch (reference=%d morton=%d)", context, len(wantIDs), len(gotIDs))
	if !slices.Equal(wantIDs, gotIDs) {
		t.Fatalf("%s: ID sets differ\nreference=%v\nmorton   =%v", context, wantIDs, gotIDs)
	}
}

// assertIDsSubset asserts that every ID of got also appears in want.
func assertIDsSubset(t *testing.T, context string, want, got []*GeoIndexedVector) {
	t.Helper()
	set := make(map[uint64]struct{})
	for _, id := range sortedGeoIDs(want) {
		set[id] = struct{}{}
	}
	for _, id := range sortedGeoIDs(got) {
		if _, ok := set[id]; !ok {
			t.Fatalf("%s: morton returned ID %d which the reference did not", context, id)
		}
	}
}

// assertRadiusParity checks MortonGrid.QueryRadius against the reference
// quadtree. Quadtree.QueryRadius appends every point of an intersecting leaf
// without a per-point filter, so it is a superset of the true box contents: the
// grid must match the exact box query and stay a subset of the radius query.
func assertRadiusParity(t *testing.T, context string, quadtree *Quadtree, grid *MortonGrid, center GeoPoint, radiusKm float64) {
	t.Helper()
	var quadtreeResults, gridResults []*GeoIndexedVector
	quadtree.QueryRadius(center, radiusKm, &quadtreeResults)
	grid.QueryRadius(center, radiusKm, &gridResults)
	assertSameIDs(t, context+"/box", quadtree.QueryBox(BoundingBox(center, radiusKm)), gridResults)
	assertIDsSubset(t, context+"/radius", quadtreeResults, gridResults)
}

// TestMortonGrid_ParityWithQuadtree is the core guarantee: for identical inputs
// the Morton grid returns exactly the point set the reference quadtree returns.
func TestMortonGrid_ParityWithQuadtree(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	for _, bits := range []int{1, 4, 12, 20} {
		t.Run(fmt.Sprintf("bits=%d", bits), func(t *testing.T) {
			const numPoints = 3000
			const numBoxQueries = 200
			const numRadiusQueries = 200

			rng := rand.New(rand.NewSource(42))
			bounds := geoWorldBounds()
			quadtree := NewQuadtree(bounds, 8, "parity")
			grid := NewMortonGridWithResolution(bounds, bits, numPoints, "parity")
			require.Equal(t, bits, grid.Bits())

			for i := 0; i < numPoints; i++ {
				var point GeoPoint
				if i%2 == 0 {
					point = randomWorldPoint(rng)
				} else {
					point = clusteredPoint(rng, i)
				}
				vec := &GeoIndexedVector{ID: uint64(i), GeoPoint: point}
				assert.Equal(t, quadtree.Insert(vec), grid.Insert(vec), "insert disagreement at point %d", i)
			}
			require.Equal(t, numPoints, grid.Len())

			for i := 0; i < numBoxQueries; i++ {
				box := randomQueryBox(rng)
				assertSameIDs(t, fmt.Sprintf("box %v", box), quadtree.QueryBox(box), grid.QueryBox(box))
			}

			for i := 0; i < numRadiusQueries; i++ {
				center := randomWorldPoint(rng)
				var radiusKm float64
				switch i % 5 {
				case 0:
					radiusKm = 0
				case 1:
					radiusKm = 10000
				case 2:
					// Near the pole the longitude delta explodes.
					center = GeoPoint{Lat: 89.999, Lon: center.Lon}
					radiusKm = 50
				default:
					radiusKm = rng.Float64() * 2000
				}
				assertRadiusParity(t, fmt.Sprintf("radius center=%v r=%v", center, radiusKm), quadtree, grid, center, radiusKm)
			}
		})
	}
}

func TestMortonGrid_Contains(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	bounds := GeoBoundingBox{
		MinLat: 40.0,
		MaxLat: 41.0,
		MinLon: -75.0,
		MaxLon: -74.0,
	}
	g := NewMortonGrid(bounds, 16, "test_dataset")

	assert.True(t, g.Contains(GeoPoint{Lat: 40.5, Lon: -74.5}))
	assert.True(t, g.Contains(GeoPoint{Lat: 40.0, Lon: -75.0}))
	assert.True(t, g.Contains(GeoPoint{Lat: 41.0, Lon: -74.0}))
	assert.False(t, g.Contains(GeoPoint{Lat: 35.0, Lon: -80.0}))
	assert.False(t, g.Contains(GeoPoint{Lat: math.NaN(), Lon: -74.5}))
}

func TestMortonGrid_InsertRejectsOutsideBounds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	bounds := geoWorldBounds()
	g := NewMortonGrid(bounds, 16, "test_dataset")

	outside := []GeoPoint{
		{Lat: 90.0001, Lon: 0},
		{Lat: -90.0001, Lon: 0},
		{Lat: 0, Lon: 180.0001},
		{Lat: 0, Lon: -180.0001},
		{Lat: math.NaN(), Lon: 0},
		{Lat: 0, Lon: math.NaN()},
	}
	for _, p := range outside {
		assert.False(t, g.Insert(&GeoIndexedVector{ID: uint64(len(outside)), GeoPoint: p}), "point %v must be rejected", p)
	}
	assert.Equal(t, 0, g.Len())
	assert.Equal(t, 0, g.CellCount())

	// The inclusive bounds corners must be accepted, exactly like Quadtree.Insert.
	quadtree := NewQuadtree(bounds, 4, "test_dataset")
	corners := []GeoPoint{
		{Lat: 90, Lon: 180},
		{Lat: 90, Lon: -180},
		{Lat: -90, Lon: 180},
		{Lat: -90, Lon: -180},
		{Lat: 90, Lon: 0},
		{Lat: 0, Lon: 180},
	}
	for i, p := range corners {
		vec := &GeoIndexedVector{ID: uint64(i), GeoPoint: p}
		assert.Equal(t, quadtree.Insert(vec), g.Insert(vec), "corner %v", p)
	}
	assert.Equal(t, len(corners), g.Len())
}

func TestMortonGrid_EmptyGrid(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	g := NewMortonGrid(geoWorldBounds(), 16, "test_dataset")

	assert.NotNil(t, g.QueryBox(geoWorldBounds()))
	assert.Empty(t, g.QueryBox(geoWorldBounds()))
	assert.Equal(t, 0, g.Len())
	assert.Equal(t, 0, g.CellCount())

	var results []*GeoIndexedVector
	g.QueryRadius(GeoPoint{Lat: 40, Lon: -74}, 100, &results)
	assert.Empty(t, results)
}

func TestMortonGrid_SinglePoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	bounds := geoWorldBounds()
	quadtree := NewQuadtree(bounds, 4, "test_dataset")
	g := NewMortonGrid(bounds, 16, "test_dataset")

	vec := &GeoIndexedVector{ID: 7, GeoPoint: GeoPoint{Lat: 40.7128, Lon: -74.0060}}
	require.True(t, g.Insert(vec))
	require.True(t, quadtree.Insert(vec))
	assert.Equal(t, 1, g.CellCount())

	hit := GeoBoundingBox{MinLat: 40.0, MaxLat: 41.0, MinLon: -75.0, MaxLon: -73.0}
	miss := GeoBoundingBox{MinLat: 41.5, MaxLat: 42.0, MinLon: -75.0, MaxLon: -73.0}
	assertSameIDs(t, "hit", quadtree.QueryBox(hit), g.QueryBox(hit))
	assertSameIDs(t, "miss", quadtree.QueryBox(miss), g.QueryBox(miss))

	// Degenerate box exactly on the point.
	exact := GeoBoundingBox{MinLat: vec.GeoPoint.Lat, MaxLat: vec.GeoPoint.Lat, MinLon: vec.GeoPoint.Lon, MaxLon: vec.GeoPoint.Lon}
	assertSameIDs(t, "exact", quadtree.QueryBox(exact), g.QueryBox(exact))
}

func TestMortonGrid_AllPointsInOneCell(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	// One bit per axis gives four cells covering the whole globe.
	g := NewMortonGridWithResolution(geoWorldBounds(), 1, 2000, "test_dataset")
	quadtree := NewQuadtree(geoWorldBounds(), 64, "test_dataset")

	rng := rand.New(rand.NewSource(7))
	northWest := GeoBoundingBox{MinLat: 0, MaxLat: 90, MinLon: -180, MaxLon: 0}
	for i := 0; i < 2000; i++ {
		vec := &GeoIndexedVector{ID: uint64(i), GeoPoint: GeoPoint{
			Lat: rng.Float64() * northWest.MaxLat,
			Lon: rng.Float64() * northWest.MaxLon,
		}}
		require.True(t, g.Insert(vec))
		require.True(t, quadtree.Insert(vec))
	}
	require.Equal(t, 2000, g.Len())
	assert.Equal(t, 1, g.CellCount(), "all points must share a single cell")

	assertSameIDs(t, "cell", quadtree.QueryBox(northWest), g.QueryBox(northWest))
	assertSameIDs(t, "world", quadtree.QueryBox(geoWorldBounds()), g.QueryBox(geoWorldBounds()))

	opposite := GeoBoundingBox{MinLat: -90, MaxLat: 0, MinLon: 0, MaxLon: 180}
	assertSameIDs(t, "opposite", quadtree.QueryBox(opposite), g.QueryBox(opposite))
	assert.Empty(t, g.QueryBox(opposite))
}

// TestMortonGrid_CellBoundaries pins points and queries exactly onto cell
// boundaries; the closed query box must behave like the quadtree.
func TestMortonGrid_CellBoundaries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	const bits = 4
	const cells = 1 << bits
	bounds := geoWorldBounds()
	quadtree := NewQuadtree(bounds, 4, "test_dataset")
	g := NewMortonGridWithResolution(bounds, bits, 256, "test_dataset")

	latStep := (bounds.MaxLat - bounds.MinLat) / cells
	lonStep := (bounds.MaxLon - bounds.MinLon) / cells

	// Points on every cell corner, plus the centre of every cell.
	id := uint64(0)
	for i := 0; i <= cells; i++ {
		for j := 0; j <= cells; j++ {
			lat := bounds.MinLat + float64(i)*latStep
			lon := bounds.MinLon + float64(j)*lonStep
			vec := &GeoIndexedVector{ID: id, GeoPoint: GeoPoint{Lat: lat, Lon: lon}}
			require.True(t, g.Insert(vec))
			require.True(t, quadtree.Insert(vec))
			id++
		}
	}
	for i := 0; i < cells; i++ {
		for j := 0; j < cells; j++ {
			vec := &GeoIndexedVector{ID: id, GeoPoint: GeoPoint{
				Lat: bounds.MinLat + (float64(i)+0.5)*latStep,
				Lon: bounds.MinLon + (float64(j)+0.5)*lonStep,
			}}
			require.True(t, g.Insert(vec))
			require.True(t, quadtree.Insert(vec))
			id++
		}
	}

	for i := 0; i < cells; i++ {
		for j := 0; j < cells; j++ {
			cellBox := GeoBoundingBox{
				MinLat: bounds.MinLat + float64(i)*latStep,
				MaxLat: bounds.MinLat + float64(i+1)*latStep,
				MinLon: bounds.MinLon + float64(j)*lonStep,
				MaxLon: bounds.MinLon + float64(j+1)*lonStep,
			}
			want := quadtree.QueryBox(cellBox)
			got := g.QueryBox(cellBox)
			assertSameIDs(t, fmt.Sprintf("cell %d,%d", i, j), want, got)
			// The cell centre is strictly inside, the four corners are on the
			// closed boundary, so every such point must be returned.
			assert.Len(t, got, 5, "cell %d,%d", i, j)
		}
	}
}

func TestMortonGrid_ExtremeCoordinates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	bounds := geoWorldBounds()
	quadtree := NewQuadtree(bounds, 4, "test_dataset")
	grid := NewMortonGrid(bounds, 16, "test_dataset")

	points := []GeoPoint{
		{Lat: 90, Lon: 180},
		{Lat: 90, Lon: -180},
		{Lat: -90, Lon: 180},
		{Lat: -90, Lon: -180},
		{Lat: 90, Lon: 0},
		{Lat: -90, Lon: 0},
		{Lat: 0, Lon: 180},
		{Lat: 0, Lon: -180},
		{Lat: 0, Lon: 0},
	}
	for i, p := range points {
		vec := &GeoIndexedVector{ID: uint64(i), GeoPoint: p}
		require.True(t, grid.Insert(vec), "point %v", p)
		require.True(t, quadtree.Insert(vec), "point %v", p)
	}

	boxes := []GeoBoundingBox{
		geoWorldBounds(),
		{MinLat: 89, MaxLat: 90, MinLon: -1, MaxLon: 1},
		{MinLat: -90, MaxLat: -89, MinLon: 179, MaxLon: 180},
		{MinLat: 85, MaxLat: 90, MinLon: 179, MaxLon: 180},
		{MinLat: -1, MaxLat: 1, MinLon: -180, MaxLon: -179},
		{MinLat: 91, MaxLat: 95, MinLon: -1, MaxLon: 1},
		{MinLat: -95, MaxLat: -91, MinLon: -1, MaxLon: 1},
		{MinLat: 0, MaxLat: 10, MinLon: 181, MaxLon: 190},
	}
	for i, box := range boxes {
		assertSameIDs(t, fmt.Sprintf("box %d %v", i, box), quadtree.QueryBox(box), grid.QueryBox(box))
	}

	// A radius query from near the pole degenerates into a global longitude span.
	centers := []struct {
		center    GeoPoint
		radiusKm  float64
		expectLen int
	}{
		{GeoPoint{Lat: 89.999, Lon: 0}, 50, 3},
		{GeoPoint{Lat: 0, Lon: 180}, 1, 1},
	}
	for _, tc := range centers {
		var gridResults []*GeoIndexedVector
		grid.QueryRadius(tc.center, tc.radiusKm, &gridResults)
		assertRadiusParity(t, fmt.Sprintf("radius %v", tc.center), quadtree, grid, tc.center, tc.radiusKm)
		require.Len(t, gridResults, tc.expectLen, "radius %v", tc.center)
	}
}

func TestMortonGrid_DuplicatePoints(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	bounds := geoWorldBounds()
	quadtree := NewQuadtree(bounds, 4, "test_dataset")
	g := NewMortonGrid(bounds, 16, "test_dataset")

	// Three vectors on the exact same location, plus one in a neighbouring cell.
	point := GeoPoint{Lat: 40.7128, Lon: -74.0060}
	for i := 0; i < 3; i++ {
		vec := &GeoIndexedVector{ID: uint64(i), GeoPoint: point}
		require.True(t, g.Insert(vec))
		require.True(t, quadtree.Insert(vec))
	}
	other := &GeoIndexedVector{ID: 3, GeoPoint: GeoPoint{Lat: 89.9, Lon: 179.9}}
	require.True(t, g.Insert(other))
	require.True(t, quadtree.Insert(other))

	assert.Equal(t, 4, g.Len())
	assert.Equal(t, 2, g.CellCount())

	// Duplicates are reported once per insert, like the quadtree does.
	box := GeoBoundingBox{MinLat: 40, MaxLat: 41, MinLon: -75, MaxLon: -73}
	assertSameIDs(t, "duplicates", quadtree.QueryBox(box), g.QueryBox(box))
	assert.Len(t, g.QueryBox(box), 3)

	global := g.QueryBox(geoWorldBounds())
	assertSameIDs(t, "global", quadtree.QueryBox(geoWorldBounds()), global)
	assert.Len(t, global, 4)
}

func TestMortonGrid_InvalidQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	g := NewMortonGrid(geoWorldBounds(), 16, "test_dataset")
	require.True(t, g.Insert(&GeoIndexedVector{ID: 1, GeoPoint: GeoPoint{Lat: 0, Lon: 0}}))

	nan := math.NaN()
	boxes := []GeoBoundingBox{
		{MinLat: nan, MaxLat: 10, MinLon: -10, MaxLon: 10},
		{MinLat: -10, MaxLat: 10, MinLon: nan, MaxLon: 10},
		{MinLat: -10, MaxLat: nan, MinLon: -10, MaxLon: 10},
		{MinLat: -10, MaxLat: 10, MinLon: -10, MaxLon: nan},
		{MinLat: 10, MaxLat: -10, MinLon: -10, MaxLon: 10},
	}
	for _, box := range boxes {
		assert.Empty(t, g.QueryBox(box), "box %v", box)
	}

	// A degenerate grid (zero extent) still answers consistently.
	degenerate := NewMortonGrid(GeoBoundingBox{MinLat: 5, MaxLat: 5, MinLon: 7, MaxLon: 7}, 4, "test_dataset")
	require.True(t, degenerate.Insert(&GeoIndexedVector{ID: 1, GeoPoint: GeoPoint{Lat: 5, Lon: 7}}))
	require.False(t, degenerate.Insert(&GeoIndexedVector{ID: 2, GeoPoint: GeoPoint{Lat: 5.1, Lon: 7}}))
	assert.Len(t, degenerate.QueryBox(GeoBoundingBox{MinLat: 4, MaxLat: 6, MinLon: 6, MaxLon: 8}), 1)
}

// TestMortonGrid_InsertAllocations shows the grid never allocates a node per
// insert, while the quadtree does.
func TestMortonGrid_InsertAllocations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	t.Run("same_point", func(t *testing.T) {
		vec := &GeoIndexedVector{ID: 1, GeoPoint: GeoPoint{Lat: 40.7128, Lon: -74.0060}}
		grid := NewMortonGrid(geoWorldBounds(), 4096, "alloc")
		quadtree := NewQuadtree(geoWorldBounds(), 64, "alloc")

		mortonAllocs := testing.AllocsPerRun(100, func() { grid.Insert(vec) })
		quadtreeAllocs := testing.AllocsPerRun(100, func() { quadtree.Insert(vec) })
		t.Logf("same_point: morton=%.2f allocs/op quadtree=%.2f allocs/op", mortonAllocs, quadtreeAllocs)
		assert.Zero(t, mortonAllocs, "MortonGrid.Insert must not allocate")
		assert.Positive(t, quadtreeAllocs, "Quadtree.Insert allocates child nodes")
	})

	t.Run("distinct_points", func(t *testing.T) {
		// The vectors are allocated up front so only index bookkeeping is
		// measured, and each run inserts a whole batch so the per-run node
		// allocations of the quadtree are visible in the average.
		const batch = 128
		const runs = 100
		points := make([]*GeoIndexedVector, batch)
		rng := rand.New(rand.NewSource(11))
		for i := range points {
			points[i] = &GeoIndexedVector{ID: uint64(i), GeoPoint: randomWorldPoint(rng)}
		}

		grid := NewMortonGrid(geoWorldBounds(), 2*batch*runs, "alloc")
		quadtree := NewQuadtree(geoWorldBounds(), 8, "alloc")

		mortonAllocs := testing.AllocsPerRun(runs, func() {
			for _, p := range points {
				grid.Insert(p)
			}
		})
		quadtreeAllocs := testing.AllocsPerRun(runs, func() {
			for _, p := range points {
				quadtree.Insert(p)
			}
		})
		t.Logf("distinct_points: morton=%.2f allocs/op quadtree=%.2f allocs/op", mortonAllocs, quadtreeAllocs)
		assert.Zero(t, mortonAllocs, "MortonGrid.Insert must not allocate")
		assert.Positive(t, quadtreeAllocs, "Quadtree.Insert allocates child nodes")
	})
}

func TestMortonGrid_ConcurrentInsertsAndQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	grid := NewMortonGrid(geoWorldBounds(), 4096, "concurrent")

	const numInserters = 4
	const pointsPerInserter = 2000
	var wg sync.WaitGroup

	wg.Add(numInserters)
	for i := 0; i < numInserters; i++ {
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < pointsPerInserter; j++ {
				grid.Insert(&GeoIndexedVector{ID: uint64(idx*pointsPerInserter + j), GeoPoint: GeoPoint{
					Lat: 40.0 + float64(j)*0.001,
					Lon: -74.0 + float64(idx)*0.1,
				}})
			}
		}(i)
	}

	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				grid.QueryBox(GeoBoundingBox{MinLat: 40.0, MaxLat: 41.0, MinLon: -75.0, MaxLon: -73.0})
				var results []*GeoIndexedVector
				grid.QueryRadius(GeoPoint{Lat: 40.5, Lon: -73.5}, 50, &results)
				grid.Contains(GeoPoint{Lat: 40.5, Lon: -73.5})
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, numInserters*pointsPerInserter, grid.Len())
}

// TestGeoIndex_MortonConcurrency exercises the default (Morton) index through
// the same concurrent Add/SearchRadius path as TestGeoIndex_Concurrency.
func TestGeoIndex_MortonConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	config := &GeoSearchConfig{
		DistanceType: GeoDistanceHaversine,
		EarthRadius:  6371.0,
		IndexType:    GeoIndexTypeMorton,
	}
	gi := NewGeoIndex("test_morton", 128, config)
	require.IsType(t, &MortonGrid{}, gi.pointIndexRef())

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

	numSearchers := 4
	wg.Add(numSearchers)
	for i := 0; i < numSearchers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = gi.SearchRadius(t.Context(), GeoPoint{Lat: 40.5, Lon: -73.5}, 50, 10)
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(numInserters*vectorsPerInserter), gi.pointCount.Load())
}

func TestGeoIndex_IndexTypeSelection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	// The Morton grid is opt-in: inserts are ~2.3x faster and allocation-free,
	// but its fixed resolution makes measured queries 17-40% slower than the
	// quadtree, so the default must stay on the quadtree.
	cases := []struct {
		name      string
		indexType string
		nilConfig bool
		want      GeoPointIndex
	}{
		{name: "nil_config", nilConfig: true, want: &Quadtree{}},
		{name: "unset_defaults_to_quadtree", indexType: "", want: &Quadtree{}},
		{name: "morton", indexType: GeoIndexTypeMorton, want: &MortonGrid{}},
		{name: "quadtree", indexType: GeoIndexTypeQuadtree, want: &Quadtree{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var config *GeoSearchConfig
			if !tc.nilConfig {
				config = &GeoSearchConfig{
					DistanceType: GeoDistanceHaversine,
					EarthRadius:  6371.0,
					IndexType:    tc.indexType,
				}
			}
			gi := NewGeoIndex("sel_"+tc.name, 128, config)
			idx := gi.pointIndexRef()
			require.NotNil(t, idx)
			assert.IsType(t, tc.want, idx)
		})
	}
}

func TestMortonEncodeDecode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	assert.Equal(t, uint64(0), mortonEncode(0, 0))
	assert.Equal(t, uint64(1), mortonEncode(0, 1))
	assert.Equal(t, uint64(2), mortonEncode(1, 0))
	assert.Equal(t, uint64(3), mortonEncode(1, 1))
	assert.Equal(t, uint64(4), mortonEncode(0, 2))
	assert.Equal(t, uint64(6), mortonEncode(1, 2))
	assert.Equal(t, uint64(8), mortonEncode(2, 0))
	assert.Equal(t, uint64(9), mortonEncode(2, 1))

	// Z-order codes are monotone in each axis, which is what bounds the cell
	// code interval used to skip cells during a query.
	for y := uint32(0); y < 64; y++ {
		prev := mortonEncode(0, y)
		for x := uint32(1); x <= 1000; x++ {
			code := mortonEncode(x, y)
			assert.GreaterOrEqual(t, code, prev)
			prev = code
		}
	}
	for x := uint32(0); x < 64; x++ {
		prev := mortonEncode(x, 0)
		for y := uint32(1); y <= 1000; y++ {
			code := mortonEncode(x, y)
			assert.GreaterOrEqual(t, code, prev)
			prev = code
		}
	}
	assert.Equal(t, uint64(3)<<62, mortonEncode(1<<31, 1<<31))

	for i := 0; i < 5000; i++ {
		x := uint32(i * 2654435761)
		y := uint32(i * 40503)
		gotX, gotY := mortonDecode(mortonEncode(x, y))
		assert.Equal(t, x, gotX)
		assert.Equal(t, y, gotY)
	}
}

// benchmarkGeoInsert measures single point inserts of a pre-built point set. The
// b.N form is used instead of b.Loop because the loop body indexes a slice sized
// after b.N and b.Loop may run the body more often than b.N times.
func benchmarkGeoInsert(b *testing.B, newIndex func(points int) GeoPointIndex) {
	points := make([]*GeoIndexedVector, b.N)
	rng := rand.New(rand.NewSource(1))
	for i := range points {
		points[i] = &GeoIndexedVector{ID: uint64(i), GeoPoint: randomWorldPoint(rng)}
	}
	// Pre-sized so the measurement covers steady-state inserts, not the amortised
	// growth of the entry arena and the cell directory.
	index := newIndex(b.N)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mortonInsertSink = index.Insert(points[i])
	}
}

func BenchmarkMortonGrid_Insert(b *testing.B) {
	benchmarkGeoInsert(b, func(points int) GeoPointIndex {
		return NewMortonGrid(geoWorldBounds(), points, "bench")
	})
}

func BenchmarkQuadtree_Insert(b *testing.B) {
	benchmarkGeoInsert(b, func(_ int) GeoPointIndex {
		return NewQuadtree(geoWorldBounds(), 64, "bench")
	})
}

// benchmarkGeoInsertBatch inserts a fixed batch of distinct points per
// operation, which keeps the quadtree subdividing and makes its per-operation
// node allocations visible next to the allocation free grid.
func benchmarkGeoInsertBatch(b *testing.B, batch int, newIndex func(points int) GeoPointIndex) {
	points := make([]*GeoIndexedVector, batch*b.N)
	rng := rand.New(rand.NewSource(1))
	for i := range points {
		points[i] = &GeoIndexedVector{ID: uint64(i), GeoPoint: randomWorldPoint(rng)}
	}
	index := newIndex(len(points))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range points[i*batch : (i+1)*batch] {
			mortonInsertSink = index.Insert(p)
		}
	}
}

func BenchmarkMortonGrid_InsertBatch(b *testing.B) {
	benchmarkGeoInsertBatch(b, 8, func(points int) GeoPointIndex {
		return NewMortonGrid(geoWorldBounds(), points, "bench")
	})
}

func BenchmarkQuadtree_InsertBatch(b *testing.B) {
	benchmarkGeoInsertBatch(b, 8, func(_ int) GeoPointIndex {
		return NewQuadtree(geoWorldBounds(), 8, "bench")
	})
}

func benchmarkGeoQueryBox(b *testing.B, newIndex func(bounds GeoBoundingBox, points int) GeoPointIndex, boxSpan float64) {
	const numPoints = 10000
	bounds := geoWorldBounds()
	index := newIndex(bounds, numPoints)

	rng := rand.New(rand.NewSource(7))
	for i := 0; i < numPoints; i++ {
		index.Insert(&GeoIndexedVector{ID: uint64(i), GeoPoint: clusteredPoint(rng, i)})
	}

	boxes := make([]GeoBoundingBox, 32)
	for i := range boxes {
		center := clusteredPoint(rng, i)
		span := rng.Float64() * boxSpan
		boxes[i] = GeoBoundingBox{
			MinLat: center.Lat - span,
			MaxLat: center.Lat + span,
			MinLon: center.Lon - span,
			MaxLon: center.Lon + span,
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	for b.Loop() {
		mortonQuerySink = index.QueryBox(boxes[i%len(boxes)])
		i++
	}
}

func BenchmarkMortonGrid_QueryBox(b *testing.B) {
	b.Run("selective", func(b *testing.B) {
		benchmarkGeoQueryBox(b, func(bounds GeoBoundingBox, points int) GeoPointIndex {
			return NewMortonGrid(bounds, points, "bench")
		}, 0.2)
	})
	b.Run("global", func(b *testing.B) {
		benchmarkGeoQueryBox(b, func(bounds GeoBoundingBox, points int) GeoPointIndex {
			return NewMortonGrid(bounds, points, "bench")
		}, 180)
	})
}

func BenchmarkQuadtree_QueryBox(b *testing.B) {
	b.Run("selective", func(b *testing.B) {
		benchmarkGeoQueryBox(b, func(bounds GeoBoundingBox, _ int) GeoPointIndex {
			return NewQuadtree(bounds, 64, "bench")
		}, 0.2)
	})
	b.Run("global", func(b *testing.B) {
		benchmarkGeoQueryBox(b, func(bounds GeoBoundingBox, _ int) GeoPointIndex {
			return NewQuadtree(bounds, 64, "bench")
		}, 180)
	})
}
