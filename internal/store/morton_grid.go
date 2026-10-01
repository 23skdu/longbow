package store

import (
	"math"
	"sync"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// Linear (non-recursive) spatial index used by GeoIndex.
const (
	// defaultMortonBits is the per-axis cell resolution used by NewMortonGrid
	// (2^12 x 2^12 cells over the globe, roughly 5 km x 8 km at the equator).
	defaultMortonBits = 12

	// mortonMaxBits is the largest per-axis resolution: a 64-bit Z-order code
	// carries 32 bits per axis.
	mortonMaxBits = 32

	// mortonMinTableSlots is the smallest power-of-two cell directory size.
	mortonMinTableSlots = 16

	// mortonMaxPreallocSlots caps the directory pre-sizing. Beyond it the
	// directory grows on demand, which keeps the 12 bytes/slot directory from
	// dwarfing the entries it indexes.
	mortonMaxPreallocSlots = 1 << 20

	// mortonLoadNum/mortonLoadDen is the load factor at which the open
	// addressed directory is doubled and rehashed.
	mortonLoadNum = 7
	mortonLoadDen = 10

	// mortonDefaultCapacity is the entry capacity used when the caller does not
	// know how many points will be indexed.
	mortonDefaultCapacity = 1024

	// mortonHashMultiplier is the 64-bit golden ratio used to spread Z-order
	// codes over the directory (Fibonacci hashing).
	mortonHashMultiplier = 0x9E3779B97F4A7C15
)

// mortonEntry is a single indexed point. Entries live in one contiguous arena;
// next is a 1-based index into that arena, with 0 terminating a cell chain.
type mortonEntry struct {
	vec  *GeoIndexedVector
	lat  float64
	lon  float64
	next uint32
}

var (
	_ GeoPointIndex = (*MortonGrid)(nil)
	_ GeoPointIndex = (*Quadtree)(nil)
)

// MortonGrid is a linear spatial index: a uniform grid of Z-order (Morton)
// coded cells backed by contiguous slices, used instead of the recursive
// Quadtree. The covered bounds are cut into 2^bits x 2^bits cells, a cell is
// identified by the 64-bit Morton code of its corner, and indexed points are
// appended to a single contiguous arena. The cell directory is an open-addressed
// table of uint32 chain heads, so an insert only grows flat slices and never
// allocates a child node the way Quadtree.subdivide does.
//
// QueryBox converts a query into a cell rectangle, walks the Z-order code
// interval of that rectangle and filters every candidate with the same
// inclusive lat/lon comparison the Quadtree applies, so both indexes return
// the same point set for the same inputs. MortonGrid is safe for concurrent use.
type MortonGrid struct {
	bounds       GeoBoundingBox
	latSpan      float64
	lonSpan      float64
	cellsPerAxis uint64
	bits         uint32
	cellsCreated prometheus.Counter

	mu       sync.RWMutex
	entries  []mortonEntry
	heads    []uint32
	keys     []uint64
	occupied int
}

// NewMortonGrid creates a Morton-coded spatial grid over bounds using the
// default cell resolution. expectedPoints pre-sizes the entry arena and the
// cell directory; values <= 0 fall back to a modest default.
func NewMortonGrid(bounds GeoBoundingBox, expectedPoints int, datasetName string) *MortonGrid {
	return NewMortonGridWithResolution(bounds, defaultMortonBits, expectedPoints, datasetName)
}

// NewMortonGridWithResolution creates a Morton-coded spatial grid with an
// explicit per-axis cell resolution of 2^bits cells per axis (1 <= bits <= 32).
// expectedPoints pre-sizes the entry arena and the cell directory.
func NewMortonGridWithResolution(bounds GeoBoundingBox, bits, expectedPoints int, datasetName string) *MortonGrid {
	if bits <= 0 {
		bits = defaultMortonBits
	}
	if bits > mortonMaxBits {
		bits = mortonMaxBits
	}
	if expectedPoints <= 0 {
		expectedPoints = mortonDefaultCapacity
	}

	g := &MortonGrid{
		bounds:       bounds,
		latSpan:      bounds.MaxLat - bounds.MinLat,
		lonSpan:      bounds.MaxLon - bounds.MinLon,
		cellsPerAxis: uint64(1) << uint(bits),
		bits:         uint32(bits), // #nosec G115 -- clamped to mortonMaxBits above
		cellsCreated: metrics.MortonGridCellsCreatedTotal.WithLabelValues(datasetName),
	}

	slots := mortonMinTableSlots
	for slots < 2*expectedPoints && slots < mortonMaxPreallocSlots {
		slots <<= 1
	}
	g.heads = make([]uint32, slots)
	g.keys = make([]uint64, slots)
	g.entries = make([]mortonEntry, 0, expectedPoints)
	return g
}

// Insert adds a geographic vector to the grid. It reports whether the point
// fell inside the grid bounds; points outside the bounds are rejected, exactly
// like Quadtree.Insert.
func (g *MortonGrid) Insert(vec *GeoIndexedVector) bool {
	if !g.Contains(vec.GeoPoint) {
		return false
	}

	lat, lon := vec.GeoPoint.Lat, vec.GeoPoint.Lon
	code := g.cellCode(lat, lon)

	g.mu.Lock()
	// Compare in uint64 space: math.MaxUint32 overflows int on 32-bit
	// platforms, where this guard is trivially false anyway.
	if uint64(len(g.entries)) >= math.MaxUint32 {
		g.mu.Unlock()
		return false
	}
	if (g.occupied+1)*mortonLoadDen > len(g.heads)*mortonLoadNum {
		g.grow()
	}
	slot := g.findSlot(code)
	if g.heads[slot] == 0 {
		g.keys[slot] = code
		g.occupied++
		g.cellsCreated.Inc()
	}
	g.entries = append(g.entries, mortonEntry{vec: vec, lat: lat, lon: lon, next: g.heads[slot]})
	g.heads[slot] = uint32(len(g.entries)) // #nosec G115 -- bounded by the check above
	g.mu.Unlock()
	return true
}

// Contains checks if a GeoPoint is within the grid bounds.
func (g *MortonGrid) Contains(point GeoPoint) bool {
	return point.Lat >= g.bounds.MinLat && point.Lat <= g.bounds.MaxLat &&
		point.Lon >= g.bounds.MinLon && point.Lon <= g.bounds.MaxLon
}

// QueryRadius returns all vectors within a given radius from a point.
func (g *MortonGrid) QueryRadius(center GeoPoint, radiusKm float64, results *[]*GeoIndexedVector) {
	g.appendBox(BoundingBox(center, radiusKm), results)
}

// QueryBox returns all vectors within a bounding box.
func (g *MortonGrid) QueryBox(box GeoBoundingBox) []*GeoIndexedVector {
	results := make([]*GeoIndexedVector, 0, 128)
	g.appendBox(box, &results)
	return results
}

// Len returns the number of indexed points.
func (g *MortonGrid) Len() int {
	g.mu.RLock()
	n := len(g.entries)
	g.mu.RUnlock()
	return n
}

// CellCount returns the number of occupied cells.
func (g *MortonGrid) CellCount() int {
	g.mu.RLock()
	n := g.occupied
	g.mu.RUnlock()
	return n
}

// Bits returns the per-axis cell resolution of the grid.
func (g *MortonGrid) Bits() int {
	return int(g.bits)
}

// appendBox appends every indexed point that falls inside box. It mirrors
// Quadtree.queryBoxRecursive: cells are pruned by overlap, points by an
// inclusive lat/lon test.
func (g *MortonGrid) appendBox(box GeoBoundingBox, results *[]*GeoIndexedVector) {
	if math.IsNaN(box.MinLat) || math.IsNaN(box.MaxLat) || math.IsNaN(box.MinLon) || math.IsNaN(box.MaxLon) {
		return
	}
	if box.MaxLat < g.bounds.MinLat || box.MinLat > g.bounds.MaxLat ||
		box.MaxLon < g.bounds.MinLon || box.MinLon > g.bounds.MaxLon {
		return
	}

	x0 := g.cellAxis(box.MinLat, g.bounds.MinLat, g.latSpan)
	x1 := g.cellAxis(box.MaxLat, g.bounds.MinLat, g.latSpan)
	y0 := g.cellAxis(box.MinLon, g.bounds.MinLon, g.lonSpan)
	y1 := g.cellAxis(box.MaxLon, g.bounds.MinLon, g.lonSpan)

	spanX, spanY := x1-x0+1, y1-y0+1

	g.mu.RLock()
	// The cell count is only computed once both spans are known to fit in the
	// directory, which keeps the product from overflowing at high resolutions.
	slots := uint64(len(g.heads))
	if spanX <= slots && spanY <= slots && spanX*spanY <= slots {
		for x := x0; x <= x1; x++ {
			for y := y0; y <= y1; y++ {
				g.collect(g.cellKey(x, y), box, results)
			}
		}
	} else {
		// The query covers more cells than the directory has slots, so walking
		// the occupied cells is cheaper than walking the cell rectangle. The
		// Z-order interval of the rectangle rejects the outside cells with a
		// single comparison.
		zLo := g.cellKey(x0, y0)
		zHi := g.cellKey(x1, y1)
		for i, head := range g.heads {
			if head == 0 {
				continue
			}
			key := g.keys[i]
			if key < zLo || key > zHi {
				continue
			}
			x, y := mortonDecode(key)
			if uint64(x) < x0 || uint64(x) > x1 || uint64(y) < y0 || uint64(y) > y1 {
				continue
			}
			g.collectHead(head, box, results)
		}
	}
	g.mu.RUnlock()
}

// cellAxis maps a coordinate onto its uniform cell index, clamping to the
// outermost cells. The mapping is monotone non-decreasing, so the cells that
// can hold a closed interval are exactly those between its clamped endpoints.
func (g *MortonGrid) cellAxis(value, min, span float64) uint64 {
	if span <= 0 {
		return 0
	}
	f := (value - min) / span
	switch {
	case f <= 0:
		return 0
	case f >= 1:
		return g.cellsPerAxis - 1
	}
	cell := uint64(f * float64(g.cellsPerAxis))
	if cell >= g.cellsPerAxis {
		return g.cellsPerAxis - 1
	}
	return cell
}

// cellKey returns the 64-bit Z-order code identifying a grid cell.
func (g *MortonGrid) cellKey(x, y uint64) uint64 {
	return mortonEncode(uint32(x), uint32(y)) // #nosec G115 -- x, y < 2^bits <= 2^32
}

// cellCode returns the Z-order code of the cell holding the given point. The
// cell corner is encoded rather than the point itself, so every point of a cell
// maps to the same key.
func (g *MortonGrid) cellCode(lat, lon float64) uint64 {
	return g.cellKey(
		g.cellAxis(lat, g.bounds.MinLat, g.latSpan),
		g.cellAxis(lon, g.bounds.MinLon, g.lonSpan),
	)
}

// collect appends the points of the cell identified by key that fall inside box.
func (g *MortonGrid) collect(key uint64, box GeoBoundingBox, results *[]*GeoIndexedVector) {
	if head := g.findHead(key); head != 0 {
		g.collectHead(head, box, results)
	}
}

// collectHead walks one cell chain, appending the points inside box. Callers
// must hold at least the read lock.
func (g *MortonGrid) collectHead(head uint32, box GeoBoundingBox, results *[]*GeoIndexedVector) {
	for i := head; i != 0; {
		e := &g.entries[i-1]
		if e.lat >= box.MinLat && e.lat <= box.MaxLat && e.lon >= box.MinLon && e.lon <= box.MaxLon {
			*results = append(*results, e.vec)
		}
		i = e.next
	}
}

// findHead returns the 1-based head entry of the cell chain for key, or 0 when
// the cell is unoccupied. Callers must hold at least the read lock.
func (g *MortonGrid) findHead(key uint64) uint32 {
	mask := uint64(len(g.heads) - 1)
	i := mortonHash(key) & mask
	for g.heads[i] != 0 {
		if g.keys[i] == key {
			return g.heads[i]
		}
		i = (i + 1) & mask
	}
	return 0
}

// findSlot returns the directory slot for code, claiming an unoccupied slot when
// the code is absent. Callers must hold the write lock.
func (g *MortonGrid) findSlot(code uint64) int {
	mask := uint64(len(g.heads) - 1)
	i := mortonHash(code) & mask
	for g.heads[i] != 0 {
		if g.keys[i] == code {
			return int(i) // #nosec G115 -- bounded by the directory length
		}
		i = (i + 1) & mask
	}
	return int(i) // #nosec G115 -- bounded by the directory length
}

// grow doubles the cell directory and rehashes the occupied cell heads. Cell
// chains are stored in the entry arena and are unaffected. Callers must hold
// the write lock.
func (g *MortonGrid) grow() {
	oldHeads, oldKeys := g.heads, g.keys
	slots := len(oldHeads) * 2
	g.heads = make([]uint32, slots)
	g.keys = make([]uint64, slots)

	mask := uint64(slots - 1)
	for i, head := range oldHeads {
		if head == 0 {
			continue
		}
		key := oldKeys[i]
		j := mortonHash(key) & mask
		for g.heads[j] != 0 {
			j = (j + 1) & mask
		}
		g.keys[j] = key
		g.heads[j] = head
	}
}

// mortonHash mixes a Z-order code into a directory index.
func mortonHash(code uint64) uint64 {
	return (code * mortonHashMultiplier) >> 32
}

// mortonEncode interleaves the bits of x and y into a 64-bit Z-order code: the
// lowest bit of the code is the lowest bit of y, the next is the lowest bit of x.
func mortonEncode(x, y uint32) uint64 {
	return splitBy2Bits(x)<<1 | splitBy2Bits(y)
}

// mortonDecode splits a 64-bit Z-order code back into its axis indices.
func mortonDecode(code uint64) (x, y uint32) {
	return compactBy2Bits(code >> 1), compactBy2Bits(code)
}

// splitBy2Bits spreads the 32 bits of v so that each one occupies an even bit
// position.
func splitBy2Bits(v uint32) uint64 {
	x := uint64(v)
	x = (x | x<<16) & 0x0000FFFF0000FFFF
	x = (x | x<<8) & 0x00FF00FF00FF00FF
	x = (x | x<<4) & 0x0F0F0F0F0F0F0F0F
	x = (x | x<<2) & 0x3333333333333333
	x = (x | x<<1) & 0x5555555555555555
	return x
}

// compactBy2Bits is the inverse of splitBy2Bits.
func compactBy2Bits(v uint64) uint32 {
	x := v & 0x5555555555555555
	x = (x | x>>1) & 0x3333333333333333
	x = (x | x>>2) & 0x0F0F0F0F0F0F0F0F
	x = (x | x>>4) & 0x00FF00FF00FF00FF
	x = (x | x>>8) & 0x0000FFFF0000FFFF
	x = (x | x>>16) & 0x00000000FFFFFFFF
	return uint32(x)
}
