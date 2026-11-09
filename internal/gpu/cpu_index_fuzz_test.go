package gpu

import (
	"encoding/binary"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// Fuzz helpers: Go fuzz targets only accept []byte/string/scalar arguments, so
// typed vectors are transported as little-endian byte blobs and decoded here.

func encodeFuzzInt64s(vals []int64) []byte {
	b := make([]byte, 8*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint64(b[i*8:], uint64(v))
	}
	return b
}

func encodeFuzzFloat32s(vals []float32) []byte {
	b := make([]byte, 4*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(v))
	}
	return b
}

func encodeFuzzUint32s(vals []uint32) []byte {
	b := make([]byte, 4*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint32(b[i*4:], v)
	}
	return b
}

func decodeFuzzInt64s(b []byte) []int64 {
	out := make([]int64, len(b)/8)
	for i := range out {
		out[i] = int64(binary.LittleEndian.Uint64(b[i*8:]))
	}
	return out
}

func decodeFuzzFloat32s(b []byte) []float32 {
	out := make([]float32, len(b)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out
}

func decodeFuzzUint32s(b []byte) []uint32 {
	out := make([]uint32, len(b)/4)
	for i := range out {
		out[i] = binary.LittleEndian.Uint32(b[i*4:])
	}
	return out
}

// refSqDist is an independent scalar reference for the squared euclidean
// distance used by the CPU fallback index. It accumulates in float32 in the
// same order as euclideanDistance so parity is expected to be exact.
func refSqDist(a, b []float32) float32 {
	var sum float32
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum
}

// refHaversine is an independent float64 reference for HaversineSearch.
// Inputs are bounded to |lat| <= 85 and |lon| <= 89 so the haversine term
// stays strictly below 1 (no NaN from sqrt(1-a)).
func refHaversine(centerLat, centerLon, lat, lon, radius float32) float64 {
	const deg = math.Pi / 180
	dLat := (float64(lat) - float64(centerLat)) * deg
	dLon := (float64(lon) - float64(centerLon)) * deg
	lat1 := float64(centerLat) * deg
	lat2 := float64(lat) * deg
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return float64(radius) * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func FuzzCPUIndexSearchParity(f *testing.F) {
	f.Add(
		encodeFuzzInt64s([]int64{1, 2, 3}),
		encodeFuzzFloat32s([]float32{0, 0, 1, 1, 2, 2, 3, 3}),
		encodeFuzzFloat32s([]float32{1, 1}),
		encodeFuzzUint32s([]uint32{1, 3, 9}),
		2,
	)
	f.Add(
		encodeFuzzInt64s([]int64{7}),
		encodeFuzzFloat32s([]float32{1, 2, 3, 4}),
		encodeFuzzFloat32s([]float32{4, 3, 2, 1}),
		encodeFuzzUint32s([]uint32{7}),
		5,
	)
	f.Add(
		encodeFuzzInt64s([]int64{10, 11}),
		encodeFuzzFloat32s([]float32{0.5, -0.5, 1.5, 2.5}),
		encodeFuzzFloat32s([]float32{0.5, -0.5}),
		encodeFuzzUint32s([]uint32{11}),
		0,
	)
	// Negative k: out of range, dropped before hitting the code under test.
	f.Add(
		encodeFuzzInt64s([]int64{5, 5}),
		encodeFuzzFloat32s([]float32{1, 1, 2, 2}),
		encodeFuzzFloat32s([]float32{9, 9}),
		encodeFuzzUint32s([]uint32{5, 6}),
		-1,
	)
	// Empty index.
	f.Add([]byte{}, []byte{}, encodeFuzzFloat32s([]float32{1}), []byte{}, 3)
	// Zero-length stored vectors.
	f.Add(
		encodeFuzzInt64s([]int64{1, 2}),
		[]byte{},
		encodeFuzzFloat32s([]float32{2, 2}),
		encodeFuzzUint32s([]uint32{1, 2}),
		1,
	)
	// Query longer than stored vectors (distance over the common prefix).
	f.Add(
		encodeFuzzInt64s([]int64{1, 2}),
		encodeFuzzFloat32s([]float32{1, 2, 3, 4}),
		encodeFuzzFloat32s([]float32{1, 2, 3, 4, 5}),
		encodeFuzzUint32s([]uint32{2}),
		1,
	)
	// Huge magnitudes: squares overflow to +Inf but never NaN.
	f.Add(
		encodeFuzzInt64s([]int64{-4, 8}),
		encodeFuzzFloat32s([]float32{1e30, 0, 0, -1e30}),
		encodeFuzzFloat32s([]float32{1e30, 1}),
		encodeFuzzUint32s([]uint32{4, 8}),
		3,
	)

	f.Fuzz(func(t *testing.T, idsRaw, dataRaw, queryRaw, candRaw []byte, k int) {
		// Bound inputs; out-of-range values are dropped, never failed on.
		if len(idsRaw) > 64*8 || len(dataRaw) > 4096*4 || len(queryRaw) > 512*4 || len(candRaw) > 64*4 {
			return
		}
		if k < 0 {
			return
		}
		ids := decodeFuzzInt64s(idsRaw)
		data := decodeFuzzFloat32s(dataRaw)
		query := decodeFuzzFloat32s(queryRaw)
		candidates := decodeFuzzUint32s(candRaw)
		for _, v := range data {
			// NaN/Inf mixes could yield Inf-Inf = NaN distances, which would
			// make distance ordering ill-defined; drop them.
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return
			}
		}
		for _, v := range query {
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return
			}
		}

		idx, err := NewCPUIndex(GPUConfig{Dimension: 4, DeviceID: 0})
		require.NoError(t, err)
		require.NoError(t, idx.Add(ids, data))

		// Rebuild the exact same reference store the index built.
		vpd := 0
		if len(ids) > 0 {
			vpd = len(data) / len(ids)
		}
		ref := make(map[int64][]float32, len(ids))
		for i, id := range ids {
			start := i * vpd
			ref[id] = data[start : start+vpd]
		}

		// --- Search parity ---
		gotIDs, gotDists, err := idx.Search(query, k)
		require.NoError(t, err)

		wantN := k
		if wantN > len(ref) {
			wantN = len(ref)
		}
		require.Len(t, gotIDs, wantN)
		require.Len(t, gotDists, wantN)

		refDists := make([]float32, 0, len(ref))
		for _, vec := range ref {
			refDists = append(refDists, refSqDist(query, vec))
		}
		sort.Slice(refDists, func(a, b int) bool { return refDists[a] < refDists[b] })
		for i := 0; i < wantN; i++ {
			// Ascending order and exact parity with the scalar reference.
			require.Equal(t, refDists[i], gotDists[i], "distance %d", i)
			if i > 0 {
				require.GreaterOrEqual(t, gotDists[i], gotDists[i-1], "distances must be sorted ascending")
			}
		}

		seen := make(map[int64]struct{}, len(gotIDs))
		for i, id := range gotIDs {
			_, dup := seen[id]
			require.False(t, dup, "duplicate id %d in results", id)
			seen[id] = struct{}{}

			vec, ok := ref[id]
			require.True(t, ok, "returned id %d was never added", id)
			require.Equal(t, refSqDist(query, vec), gotDists[i], "id %d distance mismatch", id)
		}

		// --- SearchBatchDistances parity ---
		gotBatch, err := idx.SearchBatchDistances(query, candidates)
		require.NoError(t, err)
		require.Len(t, gotBatch, len(candidates))
		for i, cid := range candidates {
			if vec, ok := ref[int64(cid)]; ok {
				require.Equal(t, refSqDist(query, vec), gotBatch[i], "candidate %d", cid)
			} else {
				require.Equal(t, float32(math.MaxFloat32), gotBatch[i], "missing candidate %d", cid)
			}
		}
	})
}

func FuzzCPUIndexHaversine(f *testing.F) {
	f.Add(float32(0), float32(0), float32(6371000), encodeFuzzFloat32s([]float32{0, 0, 45, 80, -33.5, 89}))
	f.Add(float32(51.5), float32(-0.12), float32(6371000), encodeFuzzFloat32s([]float32{51.5, -0.12, 48.85, 2.35}))
	f.Add(float32(-33.9), float32(89), float32(1e6), encodeFuzzFloat32s([]float32{-33.9, 89, 12.5, -44}))
	f.Add(float32(85), float32(-89), float32(1e7), encodeFuzzFloat32s([]float32{-85, 89, 0, 0, 85, -89, 42.5}))
	f.Add(float32(0), float32(0), float32(0), encodeFuzzFloat32s([]float32{10, 20}))    // zero radius
	f.Add(float32(1000), float32(0), float32(100), encodeFuzzFloat32s([]float32{1, 2})) // out of range: dropped
	f.Add(float32(0), float32(0), float32(-5), encodeFuzzFloat32s([]float32{1, 2}))     // negative radius: dropped
	f.Add(float32(85), float32(89), float32(6371000), []byte{})                         // no points

	f.Fuzz(func(t *testing.T, centerLat, centerLon, radius float32, pointsRaw []byte) {
		// Bound to a geographic domain where the haversine term stays
		// strictly below 1: |lat| <= 85 and |lon| <= 89 (so two points can
		// never be antipodal). Out-of-range input is dropped, not failed.
		if math.IsNaN(float64(centerLat)) || math.IsNaN(float64(centerLon)) || math.IsNaN(float64(radius)) {
			return
		}
		if math.Abs(float64(centerLat)) > 85 || math.Abs(float64(centerLon)) > 89 {
			return
		}
		if radius < 0 || radius > 1e7 {
			return
		}
		if len(pointsRaw) > 128*4 {
			return
		}
		points := decodeFuzzFloat32s(pointsRaw)
		for i, p := range points {
			if math.IsNaN(float64(p)) {
				return
			}
			if i%2 == 0 {
				if math.Abs(float64(p)) > 85 {
					return
				}
			} else if math.Abs(float64(p)) > 89 {
				return
			}
		}

		idx := &CPUIndex{}
		got, err := idx.HaversineSearch(centerLat, centerLon, points, radius)
		require.NoError(t, err)

		count := len(points) / 2
		require.Len(t, got, count)

		maxDist := float64(radius) * math.Pi
		for j := 0; j < count; j++ {
			want := refHaversine(centerLat, centerLon, points[j*2], points[j*2+1], radius)
			require.False(t, math.IsNaN(float64(got[j])), "point %d: distance must not be NaN", j)
			// Parity with the float64 reference, allowing for the float32
			// intermediate rounding done by the implementation.
			diff := math.Abs(float64(got[j]) - want)
			require.LessOrEqual(t, diff, 1e-5*math.Max(1, math.Abs(want)), "point %d: got %v want %v", j, got[j], want)
			require.GreaterOrEqual(t, float64(got[j]), 0.0)
			require.LessOrEqual(t, float64(got[j]), maxDist*(1+1e-5)+1e-5)
		}

		// Symmetry: distance(center, p) == distance(p, center).
		for j := 0; j < count; j++ {
			flipped, err := idx.HaversineSearch(points[j*2], points[j*2+1], []float32{centerLat, centerLon}, radius)
			require.NoError(t, err)
			require.Len(t, flipped, 1)
			diff := math.Abs(float64(flipped[0]) - float64(got[j]))
			require.LessOrEqual(t, diff, 1e-5*math.Max(1, float64(got[j])), "point %d not symmetric", j)
		}

		// Identity: the distance from the center to itself is exactly zero.
		self, err := idx.HaversineSearch(centerLat, centerLon, []float32{centerLat, centerLon}, radius)
		require.NoError(t, err)
		require.Len(t, self, 1)
		require.Equal(t, float32(0), self[0])
	})
}
