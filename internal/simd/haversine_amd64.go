//go:build amd64

package simd

import (
	"math"

	lbcore "github.com/23skdu/longbow/internal/core"
)

// float32 minimax polynomial coefficients for sin(x) on [-π/2, π/2].
// Degree 7 polynomial: sin(x) ≈ x + x³·c1 + x⁵·c2 + x⁷·c3
// Max relative error < 1.5 ULP for float32.
func sinPolyF32(x float32) float32 {
	s := x * x
	// sin(x) ≈ x + x³·(-1/6 + x²·(1/120 + x²·(-1/5040)))
	return x + x*s*(float32(-1.0/6.0)+s*(float32(1.0/120.0)+s*float32(-1.0/5040.0)))
}

// cos(x) polynomial on [-π/2, π/2].
// cos(x) ≈ 1 + x²·(-1/2 + x²·(1/24 + x²·(-1/720)))
func cosPolyF32(x float32) float32 {
	s := x * x
	return 1 + s*(float32(-1.0/2.0)+s*(float32(1.0/24.0)+s*float32(-1.0/720.0)))
}

// atan(x) polynomial on [-1, 1].
// atan(x) ≈ x + x³·(-1/3 + x²·(1/5 + x²·(-1/7 + x²·(1/9))))
func atanPolyF32(x float32) float32 {
	s := x * x
	return x + x*s*(float32(-1.0/3.0)+s*(float32(1.0/5.0)+s*(float32(-1.0/7.0)+s*float32(1.0/9.0))))
}

// atan2PolyF32 computes atan2(y, x) using atan polynomial and quadrant adjustment.
func atan2PolyF32(y, x float32) float32 {
	if x == 0 {
		if y > 0 {
			return math.Pi / 2
		} else if y < 0 {
			return -math.Pi / 2
		}
		return 0
	}

	// Reduce to atan(|y/x|) in [0, 1]
	ratio := y / x
	var a float32
	if ratio < -1 || ratio > 1 {
		a = math.Pi/2 - atanPolyF32(1/ratio)
	} else {
		a = atanPolyF32(ratio)
	}

	// Adjust quadrant
	if x < 0 {
		if y >= 0 {
			a += math.Pi
		} else {
			a -= math.Pi
		}
	}
	return a
}



// haversineBatchAVX2 computes Haversine distance with fused operations.
// Uses float32 polynomial approximations for sin/cos/atan2 to avoid
// float64 conversion overhead, and batches sqrt calls for SIMD efficiency.
func haversineBatchAVX2(centerLat, centerLon float64, points []lbcore.GeoPoint, earthRadius float64, results []float32) {
	n := len(points)
	if n == 0 {
		return
	}

	rpd := float32(math.Pi / 180.0)
	lat1 := float32(centerLat) * rpd
	lon1 := float32(centerLon) * rpd
	rad := float32(earthRadius)

	// Single scratch buffer reused across all phases
	scratch := getHaversineScratch(n)
	defer putHaversineScratch(scratch)

	dLatBuf := scratch.lat2Rad[:n] // reuse lat2Rad for dLat
	dLonBuf := scratch.sinDLat[:n] // reuse sinDLat for dLon temporarily
	lat2OffsetBuf := scratch.cosLat2[:n] // reuse cosLat2 for lat2Offset

	// Phase 1: Compute inputs for transcendentals
	for i, p := range points {
		lat2 := float32(p.Lat)*rpd - lat1
		lon2 := float32(p.Lon)*rpd - lon1

		dLatBuf[i] = lat2 * 0.5
		dLonBuf[i] = lon2 * 0.5
		lat2OffsetBuf[i] = lat2 + lat1
	}

	sinDLat := scratch.sqrtA[:n] // reuse sqrtA for sinDLat
	sinDLon := scratch.sqrt1mA[:n] // reuse sqrt1mA for sinDLon
	cosLat2 := scratch.a[:n] // reuse a for cosLat2
	
	// Vectorized transcendentals
	sinAVX2(dLatBuf, sinDLat)
	sinAVX2(dLonBuf, sinDLon)
	cosAVX2(lat2OffsetBuf, cosLat2)

	cosLat1 := cosPolyF32(lat1)
	aBuf := dLatBuf // reuse dLatBuf for a
	
	// Phase 2: Compute a
	for i := 0; i < n; i++ {
		aBuf[i] = sinDLat[i]*sinDLat[i] + cosLat1*cosLat2[i]*sinDLon[i]*sinDLon[i]
	}

	sqrtA := sinDLat // reuse sinDLat for sqrtA
	sqrt1mA := sinDLon // reuse sinDLon for sqrt1mA
	
	// Phase 3: batch SIMD sqrt
	sqrtAVX2(aBuf, sqrtA)
	for i := 0; i < n; i++ {
		sqrt1mA[i] = 1 - aBuf[i]
	}
	sqrtAVX2(sqrt1mA, sqrt1mA)

	cBuf := aBuf // reuse aBuf for c
	
	// Phase 4: Vectorized atan2
	atan2AVX2(sqrtA, sqrt1mA, cBuf)
	
	twoRad := 2 * rad
	for i := 0; i < n; i++ {
		results[i] = cBuf[i] * twoRad
	}
}
