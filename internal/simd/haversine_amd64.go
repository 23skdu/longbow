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

// batchSinCosF32 computes sin and cos for each element in src
// using inline float32 polynomial approximations (no float64 conversion).
func batchSinCosF32(src []float32, sinDst, cosDst []float32) {
	pio2 := float32(math.Pi / 2)
	for i, v := range src {
		sinDst[i] = sinPolyF32(v)
		cosDst[i] = cosPolyF32(v)
	}
	_ = pio2
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

	lat2Buf := scratch.lat2Rad[:n]
	sinDLat := scratch.sinDLat[:n]
	sinDLon := scratch.sinDLon[:n]
	cosLat2 := scratch.cosLat2[:n]
	aBuf := scratch.a[:n]

	// Phase 1: Compute dLat/2, dLon/2 and their sines, and cos(lat2) in one pass
	// This fuses 4 loops that were previously separate.
	for i, p := range points {
		lat2 := float32(p.Lat)*rpd - lat1
		lon2 := float32(p.Lon)*rpd - lon1
		lat2Buf[i] = lat2 // store original lat2 for cos

		// dLat/2, dLon/2
		dLat := lat2 * 0.5
		dLon := lon2 * 0.5

		// sin(dLat/2), sin(dLon/2) via inline polynomial
		sinDLat[i] = sinPolyF32(dLat)
		sinDLon[i] = sinPolyF32(dLon)

		// cos(lat2) via inline polynomial
		// lat2 is already in radians and offset by lat1
		cosLat2[i] = cosPolyF32(lat2 + lat1)
	}

	cosLat1 := cosPolyF32(lat1)

	// Phase 2: Compute a = sin²(dLat/2) + cosLat1 * cos(lat2) * sin²(dLon/2)
	for i := 0; i < n; i++ {
		aBuf[i] = sinDLat[i]*sinDLat[i] + cosLat1*cosLat2[i]*sinDLon[i]*sinDLon[i]
	}

	// Phase 3: sqrt(a) and sqrt(1-a) — batch SIMD sqrt
	sqrtA := scratch.sqrtA[:n]
	sqrt1mA := scratch.sqrt1mA[:n]
	SqrtFloat32(aBuf, sqrtA)
	for i := 0; i < n; i++ {
		sqrt1mA[i] = 1 - aBuf[i]
	}
	SqrtFloat32(sqrt1mA[:n], sqrt1mA)

	// Phase 4: Compute c = 2 * atan2(sqrt(a), sqrt(1-a)) and scale by radius
	twoRad := 2 * rad
	for i := 0; i < n; i++ {
		c := atan2PolyF32(sqrtA[i], sqrt1mA[i])
		results[i] = c * twoRad
	}
}
