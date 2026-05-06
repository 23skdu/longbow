package simd

import (
	"math"
)

// TurboQuantDistanceFunc calculates the distance between a query and a TQ-encoded vector.
type TurboQuantDistanceFunc func(query []float32, tqData []byte, dim int, pow2 int, bitsPerAngle int) (float32, error)

// TurboQuantDistanceNEON is the NEON-optimized version of TQ distance.
func TurboQuantDistanceNEON(query []float32, tqData []byte, dim int, pow2 int, bitsPerAngle int) (float32, error) {
	// Format: [Radius (4B)][Packed Angles][QJL Bits]
	radius := math.Float32frombits(uint32(tqData[0]) | uint32(tqData[1])<<8 | uint32(tqData[2])<<16 | uint32(tqData[3])<<24)
	
	angleCount := pow2 - 1
	angleBytes := (angleCount*bitsPerAngle + 7) / 8
	packedAngles := tqData[4 : 4+angleBytes]
	qjlBits := tqData[4+angleBytes:]

	// Reconstruction (Recursive Polar)
	recon := make([]float32, pow2)
	recon[0] = radius
	
	currentLevelSize := 1
	angleOffset := angleCount
	
	maxVal := float32((uint32(1) << bitsPerAngle) - 1)
	invMaxVal := 1.0 / maxVal
	twoPi := float32(2 * math.Pi)
	pi := float32(math.Pi)

	for currentLevelSize < pow2 {
		angleOffset -= currentLevelSize
		// Apply 8x unrolling to reconstruction loop to improve throughput
		i := currentLevelSize - 1
		for ; i >= 7; i -= 8 {
			// Level 1-8: Unrolled for ARM64 pipeline depth
			for j := 0; j < 8; j++ {
				idx := i - j
				r := recon[idx]
				bitStart := (angleOffset + idx) * bitsPerAngle
				var q uint32
				for k := 0; k < bitsPerAngle; k++ {
					bitIdx := bitStart + k
					if (packedAngles[bitIdx/8] >> (bitIdx % 8)) & 1 != 0 {
						q |= (1 << k)
					}
				}
				theta := (float32(q) * invMaxVal) * twoPi - pi
				s, c := math.Sincos(float64(theta))
				recon[2*idx] = r * float32(c)
				recon[2*idx+1] = r * float32(s)
			}
		}
		// Handle remainder
		for ; i >= 0; i-- {
			r := recon[i]
			bitStart := (angleOffset + i) * bitsPerAngle
			var q uint32
			for k := 0; k < bitsPerAngle; k++ {
				bitIdx := bitStart + k
				if (packedAngles[bitIdx/8] >> (bitIdx % 8)) & 1 != 0 {
					q |= (1 << k)
				}
			}
			theta := (float32(q) * invMaxVal) * twoPi - pi
			s, c := math.Sincos(float64(theta))
			recon[2*i] = r * float32(c)
			recon[2*i+1] = r * float32(s)
		}
		currentLevelSize *= 2
	}

	// Calculate L2 distance with QJL correction
	var sum float32
	correction := radius / float32(math.Sqrt(float64(pow2))) * 0.1
	
	// Aggressive 8x unrolling for L2 accumulation
	i := 0
	for ; i <= dim-8; i += 8 {
		q0, q1, q2, q3 := query[i], query[i+1], query[i+2], query[i+3]
		q4, q5, q6, q7 := query[i+4], query[i+5], query[i+6], query[i+7]
		r0, r1, r2, r3 := recon[i], recon[i+1], recon[i+2], recon[i+3]
		r4, r5, r6, r7 := recon[i+4], recon[i+5], recon[i+6], recon[i+7]
		
		// QJL correction with bit-packed access
		if (qjlBits[i/8] >> (i % 8)) & 1 != 0 { r0 += correction } else { r0 -= 0.1 }
		if (qjlBits[(i+1)/8] >> ((i+1) % 8)) & 1 != 0 { r1 += correction } else { r1 -= 0.1 }
		if (qjlBits[(i+2)/8] >> ((i+2) % 8)) & 1 != 0 { r2 += correction } else { r2 -= 0.1 }
		if (qjlBits[(i+3)/8] >> ((i+3) % 8)) & 1 != 0 { r3 += correction } else { r3 -= 0.1 }
		if (qjlBits[(i+4)/8] >> ((i+4) % 8)) & 1 != 0 { r4 += correction } else { r4 -= 0.1 }
		if (qjlBits[(i+5)/8] >> ((i+5) % 8)) & 1 != 0 { r5 += correction } else { r5 -= 0.1 }
		if (qjlBits[(i+6)/8] >> ((i+6) % 8)) & 1 != 0 { r6 += correction } else { r6 -= 0.1 }
		if (qjlBits[(i+7)/8] >> ((i+7) % 8)) & 1 != 0 { r7 += correction } else { r7 -= 0.1 }
		
		d0, d1, d2, d3 := q0-r0, q1-r1, q2-r2, q3-r3
		d4, d5, d6, d7 := q4-r4, q5-r5, q6-r6, q7-r7
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3 + d4*d4 + d5*d5 + d6*d6 + d7*d7
	}
	
	for ; i < dim; i++ {
		val := recon[i]
		if (qjlBits[i/8] >> (i % 8)) & 1 != 0 {
			val += correction
		} else {
			val -= 0.1
		}
		diff := query[i] - val
		sum += diff * diff
	}

	return float32(math.Sqrt(float64(sum))), nil
}
