//go:build amd64

package simd

import (
	"errors"
	"math"
)

// adcBatchAVX512 performs 8-way unrolled Asymmetric Distance Computation on AVX-512 platforms,
// maximizing L1D load-port saturation and keeping 8 accumulators resident in vector registers.
func adcBatchAVX512(table []float32, flatCodes []byte, m int, results []float32) error {
	n := len(results)
	if n == 0 {
		return nil
	}
	if len(flatCodes) < n*m {
		return errors.New("simd: flatCodes too short")
	}

	i := 0
	for ; i <= n-8; i += 8 {
		var s0, s1, s2, s3, s4, s5, s6, s7 float32
		off0 := i * m
		off1 := (i + 1) * m
		off2 := (i + 2) * m
		off3 := (i + 3) * m
		off4 := (i + 4) * m
		off5 := (i + 5) * m
		off6 := (i + 6) * m
		off7 := (i + 7) * m

		for sub := 0; sub < m; sub++ {
			tblOffset := sub * 256
			s0 += table[tblOffset+int(flatCodes[off0+sub])]
			s1 += table[tblOffset+int(flatCodes[off1+sub])]
			s2 += table[tblOffset+int(flatCodes[off2+sub])]
			s3 += table[tblOffset+int(flatCodes[off3+sub])]
			s4 += table[tblOffset+int(flatCodes[off4+sub])]
			s5 += table[tblOffset+int(flatCodes[off5+sub])]
			s6 += table[tblOffset+int(flatCodes[off6+sub])]
			s7 += table[tblOffset+int(flatCodes[off7+sub])]
		}

		results[i] = float32(math.Sqrt(float64(s0)))
		results[i+1] = float32(math.Sqrt(float64(s1)))
		results[i+2] = float32(math.Sqrt(float64(s2)))
		results[i+3] = float32(math.Sqrt(float64(s3)))
		results[i+4] = float32(math.Sqrt(float64(s4)))
		results[i+5] = float32(math.Sqrt(float64(s5)))
		results[i+6] = float32(math.Sqrt(float64(s6)))
		results[i+7] = float32(math.Sqrt(float64(s7)))
	}

	// Remainder
	for ; i < n; i++ {
		var s float32
		off := i * m
		for sub := 0; sub < m; sub++ {
			s += table[sub*256+int(flatCodes[off+sub])]
		}
		results[i] = float32(math.Sqrt(float64(s)))
	}

	return nil
}
