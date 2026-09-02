package simd

import (
	"math"
	"sync"
)

// tqScratchPool provides reusable buffers for TurboQuant distance computation.
// Each goroutine gets its own buffer pair, eliminating per-call heap allocations.
// Maximum pow2 = 4096 (next power of 2 for dim=3072), so buffer caps handle the max.
var tqScratchPool = sync.Pool{
	New: func() any {
		return &tqScratchBuf{
			recon:   make([]float32, 4096),
			indices: make([]byte, 4096),
		}
	},
}

type tqScratchBuf struct {
	recon   []float32
	indices []byte
}

// TurboQuantDistanceFunc calculates the distance between a query and a TQ-encoded vector.
type TurboQuantDistanceFunc func(query []float32, tqData []byte, dim int, pow2 int, bitsPerAngle int) (float32, error)

// TurboQuantPolarTransform calculates the recursive polar transform for a vector.
// src: input vector (length n, power of 2)
// dstRadii: intermediate radii (length n/2)
// dstAngles: extracted angles (length n/2)
type TurboQuantPolarTransformFunc func(src []float32, dstRadii []float32, dstAngles []float32)

var (
	tqLookup2 []float32
	tqLookup4 []float32
	tqLookup8 []float32
)

func init() {
	tqLookup2 = make([]float32, 4*2)
	for i := 0; i < 4; i++ {
		theta := (float32(i)/3.0)*2*math.Pi - math.Pi
		s, c := math.Sincos(float64(theta))
		tqLookup2[2*i] = float32(c)
		tqLookup2[2*i+1] = float32(s)
	}
	tqLookup4 = make([]float32, 16*2)
	for i := 0; i < 16; i++ {
		theta := (float32(i)/15.0)*2*math.Pi - math.Pi
		s, c := math.Sincos(float64(theta))
		tqLookup4[2*i] = float32(c)
		tqLookup4[2*i+1] = float32(s)
	}
	tqLookup8 = make([]float32, 256*2)
	for i := 0; i < 256; i++ {
		theta := (float32(i)/255.0)*2*math.Pi - math.Pi
		s, c := math.Sincos(float64(theta))
		tqLookup8[2*i] = float32(c)
		tqLookup8[2*i+1] = float32(s)
	}
}

// TurboQuantDistanceNEON is the NEON-optimized version of TQ distance.
func TurboQuantDistanceNEON(query []float32, tqData []byte, dim int, pow2 int, bitsPerAngle int) (float32, error) {
	buf := tqScratchPool.Get().(*tqScratchBuf)
	defer tqScratchPool.Put(buf)
	return turboQuantDistanceNEONScratch(query, tqData, dim, pow2, bitsPerAngle, buf.recon, buf.indices)
}

func turboQuantDistanceNEONScratch(query []float32, tqData []byte, dim int, pow2 int, bitsPerAngle int, recon []float32, qIndices []byte) (float32, error) {
	radius := math.Float32frombits(uint32(tqData[0]) | uint32(tqData[1])<<8 | uint32(tqData[2])<<16 | uint32(tqData[3])<<24)

	angleCount := pow2 - 1
	angleBytes := (angleCount*bitsPerAngle + 7) / 8
	packedAngles := tqData[4 : 4+angleBytes]
	qjlBits := tqData[4+angleBytes:]

	if cap(qIndices) < angleCount {
		qIndices = make([]byte, angleCount)
	}
	qIndices = qIndices[:angleCount]

	switch bitsPerAngle {
	case 8:
		copy(qIndices, packedAngles)
	case 4:
		for i := 0; i < angleCount/2; i++ {
			b := packedAngles[i]
			qIndices[2*i] = b & 0x0F
			qIndices[2*i+1] = b >> 4
		}
		if angleCount%2 != 0 {
			qIndices[angleCount-1] = packedAngles[angleCount/2] & 0x0F
		}
	case 2:
		for i := 0; i < angleCount/4; i++ {
			b := packedAngles[i]
			qIndices[4*i] = b & 0x03
			qIndices[4*i+1] = (b >> 2) & 0x03
			qIndices[4*i+2] = (b >> 4) & 0x03
			qIndices[4*i+3] = (b >> 6) & 0x03
		}
	default:
		return TurboQuantDistanceGeneric(query, tqData, dim, pow2, bitsPerAngle)
	}

	if cap(recon) < pow2 {
		recon = make([]float32, pow2)
	}
	recon = recon[:pow2]
	recon[0] = radius

	var lookup []float32
	switch bitsPerAngle {
	case 2:
		lookup = tqLookup2
	case 4:
		lookup = tqLookup4
	case 8:
		lookup = tqLookup8
	}

	currentLevelSize := 1
	angleOffset := angleCount
	for currentLevelSize < pow2 {
		angleOffset -= currentLevelSize
		for i := currentLevelSize - 1; i >= 0; i-- {
			r := recon[i]
			q := qIndices[angleOffset+i]
			c := lookup[2*int(q)]
			s := lookup[2*int(q)+1]
			recon[2*i] = r * c
			recon[2*i+1] = r * s
		}
		currentLevelSize *= 2
	}

	correction := radius / float32(math.Sqrt(float64(pow2))) * 0.1
	sum := l2SquaredTQCorrectionGeneric(query, recon, qjlBits, correction, dim)

	return float32(math.Sqrt(float64(sum))), nil
}

func TurboQuantDistanceGeneric(query []float32, tqData []byte, dim int, pow2 int, bitsPerAngle int) (float32, error) {
	if len(tqData) < 4 || bitsPerAngle <= 0 || bitsPerAngle > 8 {
		return 0, nil
	}
	radius := math.Float32frombits(uint32(tqData[0]) | uint32(tqData[1])<<8 | uint32(tqData[2])<<16 | uint32(tqData[3])<<24)

	angleCount := pow2 - 1
	angleBytes := (angleCount*bitsPerAngle + 7) / 8
	if len(tqData) < 4+angleBytes {
		return 0, nil
	}
	packedAngles := tqData[4 : 4+angleBytes]
	qjlBits := tqData[4+angleBytes:]

	maxVal := float32((uint32(1) << bitsPerAngle) - 1)
	qIndices := make([]byte, angleCount)
	var currentBit int
	for i := range qIndices {
		var q uint32
		for k := 0; k < bitsPerAngle; k++ {
			if (packedAngles[currentBit/8] & (byte(1) << (currentBit % 8))) != 0 {
				q |= (uint32(1) << k)
			}
			currentBit++
		}
		qIndices[i] = byte(q)
	}

	recon := make([]float32, pow2)
	recon[0] = radius

	currentLevelSize := 1
	angleOffset := angleCount
	for currentLevelSize < pow2 {
		angleOffset -= currentLevelSize
		for i := currentLevelSize - 1; i >= 0; i-- {
			r := recon[i]
			q := qIndices[angleOffset+i]
			theta := (float32(q)/maxVal)*2*math.Pi - math.Pi
			s, c := math.Sincos(float64(theta))
			recon[2*i] = r * float32(c)
			recon[2*i+1] = r * float32(s)
		}
		currentLevelSize *= 2
	}

	correction := radius / float32(math.Sqrt(float64(pow2))) * 0.1
	sum := l2SquaredTQCorrectionGeneric(query, recon, qjlBits, correction, dim)
	return float32(math.Sqrt(float64(sum))), nil
}

func TurboQuantDistanceAVX512(query []float32, tqData []byte, dim int, pow2 int, bitsPerAngle int) (float32, error) {
	// Fallback to NEON-optimized Go version for now until AVX512 assembly is finalized
	return TurboQuantDistanceNEON(query, tqData, dim, pow2, bitsPerAngle)
}

func TurboQuantDistanceAVX2(query []float32, tqData []byte, dim int, pow2 int, bitsPerAngle int) (float32, error) {
	buf := tqScratchPool.Get().(*tqScratchBuf)
	defer tqScratchPool.Put(buf)
	return turboQuantDistanceAVX2Scratch(query, tqData, dim, pow2, bitsPerAngle, buf.recon, buf.indices)
}

// turboQuantDistanceAVX2Scratch is the allocation-free variant that uses caller-provided scratch buffers.
// Pass nil for scratch buffers to allocate on first call (buffers are grown as needed).
func turboQuantDistanceAVX2Scratch(query []float32, tqData []byte, dim int, pow2 int, bitsPerAngle int, recon []float32, qIndices []byte) (float32, error) {
	radius := math.Float32frombits(uint32(tqData[0]) | uint32(tqData[1])<<8 | uint32(tqData[2])<<16 | uint32(tqData[3])<<24)

	angleCount := pow2 - 1
	angleBytes := (angleCount*bitsPerAngle + 7) / 8
	packedAngles := tqData[4 : 4+angleBytes]
	qjlBits := tqData[4+angleBytes:]

	// Reuse scratch buffers, grow if needed
	if cap(qIndices) < angleCount {
		qIndices = make([]byte, angleCount)
	}
	qIndices = qIndices[:angleCount]

	// Unpack angles into byte indices
	switch bitsPerAngle {
	case 8:
		copy(qIndices, packedAngles)
	case 4:
		for i := 0; i < angleCount/2; i++ {
			b := packedAngles[i]
			qIndices[2*i] = b & 0x0F
			qIndices[2*i+1] = b >> 4
		}
		if angleCount%2 != 0 {
			qIndices[angleCount-1] = packedAngles[angleCount/2] & 0x0F
		}
	case 2:
		for i := 0; i < angleCount/4; i++ {
			b := packedAngles[i]
			qIndices[4*i] = b & 0x03
			qIndices[4*i+1] = (b >> 2) & 0x03
			qIndices[4*i+2] = (b >> 4) & 0x03
			qIndices[4*i+3] = (b >> 6) & 0x03
		}
	default:
		return TurboQuantDistanceGeneric(query, tqData, dim, pow2, bitsPerAngle)
	}

	// Reconstruct vector via recursive polar transform
	var lookup []float32
	switch bitsPerAngle {
	case 2:
		lookup = tqLookup2
	case 4:
		lookup = tqLookup4
	case 8:
		lookup = tqLookup8
	}

	if cap(recon) < pow2 {
		recon = make([]float32, pow2)
	}
	recon = recon[:pow2]
	recon[0] = radius

	currentLevelSize := 1
	angleOffset := angleCount
	for currentLevelSize < pow2 {
		angleOffset -= currentLevelSize
		for i := currentLevelSize - 1; i >= 0; i-- {
			r := recon[i]
			q := qIndices[angleOffset+i]
			c := lookup[2*int(q)]
			s := lookup[2*int(q)+1]
			recon[2*i] = r * c
			recon[2*i+1] = r * s
		}
		currentLevelSize *= 2
	}

	// Apply QJL correction and compute L2 using AVX2 float32 kernel
	correction := radius / float32(math.Sqrt(float64(pow2))) * 0.1
	for i := range recon {
		if (qjlBits[i/8]>>uint(i%8))&1 != 0 {
			recon[i] += correction
		} else {
			recon[i] -= correction
		}
	}

	// Use AVX2 float32 L2 kernel on the corrected reconstruction
	sum, err := l2SquaredAVX2(query[:dim], recon[:dim])
	if err != nil {
		return 0, err
	}
	return float32(math.Sqrt(float64(sum))), nil
}

// TurboQuantPolarTransformNEON is the NEON-optimized version of the polar transform stage.
func TurboQuantPolarTransformNEON(src []float32, dstRadii []float32, dstAngles []float32) {
	n := len(src)
	halfN := n / 2
	for i := 0; i < halfN; i++ {
		x := src[2*i]
		y := src[2*i+1]
		dstRadii[i] = float32(math.Sqrt(float64(x*x + y*y)))
		dstAngles[i] = float32(math.Atan2(float64(y), float64(x)))
	}
}

// TurboQuantPolarTransformAVX2 is the AVX2-optimized version of the polar transform stage.
func TurboQuantPolarTransformAVX2(src []float32, dstRadii []float32, dstAngles []float32) {
	TurboQuantPolarTransformNEON(src, dstRadii, dstAngles)
}

// GetTurboQuantPolarTransformFunc returns the optimal TQ polar transform function for the current CPU.
func GetTurboQuantPolarTransformFunc() TurboQuantPolarTransformFunc {
	if features.HasAVX2 {
		return TurboQuantPolarTransformAVX2
	}
	return TurboQuantPolarTransformNEON
}

// GetTurboQuantDistanceFunc returns the optimal TQ distance function for the current CPU.
func GetTurboQuantDistanceFunc() TurboQuantDistanceFunc {
	if features.HasAVX2 {
		return TurboQuantDistanceAVX2
	}
	if features.HasAVX512 {
		return TurboQuantDistanceAVX512
	}
	return TurboQuantDistanceNEON
}
