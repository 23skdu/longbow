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

// tqLUTMinBits and tqLUTMaxBits bound the angle depths covered by tqPolarLUT.
const (
	tqLUTMinBits = 1
	tqLUTMaxBits = 8
	// tqLUTEntries is sum(2^bits) for bits in [tqLUTMinBits, tqLUTMaxBits] = 510,
	// i.e. 510 interleaved (cos, sin) pairs.
	tqLUTEntries = 510
)

// tqPolarLUT holds sin/cos for every quantized angle code of every supported
// bit depth, interleaved as [cos(q), sin(q)]. Depth b owns the pairs
// [tqLUTBase(b), tqLUTBase(b)+2^b) where tqLUTBase(b) = sum(2^k, k<b) = 2^b-2.
// The whole table is 4080 bytes and is built once in init, before any
// goroutine can read it, so lookups are race-free and stay resident in L1.
var tqPolarLUT [2 * tqLUTEntries]float32

// tqLUTBase returns the index of the first (cos, sin) pair of a bit depth.
func tqLUTBase(bitsPerAngle int) int {
	return (1 << bitsPerAngle) - 2
}

// tqLUTFor returns the interleaved cos/sin table for bitsPerAngle, or nil when
// the depth is outside [tqLUTMinBits, tqLUTMaxBits].
func tqLUTFor(bitsPerAngle int) []float32 {
	if bitsPerAngle < tqLUTMinBits || bitsPerAngle > tqLUTMaxBits {
		return nil
	}
	base := tqLUTBase(bitsPerAngle)
	return tqPolarLUT[2*base : 2*(base+(1<<bitsPerAngle))]
}

// Precomputed trigonometric distance and inner product matrices for quantized codebooks
// Eliminates trigonometric calls and dequantization stalls.
var (
	tqAngleInnerProductMatrix2 [4 * 4]float32
	tqAngleDistanceMatrix2     [4 * 4]float32

	tqAngleInnerProductMatrix4 [16 * 16]float32
	tqAngleDistanceMatrix4     [16 * 16]float32

	tqAngleInnerProductMatrix8 [256 * 256]float32
	tqAngleDistanceMatrix8     [256 * 256]float32
)

// GetTurboQuantAngleInnerProductMatrix returns the precomputed inner product matrix for bits (2, 4, 8).
func GetTurboQuantAngleInnerProductMatrix(bits int) []float32 {
	switch bits {
	case 2:
		return tqAngleInnerProductMatrix2[:]
	case 4:
		return tqAngleInnerProductMatrix4[:]
	case 8:
		return tqAngleInnerProductMatrix8[:]
	default:
		return nil
	}
}

// GetTurboQuantAngleDistanceMatrix returns the precomputed distance matrix for bits (2, 4, 8).
func GetTurboQuantAngleDistanceMatrix(bits int) []float32 {
	switch bits {
	case 2:
		return tqAngleDistanceMatrix2[:]
	case 4:
		return tqAngleDistanceMatrix4[:]
	case 8:
		return tqAngleDistanceMatrix8[:]
	default:
		return nil
	}
}

// TurboQuantAngleDistance returns the precomputed squared distance between two quantized angle codes.
func TurboQuantAngleDistance(bits int, q1, q2 byte) float32 {
	switch bits {
	case 2:
		return tqAngleDistanceMatrix2[(int(q1)&3)*4+(int(q2)&3)]
	case 4:
		return tqAngleDistanceMatrix4[(int(q1)&15)*16+(int(q2)&15)]
	case 8:
		return tqAngleDistanceMatrix8[int(q1)*256+int(q2)]
	default:
		return 0
	}
}

// TurboQuantAngleInnerProduct returns the precomputed inner product between two quantized angle codes.
func TurboQuantAngleInnerProduct(bits int, q1, q2 byte) float32 {
	switch bits {
	case 2:
		return tqAngleInnerProductMatrix2[(int(q1)&3)*4+(int(q2)&3)]
	case 4:
		return tqAngleInnerProductMatrix4[(int(q1)&15)*16+(int(q2)&15)]
	case 8:
		return tqAngleInnerProductMatrix8[int(q1)*256+int(q2)]
	default:
		return 0
	}
}

func init() {
	for bits := tqLUTMinBits; bits <= tqLUTMaxBits; bits++ {
		n := 1 << bits
		base := tqLUTBase(bits)
		maxVal := float32(n - 1)
		for i := 0; i < n; i++ {
			// Same expression as the per-element math.Sincos call this table
			// replaces, so every entry is bit-identical to it.
			theta := (float32(i)/maxVal)*2*math.Pi - math.Pi
			s, c := math.Sincos(float64(theta))
			tqPolarLUT[2*(base+i)] = float32(c)
			tqPolarLUT[2*(base+i)+1] = float32(s)
		}
	}

	// Precompute trigonometric inner product and distance matrices for 2, 4, 8 bits
	for _, bits := range []int{2, 4, 8} {
		n := 1 << bits
		base := tqLUTBase(bits)
		for i := 0; i < n; i++ {
			c1 := tqPolarLUT[2*(base+i)]
			s1 := tqPolarLUT[2*(base+i)+1]
			for j := 0; j < n; j++ {
				c2 := tqPolarLUT[2*(base+j)]
				s2 := tqPolarLUT[2*(base+j)+1]
				ip := c1*c2 + s1*s2
				dist := (c1-c2)*(c1-c2) + (s1-s2)*(s1-s2)
				if i == j {
					dist = 0
					ip = 1.0
				}
				switch bits {
				case 2:
					tqAngleInnerProductMatrix2[i*4+j] = ip
					tqAngleDistanceMatrix2[i*4+j] = dist
				case 4:
					tqAngleInnerProductMatrix4[i*16+j] = ip
					tqAngleDistanceMatrix4[i*16+j] = dist
				case 8:
					tqAngleInnerProductMatrix8[i*256+j] = ip
					tqAngleDistanceMatrix8[i*256+j] = dist
				}
			}
		}
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
		i := 0
		for ; i+4 <= angleCount; i += 4 {
			b := packedAngles[i/4]
			qIndices[i] = b & 0x03
			qIndices[i+1] = (b >> 2) & 0x03
			qIndices[i+2] = (b >> 4) & 0x03
			qIndices[i+3] = b >> 6
		}
		// angleCount is pow2-1 and therefore odd, so the vector loop always
		// leaves 1-3 codes behind. They must be written too: qIndices is pooled
		// scratch and would otherwise feed a previous request's codes into this
		// reconstruction.
		for ; i < angleCount; i++ {
			qIndices[i] = (packedAngles[i/4] >> (uint(i%4) * 2)) & 0x03
		}
	default:
		return TurboQuantDistanceGeneric(query, tqData, dim, pow2, bitsPerAngle)
	}

	if cap(recon) < pow2 {
		recon = make([]float32, pow2)
	}
	recon = recon[:pow2]
	recon[0] = radius

	lookup := tqLUTFor(bitsPerAngle)

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

	lookup := tqLUTFor(bitsPerAngle)
	if lookup == nil {
		return 0, nil
	}
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

func TurboQuantDistanceAVX512(query []float32, tqData []byte, dim int, pow2 int, bitsPerAngle int) (float32, error) {
	buf := tqScratchPool.Get().(*tqScratchBuf)
	defer tqScratchPool.Put(buf)
	return turboQuantDistanceAVX2Scratch(query, tqData, dim, pow2, bitsPerAngle, buf.recon, buf.indices)
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
		i := 0
		for ; i+4 <= angleCount; i += 4 {
			b := packedAngles[i/4]
			qIndices[i] = b & 0x03
			qIndices[i+1] = (b >> 2) & 0x03
			qIndices[i+2] = (b >> 4) & 0x03
			qIndices[i+3] = b >> 6
		}
		// angleCount is pow2-1 and therefore odd, so the vector loop always
		// leaves 1-3 codes behind. They must be written too: qIndices is pooled
		// scratch and would otherwise feed a previous request's codes into this
		// reconstruction.
		for ; i < angleCount; i++ {
			qIndices[i] = (packedAngles[i/4] >> (uint(i%4) * 2)) & 0x03
		}
	default:
		return TurboQuantDistanceGeneric(query, tqData, dim, pow2, bitsPerAngle)
	}

	// Reconstruct vector via recursive polar transform
	lookup := tqLUTFor(bitsPerAngle)

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

	// Apply QJL correction.
	//
	// The sign of the correction comes from one data-dependent bit per element,
	// so this used to be an `if/else` that mispredicts on roughly every other
	// element. Selecting the sign arithmetically removes the branch and is
	// bit-exact: `correction * -1` is an exact IEEE negation, so
	// `correction * (1 - 2*bit)` is precisely `-correction` when the bit is set
	// and exactly `+correction` when it is clear. Bytes are read once per eight
	// elements, which is what the packed layout gives us.
	//
	// Measured on this host at dim=768, bits=4: 2,905 -> 2,721 ns/op (-6%). The
	// loop is load-modify-store bound rather than branch bound at this width, so
	// the branch was not the dominant term it looked like in the profile; the
	// remaining cost is the scalar polar reconstruction above, which needs an
	// AVX2 kernel to move.
	correction := radius / float32(math.Sqrt(float64(pow2))) * 0.1
	tqApplyQJLCorrection(recon, qjlBits, correction)

	// Use AVX-512 / AVX2 float32 L2 kernel on the corrected reconstruction
	var sum float32
	var err error
	if features.HasAVX512 {
		sum, err = l2SquaredAVX512(query[:dim], recon[:dim])
	} else {
		sum, err = l2SquaredAVX2(query[:dim], recon[:dim])
	}
	if err != nil {
		return 0, err
	}
	return float32(math.Sqrt(float64(sum))), nil
}

// tqApplyQJLCorrection adds correction to recon[i] when bit i of qjlBits is set
// and subtracts it otherwise, branchlessly.
//
// The branchless form is exact, not an approximation: `correction * -1` is an
// IEEE-754 negation of the same magnitude, and `correction * 1` returns the
// identical value, so every element receives the same float32 it received from
// the `if/else` this replaced. Only the control flow changed.
func tqApplyQJLCorrection(recon []float32, qjlBits []byte, correction float32) {
	i := 0
	for ; i+8 <= len(recon); i += 8 {
		bits := qjlBits[i>>3]
		for j := 0; j < 8; j++ {
			b := (bits >> uint(j)) & 1
			recon[i+j] += correction * (1 - 2*float32(b))
		}
	}
	if i < len(recon) {
		bits := qjlBits[i>>3]
		for ; i < len(recon); i++ {
			b := (bits >> (uint(i) & 7)) & 1
			recon[i] += correction * (1 - 2*float32(b))
		}
	}
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

// cachedTQDistFunc is the cached TQ distance function, resolved once at init time.
var cachedTQDistFunc TurboQuantDistanceFunc

func init() {
	if features.HasAVX512 {
		cachedTQDistFunc = TurboQuantDistanceAVX512
	} else if features.HasAVX2 {
		cachedTQDistFunc = TurboQuantDistanceAVX2
	} else {
		cachedTQDistFunc = TurboQuantDistanceNEON
	}
}

// GetTurboQuantDistanceFunc returns the optimal TQ distance function for the current CPU.
// The result is cached at init time to avoid repeated feature detection in the hot path.
func GetTurboQuantDistanceFunc() TurboQuantDistanceFunc {
	return cachedTQDistFunc
}
