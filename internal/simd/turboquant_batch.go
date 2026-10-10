package simd

import (
	"errors"
	"math"
	"sync"
)

// Batched TurboQuant distance.
//
// R24 asks for a batched TurboQuant kernel for searchLayer to dispatch to, and
// there was none: GetTurboQuantDistanceFunc returns a single-vector function,
// and euclideanDistanceBatch4Way - the 4-way path the float32 computers use -
// takes [][]float32, not packed codes. Every distance in a TurboQuant search
// went through one call per candidate.
//
// Why this kernel is built the way it is.
//
// The obvious design is a 4-way vertical L2 kernel over four reconstructed
// vectors, which is what the float32 path does. That is not usable here.
// TestTQComputeBatchMatchesPerCandidate asserts dst[i] != want on the raw
// float32, so this kernel has to be bit-identical to the single-vector path,
// and a vertical kernel accumulates its four lanes in a different order from the
// horizontal reduction inside l2SquaredAVX2. Any such kernel would be a
// different function, not a faster one, and would silently change which
// neighbour wins a tie.
//
// What the profile actually says is where the time goes. The most recent
// decomposition (docs/performance.md §2) attributes 47.8% of distance
// evaluation to the recursive polar reconstruction, 28.5% to the QJL sign
// correction, 12.9% to angle unpacking and under 5% to the SIMD L2 kernel
// itself. The first three are scalar, and the first is a chain of dependent
// multiplies: recon[2i] and recon[2i+1] cannot be computed until recon[i] is
// known, so each level of the transform costs its full multiplication latency
// with nothing else to fill it.
//
// That is a latency problem, not a throughput problem, and the fix for a
// latency problem is to give the machine independent work to overlap. So this
// kernel decodes four candidates through the reconstruction together: the four
// chains are independent, so their latencies overlap, and each candidate
// performs exactly the same operations in exactly the same order as it would
// alone. The L2 step stays per candidate, on the same kernel as before, for the
// bit-identity reason above.
//
// The pool round trip and the tqLUTFor lookup are hoisted out of the per-
// candidate path for the same reason: they were once per candidate and are now
// once per block.

// tqBatchWidth is the number of candidates decoded together. Four keeps the
// four reconstruction chains inside the L1-resident working set at the widths
// that matter (4 x 128 float32 is 2 KiB of recon) and matches the width of the
// vertical kernels elsewhere in the tree, so the two can be composed later if
// the bit-identity constraint is ever relaxed.
const tqBatchWidth = 4

// tqBatchScratchBuf holds the per-candidate scratch for one block. recon is
// laid out as tqBatchWidth consecutive pow2-length runs and indices likewise, so
// one allocation covers the whole block.
type tqBatchScratchBuf struct {
	recon   []float32
	indices []byte
	radius  [tqBatchWidth]float32
}

var tqBatchScratchPool = sync.Pool{
	New: func() any {
		// pow2 tops out at 4096 (the next power of two above dim=3072).
		return &tqBatchScratchBuf{
			recon:   make([]float32, tqBatchWidth*4096),
			indices: make([]byte, tqBatchWidth*4096),
		}
	},
}

// TurboQuantDistanceBatch computes the distance from one query to each of n
// TQ-encoded vectors, writing into dst, using the optimal SIMD kernel for the CPU.
//
// dst must have room for n float32. codes[i] is one vector's packed payload.
// Every element of dst is bit-identical to what TurboQuantDistanceGeneric,
// TurboQuantDistanceAVX2, TurboQuantDistanceAVX512 or TurboQuantDistanceNEON
// would have produced for the same input, which is what lets a batched caller
// be substituted for the single-vector one without changing a search result.
func TurboQuantDistanceBatch(query []float32, codes [][]byte, dst []float32, dim, pow2, bitsPerAngle int) error {
	if turboQuantDistanceBatchImpl != nil {
		return turboQuantDistanceBatchImpl(query, codes, dst, dim, pow2, bitsPerAngle)
	}
	if features.HasAVX512 {
		return turboQuantDistanceBatchAVX512(query, codes, dst, dim, pow2, bitsPerAngle)
	} else if features.HasAVX2 {
		return turboQuantDistanceBatchAVX2(query, codes, dst, dim, pow2, bitsPerAngle)
	} else if features.HasNEON {
		return turboQuantDistanceBatchNEON(query, codes, dst, dim, pow2, bitsPerAngle)
	}
	return turboQuantDistanceBatchGeneric(query, codes, dst, dim, pow2, bitsPerAngle)
}

// GetTurboQuantDistanceBatchFunc returns the optimal batched TQ distance function for the current CPU.
func GetTurboQuantDistanceBatchFunc() TurboQuantDistanceBatchFunc {
	if turboQuantDistanceBatchImpl != nil {
		return turboQuantDistanceBatchImpl
	}
	return TurboQuantDistanceBatch
}

func turboQuantDistanceBatchAVX512(query []float32, codes [][]byte, dst []float32, dim, pow2, bitsPerAngle int) error {
	return turboQuantDistanceBatchWithL2(query, codes, dst, dim, pow2, bitsPerAngle, l2SquaredAVX512)
}

func turboQuantDistanceBatchAVX2(query []float32, codes [][]byte, dst []float32, dim, pow2, bitsPerAngle int) error {
	return turboQuantDistanceBatchWithL2(query, codes, dst, dim, pow2, bitsPerAngle, l2SquaredAVX2)
}

func turboQuantDistanceBatchNEON(query []float32, codes [][]byte, dst []float32, dim, pow2, bitsPerAngle int) error {
	return turboQuantDistanceBatchWithL2(query, codes, dst, dim, pow2, bitsPerAngle, nil)
}

func turboQuantDistanceBatchGeneric(query []float32, codes [][]byte, dst []float32, dim, pow2, bitsPerAngle int) error {
	return turboQuantDistanceBatchWithL2(query, codes, dst, dim, pow2, bitsPerAngle, nil)
}

func turboQuantDistanceBatchWithL2(query []float32, codes [][]byte, dst []float32, dim, pow2, bitsPerAngle int, l2 distanceFunc) error {
	n := len(codes)
	if n == 0 {
		return nil
	}
	if len(dst) < n {
		return errors.New("simd: turboquant batch destination too small")
	}
	if dim <= 0 || pow2 <= 1 || (bitsPerAngle != 2 && bitsPerAngle != 4 && bitsPerAngle != 8) {
		return turboQuantDistanceBatchFallback(query, codes, dst, dim, pow2, bitsPerAngle)
	}
	lookup := tqLUTFor(bitsPerAngle)
	if lookup == nil {
		return turboQuantDistanceBatchFallback(query, codes, dst, dim, pow2, bitsPerAngle)
	}

	angleCount := pow2 - 1
	angleBytes := (angleCount*bitsPerAngle + 7) / 8
	needRecon := tqBatchWidth * pow2
	needIdx := tqBatchWidth * angleCount

	buf := tqBatchScratchPool.Get().(*tqBatchScratchBuf)
	defer tqBatchScratchPool.Put(buf)
	if cap(buf.recon) < needRecon {
		buf.recon = make([]float32, needRecon)
	}
	if cap(buf.indices) < needIdx {
		buf.indices = make([]byte, needIdx)
	}
	recon := buf.recon[:needRecon]
	indices := buf.indices[:needIdx]

	for base := 0; base < n; base += tqBatchWidth {
		m := n - base
		if m > tqBatchWidth {
			m = tqBatchWidth
		}
		block := codes[base : base+m]

		// Decode: radius and angle codes. A payload that cannot hold the
		// geometry the caller declared is the single-vector path's case, not
		// this one's, so hand the whole block over rather than inventing a
		// result for it here.
		short := false
		for w := 0; w < m; w++ {
			tqData := block[w]
			if len(tqData) < 4+angleBytes {
				short = true
				break
			}
			buf.radius[w] = math.Float32frombits(uint32(tqData[0]) | uint32(tqData[1])<<8 | uint32(tqData[2])<<16 | uint32(tqData[3])<<24)
			tqUnpackAngles(tqData[4:4+angleBytes], indices[w*angleCount:(w+1)*angleCount], bitsPerAngle)
		}
		if short {
			if err := turboQuantDistanceBatchFallback(query, block, dst[base:base+m], dim, pow2, bitsPerAngle); err != nil {
				return err
			}
			continue
		}

		// Reconstruct four polar trees together.
		//
		// The inner loop order is the whole point of this function. Written as
		// one candidate at a time, every iteration waits on the previous one's
		// recon[i]; written as four candidates at a time, the four chains are
		// independent and overlap. Each candidate still executes the same
		// operations on the same values in the same order, so each recon run is
		// bit-identical to running it alone.
		for w := 0; w < m; w++ {
			recon[w*pow2] = buf.radius[w]
		}
		currentLevelSize := 1
		angleOffset := angleCount
		for currentLevelSize < pow2 {
			angleOffset -= currentLevelSize
			for i := currentLevelSize - 1; i >= 0; i-- {
				for w := 0; w < m; w++ {
					r := recon[w*pow2+i]
					q := indices[w*angleCount+angleOffset+i]
					c := lookup[2*int(q)]
					s := lookup[2*int(q)+1]
					recon[w*pow2+2*i] = r * c
					recon[w*pow2+2*i+1] = r * s
				}
			}
			currentLevelSize *= 2
		}

		if l2 != nil {
			// QJL sign correction, four candidates per pass for the same reason.
			for w := 0; w < m; w++ {
				correction := buf.radius[w] / float32(math.Sqrt(float64(pow2))) * 0.1
				tqApplyQJLCorrection(recon[w*pow2:w*pow2+pow2], block[w][4+angleBytes:], correction)
			}

			// L2 stays per candidate, on the same kernel the single-vector path
			// uses. A vertical four-lane kernel would be the natural next step and
			// would change the summation order, which the exact-equality gate in
			// TestTQComputeBatchMatchesPerCandidate forbids.
			for w := 0; w < m; w++ {
				sum, err := l2(query[:dim], recon[w*pow2:w*pow2+dim])
				if err != nil {
					return err
				}
				dst[base+w] = float32(math.Sqrt(float64(sum)))
			}
		} else {
			// When l2 is nil, use the generic fused QJL + L2 kernel which matches
			// TurboQuantDistanceGeneric and TurboQuantDistanceNEON bit-identically.
			for w := 0; w < m; w++ {
				correction := buf.radius[w] / float32(math.Sqrt(float64(pow2))) * 0.1
				sum := l2SquaredTQCorrectionGeneric(query, recon[w*pow2:w*pow2+dim], block[w][4+angleBytes:], correction, dim)
				dst[base+w] = float32(math.Sqrt(float64(sum)))
			}
		}
	}
	return nil
}

// tqUnpackAngles expands packed angle codes into one byte per angle. It is the
// same expansion the single-vector paths perform inline, factored out so the
// batched kernel cannot drift from them.
func tqUnpackAngles(packed []byte, dst []byte, bitsPerAngle int) {
	n := len(dst)
	switch bitsPerAngle {
	case 8:
		copy(dst[:n], packed)
	case 4:
		for i := 0; i < n/2; i++ {
			b := packed[i]
			dst[2*i] = b & 0x0F
			dst[2*i+1] = b >> 4
		}
		if n%2 != 0 {
			dst[n-1] = packed[n/2] & 0x0F
		}
	case 2:
		i := 0
		for ; i+4 <= n; i += 4 {
			b := packed[i/4]
			dst[i] = b & 0x03
			dst[i+1] = (b >> 2) & 0x03
			dst[i+2] = (b >> 4) & 0x03
			dst[i+3] = b >> 6
		}
		// n is pow2-1 and therefore odd, so the vector loop always leaves 1-3
		// codes behind. They must be written too: dst is pooled scratch and
		// would otherwise feed a previous request's codes into this
		// reconstruction.
		for ; i < n; i++ {
			dst[i] = (packed[i/4] >> (uint(i%4) * 2)) & 0x03
		}
	}
}

// turboQuantDistanceBatchFallback runs the whole block through
// TurboQuantDistanceGeneric.
//
// Generic rather than the dispatched SIMD function, deliberately. Generic is the
// implementation that owns the definition of a payload too short for its
// declared geometry: it returns a zero distance, and it is the only one of the
// four that checked the length at all until this kernel needed somewhere to
// defer to. Deferring anywhere else would leave the batched path answering a
// case its single-vector counterpart cannot.
func turboQuantDistanceBatchFallback(query []float32, codes [][]byte, dst []float32, dim, pow2, bitsPerAngle int) error {
	for i, c := range codes {
		d, err := TurboQuantDistanceGeneric(query, c, dim, pow2, bitsPerAngle)
		if err != nil {
			return err
		}
		dst[i] = d
	}
	return nil
}
