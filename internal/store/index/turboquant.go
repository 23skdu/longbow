package index

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/23skdu/longbow/internal/simd"
)

// TurboQuantParams defines the quantization settings.
type TurboQuantParams struct {
	// BitsPerAngle is the angular grid resolution. The codec accepts 1..8, but
	// only 4..8 preserve the vector direction: the recursive polar transform
	// spends one angle per reconstructed coordinate, so a coarse grid caps the
	// achievable cosine. Measured on a linear ramp (dims 128/384/768):
	// 1 bit ~0.06/0.00/-0.00, 2 bits ~0.50/0.39/0.34, 3 bits ~0.82/0.72/0.72,
	// 4 bits ~0.95/0.93/0.92, 5 bits ~0.99/0.98/0.97, 6 bits ~0.992,
	// 7 bits ~0.994, 8 bits ~0.995. TestTurboQuantRoundTrip pins the 4..8
	// contract at cosine > 0.90.
	BitsPerAngle int
	Seed         int64 // For random rotation
}

// Angle-grid sin/cos tables for the quantized (decode) path. Every supported
// bit depth owns the 2^bits entries [tqPolarLUTBase(bits), +2^bits) of a
// single 510-pair array, interleaved as [cos(q), sin(q)]. The array is
// 4080 bytes, built once in init before any goroutine can observe it, so
// lookups are race-free and stay resident in L1.
const tqPolarLUTEntries = 2 + 4 + 8 + 16 + 32 + 64 + 128 + 256

var tqPolarLUT [2 * tqPolarLUTEntries]float32

// tqPolarLUTBase returns the pair index where a bit depth's entries start:
// sum(2^k, k<bits) == 2^bits-2.
func tqPolarLUTBase(bits int) int {
	return (1 << bits) - 2
}

// tqPolarLUTFor returns the interleaved cos/sin table for bits, or nil when
// the depth is outside [1, 8].
func tqPolarLUTFor(bits int) []float32 {
	if bits < 1 || bits > 8 {
		return nil
	}
	base := tqPolarLUTBase(bits)
	return tqPolarLUT[2*base : 2*(base+(1<<bits))]
}

func init() {
	for bits := 1; bits <= 8; bits++ {
		n := 1 << bits
		// The grid is produced by the very unpacker Decode would have used, so
		// the table reproduces the exact float32 angle (the AVX2/NEON kernels
		// compute code*scale+bias with FMA, which can differ from the multiply
		// and add being separately rounded) and the Sincos of it is bit-exact.
		thetas := make([]float32, n)
		unpackAngleValues(bits, packAngleCodes(bits, n), thetas)
		base := tqPolarLUTBase(bits)
		for code, theta := range thetas {
			s, c := math.Sincos(float64(theta))
			tqPolarLUT[2*(base+code)] = float32(c)
			tqPolarLUT[2*(base+code)+1] = float32(s)
		}
	}
}

// packAngleCodes writes the codes 0..count-1 into a fresh buffer using the same
// LSB-first layout as packAngles. Codes do not fit once count exceeds 2^bits, so
// the sequence wraps modulo 2^bits; the table builder asks for exactly 2^bits.
func packAngleCodes(bits, count int) []byte {
	dst := make([]byte, (count*bits+7)/8)
	bit := 0
	for code := 0; code < count; code++ {
		for k := 0; k < bits; k++ {
			if code&(1<<k) != 0 {
				dst[bit/8] |= 1 << (bit % 8)
			}
			bit++
		}
	}
	return dst
}

// TurboQuantEncoder handles the two-stage compression: PolarQuant + QJL.
type TurboQuantEncoder struct {
	params TurboQuantParams
	dims   int
	pow2   int
	had    *simd.HadamardTransformer
	// Lock-free ring buffer for float32 workspaces (work/recon/stack/angles)
	pool *LockFreeRingBuffer[*[]float32]
	// Lock-free ring buffer for QJL bit-scratch byte slices.
	qjlPool *LockFreeRingBuffer[*[]byte]
	// Lock-free ring buffer for decoded angle-code scratch byte slices.
	codesPool *LockFreeRingBuffer[*[]byte]
}

// NewTurboQuantEncoder creates a new encoder.
func NewTurboQuantEncoder(dims int, bitsPerAngle int, seed int64) *TurboQuantEncoder {
	if bitsPerAngle <= 0 || bitsPerAngle > 8 {
		bitsPerAngle = 4
	}
	pow2 := 1
	for pow2 < dims {
		pow2 <<= 1
	}
	// Create ring buffers for workspaces. Size 1024 is plenty for concurrent bulk inserts.
	rb := NewLockFreeRingBuffer[*[]float32](1024)
	qb := NewLockFreeRingBuffer[*[]byte](1024)
	cb := NewLockFreeRingBuffer[*[]byte](1024)

	return &TurboQuantEncoder{
		params:    TurboQuantParams{BitsPerAngle: bitsPerAngle, Seed: seed},
		dims:      dims,
		pow2:      pow2,
		had:       simd.NewHadamardTransformer(pow2),
		pool:      rb,
		qjlPool:   qb,
		codesPool: cb,
	}
}

func (e *TurboQuantEncoder) getWorkspace() *[]float32 {
	wsPtr, ok := e.pool.Pop()
	if !ok {
		ws := make([]float32, e.pow2*4)
		return &ws
	}
	if len(*wsPtr) < e.pow2*4 {
		ws := make([]float32, e.pow2*4)
		return &ws
	}
	return wsPtr
}

func (e *TurboQuantEncoder) putWorkspace(wsPtr *[]float32) {
	e.pool.Push(wsPtr) // Ignore if full, let GC handle it
}

// getQJLScratch returns a byte slice of at least n bytes for QJL bit packing.
func (e *TurboQuantEncoder) getQJLScratch(n int) *[]byte {
	if ptr, ok := e.qjlPool.Pop(); ok {
		if cap(*ptr) >= n {
			s := (*ptr)[:n]
			clear(s)
			return &s
		}
	}
	s := make([]byte, n)
	return &s
}

func (e *TurboQuantEncoder) putQJLScratch(ptr *[]byte) {
	e.qjlPool.Push(ptr)
}

// getCodesScratch returns a byte slice of at least n bytes for the decoded
// angle codes. No clearing is needed: unpackAngleCodes writes every element.
// The pooled header is resized in place so a hit costs no allocation.
func (e *TurboQuantEncoder) getCodesScratch(n int) *[]byte {
	if ptr, ok := e.codesPool.Pop(); ok {
		if cap(*ptr) >= n {
			*ptr = (*ptr)[:n]
			return ptr
		}
	}
	s := make([]byte, n)
	return &s
}

func (e *TurboQuantEncoder) putCodesScratch(ptr *[]byte) {
	e.codesPool.Push(ptr)
}

// Encode compresses a float32 vector into a TurboQuant byte stream.
func (e *TurboQuantEncoder) Encode(vec []float32) ([]byte, error) {
	wsPtr := e.getWorkspace()
	workspace := *wsPtr
	defer e.putWorkspace(wsPtr)

	// 1. Padding to power of 2 for Hadamard
	work := workspace[:e.pow2]
	copy(work, vec)
	if len(vec) < e.pow2 {
		for i := len(vec); i < e.pow2; i++ {
			work[i] = 0
		}
	}

	// 2. Random Rotation (sign-flip + FWHT) — must match PrecomputeRotatedQuery
	if err := simd.RandomRotation(work, e.params.Seed); err != nil {
		return nil, err
	}

	// 3. Stage 1: Recursive PolarQuant
	// We'll store:
	// - 1 float32 (radius)
	// - (pow2-1) angles (packed bits)
	// angles lives in the 4th workspace quadrant to avoid per-call allocation.
	angles := workspace[e.pow2*3 : e.pow2*3+(e.pow2-1)]
	stack := workspace[e.pow2*2 : e.pow2*3]
	radius, err := e.polarTransformRecursive(work, angles, stack)
	if err != nil {
		return nil, err
	}

	// 4. Reconstruction (to calculate residuals)
	// Use the second section of the workspace for recon, isolated from work and stack
	recon := workspace[e.pow2 : e.pow2*2]
	e.polarReconstructRecursive(radius, angles, recon, stack)

	// 5. Stage 2: QJL (Sign bit of residual)
	// residual = work - recon
	qjlPtr := e.getQJLScratch((e.pow2 + 7) / 8)
	qjlBits := *qjlPtr
	defer e.putQJLScratch(qjlPtr)
	for i := 0; i < e.pow2; i++ {
		if work[i] > recon[i] {
			qjlBits[i/8] |= (byte(1) << (i % 8))
		}
	}

	// 6. Packing
	// Format: [Radius (4B)][Packed Angles (Variable)][QJL Bits (Variable)]
	angleBytes := (len(angles)*e.params.BitsPerAngle + 7) / 8
	result := make([]byte, 4+angleBytes+len(qjlBits))

	// Radius
	binary.LittleEndian.PutUint32(result[0:4], math.Float32bits(radius))

	// Pack Angles
	e.packAngles(angles, result[4:4+angleBytes])

	// QJL Bits
	copy(result[4+angleBytes:], qjlBits)

	return result, nil
}

func (e *TurboQuantEncoder) polarTransformRecursive(vec []float32, angles []float32, stack []float32) (float32, error) {
	n := len(vec)
	if n == 1 {
		return vec[0], nil
	}

	stackOffset := e.pow2 - n
	nextRadii := stack[stackOffset : stackOffset+n/2]

	simd.GetTurboQuantPolarTransformFunc()(vec, nextRadii, angles[:n/2])

	// Recursive call on the radii
	return e.polarTransformRecursive(nextRadii, angles[n/2:], stack)
}

// polarReconstructRecursive rebuilds the Cartesian vector from a radius and a
// continuous angle list. Encode feeds it the raw atan2 output, which is not a
// quantized grid point, so it must stay on math.Sincos: a code-indexed table
// cannot represent it.
func (e *TurboQuantEncoder) polarReconstructRecursive(radius float32, angles []float32, dst []float32, stack []float32) {
	n := len(dst)
	if n == 1 {
		dst[0] = radius
		return
	}

	stackOffset := e.pow2 - n
	nextRadii := stack[stackOffset : stackOffset+n/2]

	e.polarReconstructRecursive(radius, angles[n/2:], nextRadii, stack)

	// Now expand each radius to a pair (x, y) using the first n/2 angles
	for i := 0; i < n/2; i++ {
		r := nextRadii[i]
		theta := angles[i]
		sin, cos := math.Sincos(float64(theta))
		dst[2*i] = r * float32(cos)
		dst[2*i+1] = r * float32(sin)
	}
}

// polarReconstructCodes is the LUT counterpart of polarReconstructRecursive for
// the decode path: the angles are quantized codes, so the sin/cos pair is a
// single table lookup instead of a math.Sincos call.
func (e *TurboQuantEncoder) polarReconstructCodes(radius float32, codes []byte, dst []float32, stack []float32) {
	n := len(dst)
	if n == 1 {
		dst[0] = radius
		return
	}

	stackOffset := e.pow2 - n
	nextRadii := stack[stackOffset : stackOffset+n/2]

	e.polarReconstructCodes(radius, codes[n/2:], nextRadii, stack)

	lookup := tqPolarLUTFor(e.params.BitsPerAngle)
	for i := 0; i < n/2; i++ {
		r := nextRadii[i]
		pair := 2 * int(codes[i])
		dst[2*i] = r * lookup[pair]
		dst[2*i+1] = r * lookup[pair+1]
	}
}

func (e *TurboQuantEncoder) packAngles(angles []float32, dst []byte) {
	bits := e.params.BitsPerAngle
	maxVal := float32((uint32(1) << bits) - 1)

	// Optimized path for 4 and 8 bits
	if bits == 8 {
		simd.PackTQ8(angles, dst)
		return
	}
	if bits == 4 {
		simd.PackTQ4(angles, dst)
		return
	}
	if bits == 2 {
		simd.PackTQ2(angles, dst)
		return
	}

	// Bit-accumulator fallback for non-power-of-two depths (1,3,5,6,7).
	// Packs whole values into a uint64 accumulator and flushes full bytes,
	// avoiding per-bit division/modulo in the hot loop.
	var acc uint64
	var accBits uint
	byteIdx := 0
	for _, angle := range angles {
		norm := (angle + math.Pi) * (1.0 / (2 * math.Pi))
		if norm < 0 {
			norm = 0
		} else if norm > 1 {
			norm = 1
		}
		q := uint64(norm*maxVal + 0.5) // #nosec G115
		acc |= q << accBits
		accBits += uint(bits)
		for accBits >= 8 {
			dst[byteIdx] = byte(acc)
			acc >>= 8
			accBits -= 8
			byteIdx++
		}
	}
	if accBits > 0 {
		dst[byteIdx] = byte(acc)
	}
}

// DecodeInto reconstructs the (rotated) vector from the byte stream directly into dst.
// dst must have length >= e.pow2 (or e.dims).
// Note: Inverse Hadamard must be applied afterwards if full Cartesian reconstruction is needed.
func (e *TurboQuantEncoder) DecodeInto(data []byte, dst []float32) error {
	if len(data) < 4 {
		return errors.New("invalid tq data: length < 4")
	}
	radius := math.Float32frombits(binary.LittleEndian.Uint32(data[0:4]))

	angleCount := e.pow2 - 1
	angleBytes := (angleCount*e.params.BitsPerAngle + 7) / 8
	qjlOffset := 4 + angleBytes
	if len(data) < qjlOffset {
		return errors.New("invalid tq data: truncated angle stream")
	}

	// Unpack the quantized angle codes: Decode only needs the code, because the
	// sin/cos grid is served by the LUT.
	wsPtr := e.getWorkspace()
	workspace := *wsPtr
	defer e.putWorkspace(wsPtr)

	codesPtr := e.getCodesScratch(angleCount)
	codes := *codesPtr
	defer e.putCodesScratch(codesPtr)
	e.unpackAngleCodes(data[4:qjlOffset], codes)

	// Reconstruct Cartesian using workspace quadrant 2 for recon, quadrant 3 for stack.
	recon := workspace[e.pow2 : e.pow2*2]
	stack := workspace[e.pow2*2 : e.pow2*3]
	e.polarReconstructCodes(radius, codes, recon, stack)

	// Apply QJL Correction
	qjlBits := data[qjlOffset:]
	correction := radius / float32(math.Sqrt(float64(e.pow2))) * 0.1 // Heuristic
	for i := 0; i < e.pow2; i++ {
		if i/8 < len(qjlBits) && (qjlBits[i/8]&(byte(1)<<(i%8))) != 0 {
			recon[i] += correction
		} else {
			recon[i] -= correction
		}
	}

	n := e.pow2
	if len(dst) < n {
		n = len(dst)
	}
	copy(dst[:n], recon[:n])
	return nil
}

// Decode reconstrucs the (rotated) vector from the byte stream.
// Note: Inverse Hadamard must be applied afterwards.
func (e *TurboQuantEncoder) Decode(data []byte) ([]float32, error) {
	out := make([]float32, e.pow2)
	if err := e.DecodeInto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetRadius extracts the radius (magnitude) from an encoded TurboQuant byte stream.
func (e *TurboQuantEncoder) GetRadius(data []byte) float32 {
	if len(data) < 4 {
		return 0
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(data[0:4]))
}

func (e *TurboQuantEncoder) unpackAngles(src []byte, dst []float32) {
	unpackAngleValues(e.params.BitsPerAngle, src, dst)
}

// unpackAngleValues expands packed angle codes into their theta grid values,
// i.e. code q of depth bits maps to float32(q)*(2*pi/(2^bits-1)) - pi as
// computed by the depth's unpacker. The LUT in init is built from this, so the
// two must stay in lockstep.
func unpackAngleValues(bits int, src []byte, dst []float32) {
	maxVal := float32((uint32(1) << bits) - 1)

	// Optimized path for 4 and 8 bits
	if bits == 8 {
		simd.UnpackTQ8(src, dst, 2*math.Pi/maxVal, -math.Pi)
		return
	}

	if bits == 4 {
		simd.UnpackTQ4(src, dst, 2*math.Pi/maxVal, -math.Pi)
		return
	}

	if bits == 2 {
		simd.UnpackTQ2(src, dst, 2*math.Pi/maxVal, -math.Pi)
		return
	}

	// Bit-accumulator fallback for non-power-of-two depths.
	// Pulls whole bytes into a uint64 accumulator and extracts `bits` at a
	// time, avoiding per-bit division/modulo.
	scale := (2 * math.Pi) / maxVal
	var acc uint64
	var accBits uint
	byteIdx := 0
	mask := uint64(1)<<bits - 1
	for i := range dst {
		for accBits < uint(bits) {
			acc |= uint64(src[byteIdx]) << accBits // #nosec G115
			byteIdx++
			accBits += 8
		}
		q := acc & mask
		acc >>= bits
		accBits -= uint(bits)
		dst[i] = float32(q)*scale - math.Pi
	}
}

// unpackAngleCodes extracts the raw quantized angle codes, the index of the
// sin/cos pair polarReconstructCodes needs. It is the exact inverse of
// packAngleCodes, so no trigonometry is evaluated here.
func (e *TurboQuantEncoder) unpackAngleCodes(src []byte, dst []byte) {
	bits := e.params.BitsPerAngle

	switch bits {
	case 8:
		copy(dst, src)
	case 4:
		i := 0
		for ; i+1 < len(dst); i += 2 {
			b := src[i/2]
			dst[i] = b & 0x0F
			dst[i+1] = b >> 4
		}
		if i < len(dst) {
			dst[i] = src[i/2] & 0x0F
		}
	case 2:
		i := 0
		for ; i+3 < len(dst); i += 4 {
			b := src[i/4]
			dst[i] = b & 0x03
			dst[i+1] = (b >> 2) & 0x03
			dst[i+2] = (b >> 4) & 0x03
			dst[i+3] = b >> 6
		}
		for ; i < len(dst); i++ {
			dst[i] = (src[i/4] >> (uint(i%4) * 2)) & 0x03
		}
	default:
		// Bit-accumulator fallback for non-power-of-two depths (1,3,5,6,7).
		var acc uint64
		var accBits uint
		byteIdx := 0
		mask := uint64(1)<<bits - 1
		for i := range dst {
			for accBits < uint(bits) {
				acc |= uint64(src[byteIdx]) << accBits // #nosec G115
				byteIdx++
				accBits += 8
			}
			dst[i] = byte(acc & mask) // #nosec G115 -- mask bounds the value to 8 bits
			acc >>= bits
			accBits -= uint(bits)
		}
	}
}

// PackedSize calculates the total byte size required to store a TurboQuant-encoded vector
// for the given logical dimension, including power-of-2 padding and bit-packing overhead.
// Part 5: Pads to 32-byte warp-aligned boundaries for coalesced GPU memory access.
func PackedSize(dims int, bitsPerAngle int) int {
	if dims <= 0 {
		return 0
	}
	p2 := int(1 << uint(math.Ceil(math.Log2(float64(dims)))))
	angleBytes := ((p2-1)*bitsPerAngle + 7) / 8
	bitBytes := (p2 + 7) / 8
	size := 4 + angleBytes + bitBytes
	return (size + 31) &^ 31 // Part 5: Pad to 32 bytes for GPU warp alignment
}

// PackedSize returns the stride needed for this encoder's configuration.
func (e *TurboQuantEncoder) PackedSize() int {
	return PackedSize(e.dims, e.params.BitsPerAngle)
}
