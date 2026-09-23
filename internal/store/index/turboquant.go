package index

import (
	"encoding/binary"
	"math"

	"github.com/23skdu/longbow/internal/simd"
)

// TurboQuantParams defines the quantization settings.
type TurboQuantParams struct {
	BitsPerAngle int   // e.g., 3 or 4 bits
	Seed         int64 // For random rotation
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

	return &TurboQuantEncoder{
		params:  TurboQuantParams{BitsPerAngle: bitsPerAngle, Seed: seed},
		dims:    dims,
		pow2:    pow2,
		had:     simd.NewHadamardTransformer(pow2),
		pool:    rb,
		qjlPool: qb,
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

// Decode reconstrucs the (rotated) vector from the byte stream.
// Note: Inverse Hadamard must be applied afterwards.
func (e *TurboQuantEncoder) Decode(data []byte) ([]float32, error) {
	radius := math.Float32frombits(binary.LittleEndian.Uint32(data[0:4]))

	angleCount := e.pow2 - 1
	angleBytes := (angleCount*e.params.BitsPerAngle + 7) / 8
	qjlOffset := 4 + angleBytes

	// Unpack Angles into workspace quadrant 4 (avoids per-call allocation).
	wsPtr := e.getWorkspace()
	workspace := *wsPtr
	defer e.putWorkspace(wsPtr)

	angles := workspace[e.pow2*3 : e.pow2*3+angleCount]
	e.unpackAngles(data[4:qjlOffset], angles)

	// Reconstruct Cartesian using workspace quadrant 2 for recon, quadrant 3 for stack.
	recon := workspace[e.pow2 : e.pow2*2]
	stack := workspace[e.pow2*2 : e.pow2*3]
	e.polarReconstructRecursive(radius, angles, recon, stack)

	// Apply QJL Correction
	qjlBits := data[qjlOffset:]
	// The QJL term in the estimator is often added as a bias or scale.
	// Here we'll treat it as a sign bit of the residual to improve accuracy.
	// In the paper, QJL error correction allows the model to calculate
	// attention scores more accurately by eliminating bias.
	// Hoist the loop-invariant correction scale out of the hot loop.
	correction := radius / float32(math.Sqrt(float64(e.pow2))) * 0.1 // Heuristic
	for i := 0; i < e.pow2; i++ {
		if (qjlBits[i/8] & (byte(1) << (i % 8))) != 0 {
			// If bit is set, the residual was positive.
			// Add a small correction factor based on the radius/dims.
			recon[i] += correction
		} else {
			recon[i] -= correction
		}
	}

	// Return a copy: callers own the result and the workspace is recycled.
	out := make([]float32, e.pow2)
	copy(out, recon)
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
	bits := e.params.BitsPerAngle
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
