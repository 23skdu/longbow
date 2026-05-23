package core

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

	// Workspace for recursive transforms to avoid allocations
	workspace []float32
}

// NewTurboQuantEncoder creates a new encoder.
func NewTurboQuantEncoder(dims int, bitsPerAngle int, seed int64) *TurboQuantEncoder {
	pow2 := 1
	for pow2 < dims {
		pow2 <<= 1
	}
	return &TurboQuantEncoder{
		params:    TurboQuantParams{BitsPerAngle: bitsPerAngle, Seed: seed},
		dims:      dims,
		pow2:      pow2,
		had:       simd.NewHadamardTransformer(pow2),
		workspace: make([]float32, pow2*2), // Workspace for recursion
	}
}

// Encode compresses a float32 vector into a TurboQuant byte stream.
func (e *TurboQuantEncoder) Encode(vec []float32) ([]byte, error) {
	// 1. Padding to power of 2 for Hadamard
	work := e.workspace[:e.pow2]
	copy(work, vec)
	if len(vec) < e.pow2 {
		for i := len(vec); i < e.pow2; i++ {
			work[i] = 0
		}
	}

	// 2. Random Rotation (Hadamard Transform)
	if err := e.had.Transform(work); err != nil {
		return nil, err
	}

	// 3. Stage 1: Recursive PolarQuant
	// We'll store:
	// - 1 float32 (radius)
	// - (pow2-1) angles (packed bits)
	angles := make([]float32, e.pow2-1)
	radius, err := e.polarTransformRecursive(work, angles)
	if err != nil {
		return nil, err
	}

	// 4. Reconstruction (to calculate residuals)
	// Use the second half of the workspace for recon
	recon := e.workspace[e.pow2 : e.pow2*2]
	e.polarReconstructRecursive(radius, angles, recon)

	// 5. Stage 2: QJL (Sign bit of residual)
	// residual = work - recon
	qjlBits := make([]byte, (e.pow2+7)/8)
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

// polarTransformRecursive converts Cartesian to [1 Radius, N-1 Angles].
func (e *TurboQuantEncoder) polarTransformRecursive(vec []float32, angles []float32) (float32, error) {
	n := len(vec)
	if n == 1 {
		return vec[0], nil
	}

	// Use part of the workspace for nextRadii
	// Workspace layout: [p2] nextRadii, [p2] ...
	// We need n/2 for nextRadii
	nextRadii := e.workspace[:n/2]

	simd.GetTurboQuantPolarTransformFunc()(vec, nextRadii, angles[:n/2])

	// Recursive call on the radii
	return e.polarTransformRecursive(nextRadii, angles[n/2:])
}

// polarReconstructRecursive converts [1 Radius, N-1 Angles] back to Cartesian.
func (e *TurboQuantEncoder) polarReconstructRecursive(radius float32, angles []float32, dst []float32) {
	n := len(dst)
	if n == 1 {
		dst[0] = radius
		return
	}

	// Use part of the workspace for nextRadii
	// Since we are reconstructing, we need to be careful with workspace reuse in recursion.
	// We'll use an offset into the workspace based on depth.
	depth := 0
	tmpN := n
	for tmpN < e.pow2 {
		tmpN *= 2
		depth++
	}
	// Simplified: use a separate workspace area or just the second half of the main workspace
	nextRadii := e.workspace[e.pow2 : e.pow2+n/2]

	e.polarReconstructRecursive(radius, angles[n/2:], nextRadii)

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

	// Fallback for other bit depths
	var currentBit int
	for _, angle := range angles {
		norm := (angle + math.Pi) / (2 * math.Pi)
		if norm < 0 {
			norm = 0
		} else if norm > 1 {
			norm = 1
		}
		q := uint32(norm*maxVal + 0.5)
		for k := 0; k < bits; k++ {
			if (q & (uint32(1) << k)) != 0 {
				dst[currentBit/8] |= (1 << (currentBit % 8))
			}
			currentBit++
		}
	}
}

// Decode reconstrucs the (rotated) vector from the byte stream.
// Note: Inverse Hadamard must be applied afterwards.
func (e *TurboQuantEncoder) Decode(data []byte) ([]float32, error) {
	radius := math.Float32frombits(binary.LittleEndian.Uint32(data[0:4]))

	angleCount := e.pow2 - 1
	angleBytes := (angleCount*e.params.BitsPerAngle + 7) / 8
	qjlOffset := 4 + angleBytes

	// Unpack Angles
	angles := make([]float32, angleCount)
	e.unpackAngles(data[4:qjlOffset], angles)

	// Reconstruct Cartesian
	recon := make([]float32, e.pow2) // We can't use e.workspace here if Decode is called concurrently, but e is usually owned by a dataset
	e.polarReconstructRecursive(radius, angles, recon)

	// Apply QJL Correction
	qjlBits := data[qjlOffset:]
	// The QJL term in the estimator is often added as a bias or scale.
	// Here we'll treat it as a sign bit of the residual to improve accuracy.
	// In the paper, QJL error correction allows the model to calculate
	// attention scores more accurately by eliminating bias.
	for i := 0; i < e.pow2; i++ {
		correction := radius / float32(math.Sqrt(float64(e.pow2))) * 0.1 // Heuristic
		if (qjlBits[i/8] & (byte(1) << (i % 8))) != 0 {
			// If bit is set, the residual was positive.
			// Add a small correction factor based on the radius/dims.
			recon[i] += correction
		} else {
			recon[i] -= correction
		}
	}

	return recon, nil
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

	// Fallback
	var currentBit int
	for i := range dst {
		var q uint32
		for k := 0; k < bits; k++ {
			if (src[currentBit/8] & (byte(1) << (currentBit % 8))) != 0 {
				q |= (uint32(1) << k)
			}
			currentBit++
		}
		norm := float32(q) / maxVal
		dst[i] = norm*2*math.Pi - math.Pi
	}
}

// PackedSize calculates the total byte size required to store a TurboQuant-encoded vector
// for the given logical dimension, including power-of-2 padding and bit-packing overhead.
func PackedSize(dims int, bitsPerAngle int) int {
	if dims <= 0 {
		return 0
	}
	p2 := int(1 << uint(math.Ceil(math.Log2(float64(dims)))))
	angleBytes := ((p2-1)*bitsPerAngle + 7) / 8
	bitBytes := (p2 + 7) / 8
	size := 4 + angleBytes + bitBytes
	return (size + 3) &^ 3 // Pad to 4 bytes for GPU alignment
}

// PackedSize returns the stride needed for this encoder's configuration.
func (e *TurboQuantEncoder) PackedSize() int {
	return PackedSize(e.dims, e.params.BitsPerAngle)
}
