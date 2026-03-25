package store

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
}

// NewTurboQuantEncoder creates a new encoder.
func NewTurboQuantEncoder(dims int, bitsPerAngle int, seed int64) *TurboQuantEncoder {
	pow2 := 1
	for pow2 < dims {
		pow2 <<= 1
	}
	return &TurboQuantEncoder{
		params: TurboQuantParams{BitsPerAngle: bitsPerAngle, Seed: seed},
		dims:   dims,
		pow2:   pow2,
	}
}

// Encode compresses a float32 vector into a TurboQuant byte stream.
func (e *TurboQuantEncoder) Encode(vec []float32) ([]byte, error) {
	// 1. Padding to power of 2 for Hadamard
	work := make([]float32, e.pow2)
	copy(work, vec)

	// 2. Random Rotation (Hadamard Transform)
	if err := simd.RandomRotation(work, e.params.Seed); err != nil {
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
	recon := make([]float32, e.pow2)
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

	nextVec := make([]float32, n/2)
	
	// This is a simplified recursive polar transform:
	// Each pair (x, y) -> (r, theta)
	// The radii become the input for the next level.
	for i := 0; i < n/2; i++ {
		x := vec[2*i]
		y := vec[2*i+1]
		r := float32(math.Sqrt(float64(x*x + y*y)))
		theta := float32(math.Atan2(float64(y), float64(x)))
		
		nextVec[i] = r
		// Store angles in reverse order of levels or similar
		// Here we'll just pack them predictably
		angles[i] = theta
	}
	
	// Recursive call on the radii
	return e.polarTransformRecursive(nextVec, angles[n/2:])
}

// polarReconstructRecursive converts [1 Radius, N-1 Angles] back to Cartesian.
func (e *TurboQuantEncoder) polarReconstructRecursive(radius float32, angles []float32, dst []float32) {
	n := len(dst)
	if n == 1 {
		dst[0] = radius
		return
	}

	// First, reconstruct the intermediate radii from the top-level radius and last (n-1)-(n/2) angles
	nextRadii := make([]float32, n/2)
	e.polarReconstructRecursive(radius, angles[n/2:], nextRadii)

	// Now expand each radius to a pair (x, y) using the first n/2 angles
	for i := 0; i < n/2; i++ {
		r := nextRadii[i]
		theta := angles[i]
		dst[2*i] = r * float32(math.Cos(float64(theta)))
		dst[2*i+1] = r * float32(math.Sin(float64(theta)))
	}
}

func (e *TurboQuantEncoder) packAngles(angles []float32, dst []byte) {
	bits := e.params.BitsPerAngle
	maxVal := float32((uint32(1) << bits) - 1)
	
	var currentBit int
	
	for _, angle := range angles {
		// Normalize angle [-PI, PI] to [0, 1]
		norm := (angle + math.Pi) / (2 * math.Pi)
		if norm < 0 { norm = 0 }
		if norm > 1 { norm = 1 }
		
		// Quantize to int
		q := uint32(norm * maxVal + 0.5)
		
		// Pack into bits
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
	recon := make([]float32, e.pow2)
	e.polarReconstructRecursive(radius, angles, recon)
	
	// Apply QJL Correction
	qjlBits := data[qjlOffset:]
	// The QJL term in the estimator is often added as a bias or scale.
	// Here we'll treat it as a sign bit of the residual to improve accuracy.
	// In the paper, QJL error correction allows the model to calculate 
	// attention scores more accurately by eliminating bias.
	for i := 0; i < e.pow2; i++ {
		if (qjlBits[i/8] & (byte(1) << (i % 8))) != 0 {
			// If bit is set, the residual was positive.
			// Add a small correction factor based on the radius/dims.
			correction := radius / float32(math.Sqrt(float64(e.pow2))) * 0.1 // Heuristic
			recon[i] += correction
		} else {
			recon[i] -= 0.1 // Heuristic
		}
	}
	
	return recon, nil
}

func (e *TurboQuantEncoder) unpackAngles(src []byte, dst []float32) {
	bits := e.params.BitsPerAngle
	maxVal := float32((uint32(1) << bits) - 1)
	
	var currentBit int
	for i := range dst {
		var q uint32
		for k := 0; k < bits; k++ {
			if (src[currentBit/8] & (byte(1) << (currentBit % 8))) != 0 {
				q |= (uint32(1) << k)
			}
			currentBit++
		}
		
		// Map back to [-PI, PI]
		norm := float32(q) / maxVal
		dst[i] = norm*2*math.Pi - math.Pi
	}
}
