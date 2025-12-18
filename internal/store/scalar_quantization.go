package store

import (
"errors"
"math"
)

// =============================================================================
// Scalar Quantization (SQ8) - 4x Memory Reduction
// =============================================================================
// Converts float32 vectors to uint8 by mapping [min, max] -> [0, 255]
// Memory: 128-dim float32 (512 bytes) -> 128-dim uint8 (128 bytes) = 4x reduction

// SQ8Config holds the configuration for scalar quantization.
type SQ8Config struct {
Min     []float32 // Per-dimension minimum values
Max     []float32 // Per-dimension maximum values
Trained bool      // Whether min/max have been learned from data
}

// DefaultSQ8Config returns a default untrained configuration.
func DefaultSQ8Config() *SQ8Config {
return &SQ8Config{
Trained: false,
}
}

// Validate checks if the configuration is valid.
func (c *SQ8Config) Validate() error {
if c == nil {
return errors.New("SQ8Config is nil")
}

if !c.Trained {
// Untrained config is valid - will be trained later
return nil
}

// Trained config must have matching min/max lengths
if len(c.Min) != len(c.Max) {
return errors.New("min and max must have same length")
}

// Check that min < max for each dimension
for i := range c.Min {
if c.Min[i] >= c.Max[i] {
return errors.New("min must be less than max for all dimensions")
}
}

return nil
}

// SQ8Encoder handles encoding and decoding of vectors using scalar quantization.
type SQ8Encoder struct {
config *SQ8Config
scale  []float32 // Precomputed: 255 / (max - min)
invScale []float32 // Precomputed: (max - min) / 255
}

// NewSQ8Encoder creates a new encoder from a trained configuration.
func NewSQ8Encoder(cfg *SQ8Config) (*SQ8Encoder, error) {
if err := cfg.Validate(); err != nil {
return nil, err
}

if !cfg.Trained {
return nil, errors.New("config must be trained before creating encoder")
}

dims := len(cfg.Min)
scale := make([]float32, dims)
invScale := make([]float32, dims)

for i := 0; i < dims; i++ {
range_ := cfg.Max[i] - cfg.Min[i]
scale[i] = 255.0 / range_
invScale[i] = range_ / 255.0
}

return &SQ8Encoder{
config:   cfg,
scale:    scale,
invScale: invScale,
}, nil
}

// TrainSQ8Encoder creates an encoder by learning min/max from sample vectors.
func TrainSQ8Encoder(vectors [][]float32) (*SQ8Encoder, error) {
if len(vectors) == 0 {
return nil, errors.New("no vectors provided for training")
}

dims := len(vectors[0])
if dims == 0 {
return nil, errors.New("vectors have zero dimensions")
}

// Initialize min/max with first vector
minVals := make([]float32, dims)
maxVals := make([]float32, dims)
copy(minVals, vectors[0])
copy(maxVals, vectors[0])

// Find per-dimension min/max across all vectors
for _, vec := range vectors[1:] {
if len(vec) != dims {
return nil, errors.New("all vectors must have same dimensions")
}
for i, v := range vec {
if v < minVals[i] {
minVals[i] = v
}
if v > maxVals[i] {
maxVals[i] = v
}
}
}

// Add small epsilon to prevent division by zero
for i := range minVals {
if minVals[i] == maxVals[i] {
maxVals[i] = minVals[i] + 1e-7
}
}

cfg := &SQ8Config{
Min:     minVals,
Max:     maxVals,
Trained: true,
}

return NewSQ8Encoder(cfg)
}

// Dims returns the number of dimensions this encoder handles.
func (e *SQ8Encoder) Dims() int {
return len(e.config.Min)
}

// GetBounds returns the learned min and max values.
func (e *SQ8Encoder) GetBounds() (minVals, maxVals []float32) {
return e.config.Min, e.config.Max /* minVals, maxVals */
}

// Encode converts a float32 vector to uint8 with clamping.
func (e *SQ8Encoder) Encode(vec []float32) []uint8 {
result := make([]uint8, len(vec))
e.EncodeInto(vec, result)
return result
}

// EncodeInto encodes a vector into a pre-allocated destination slice.
// This is the zero-allocation hot path for bulk encoding.
func (e *SQ8Encoder) EncodeInto(vec []float32, dst []uint8) {
for i, v := range vec {
// Clamp to [min, max]
if v < e.config.Min[i] {
v = e.config.Min[i]
} else if v > e.config.Max[i] {
v = e.config.Max[i]
}

// Normalize to [0, 255]
normalized := (v - e.config.Min[i]) * e.scale[i]

// Convert to uint8 with floor
dst[i] = uint8(normalized)
}
}

// Decode converts a uint8 vector back to float32.
func (e *SQ8Encoder) Decode(quantized []uint8) []float32 {
result := make([]float32, len(quantized))
e.DecodeInto(quantized, result)
return result
}

// DecodeInto decodes a quantized vector into a pre-allocated destination slice.
func (e *SQ8Encoder) DecodeInto(quantized []uint8, dst []float32) {
for i, q := range quantized {
dst[i] = e.config.Min[i] + float32(q)*e.invScale[i]
}
}

// =============================================================================
// Distance Functions for Quantized Vectors
// =============================================================================

// SQ8EuclideanDistance computes Euclidean distance between quantized vectors.
// It decodes to float32 for accurate distance computation.
func SQ8EuclideanDistance(q1, q2 []uint8, enc *SQ8Encoder) float32 {
var sum float32
for i := range q1 {
// Decode both values
v1 := enc.config.Min[i] + float32(q1[i])*enc.invScale[i]
v2 := enc.config.Min[i] + float32(q2[i])*enc.invScale[i]

diff := v1 - v2
sum += diff * diff
}
return float32(math.Sqrt(float64(sum)))
}

// SQ8DistanceFast computes squared L2 distance directly in quantized space.
// This is faster but returns integer distance (not actual Euclidean).
// Useful for ranking (ordering is preserved) but not exact distances.
func SQ8DistanceFast(q1, q2 []uint8) uint32 {
var sum uint32
for i := range q1 {
diff := int32(q1[i]) - int32(q2[i])
sum += uint32(diff * diff)
}
return sum
}

// SQ8CosineDistance computes cosine distance between quantized vectors.
func SQ8CosineDistance(q1, q2 []uint8, enc *SQ8Encoder) float32 {
var dotProduct, norm1, norm2 float32

for i := range q1 {
v1 := enc.config.Min[i] + float32(q1[i])*enc.invScale[i]
v2 := enc.config.Min[i] + float32(q2[i])*enc.invScale[i]

dotProduct += v1 * v2
norm1 += v1 * v1
norm2 += v2 * v2
}

if norm1 == 0 || norm2 == 0 {
return 1.0 // Maximum distance for zero vectors
}

cosine := dotProduct / (float32(math.Sqrt(float64(norm1))) * float32(math.Sqrt(float64(norm2))))
return 1.0 - cosine // Convert similarity to distance
}
