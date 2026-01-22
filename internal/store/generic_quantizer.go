package store

import (
	"fmt"

	"github.com/23skdu/longbow/internal/pq"
)

// VectorType is a marker interface for all supported vector types
type VectorType interface{}

// Float32Type identifies float32 vector type
type Float32Type interface{}

// Float16Type identifies float16 vector type
type Float16Type interface{}

// Int8Type identifies int8 vector type
type Int8Type interface{}

// Uint8Type identifies uint8 code type (used by SQ8)
type Uint8Type interface{}

// Uint64Type identifies uint64 code type (used by BQ)
type Uint64Type interface{}

// ByteType identifies byte code type (used by PQ)
type ByteType interface{}

// QuantizerType identifies all quantizer types
type QuantizerType interface{}

// ==============================================================================
// Type Conversion Helpers
// ==============================================================================
// These helpers convert between different vector types before/after quantization.

// ConvertFloat32ToUint8 converts a slice of float32 to uint8
// This is a placeholder - the actual implementation would do proper casting
func ConvertFloat32ToUint8(vec []float32) ([]uint8, error) {
	result := make([]uint8, len(vec))
	for i, v := range vec {
		// Simple clamping to [0, 255] for now
		// TODO: Implement proper casting with bounds checking
		if v < 0 {
			result[i] = 0
		} else if v > 255 {
			result[i] = 255
		} else {
			result[i] = uint8(v)
		}
	}
	return result, nil
}

// ConvertUint8ToFloat32 converts a slice of uint8 to float32
func ConvertUint8ToFloat32(codes []uint8) ([]float32, error) {
	result := make([]float32, len(codes))
	for i, c := range codes {
		result[i] = float32(c)
	}
	return result, nil
}

// ConvertFloat32ToUint64 converts a slice of float32 to uint64 codes (BQ style)
func ConvertFloat32ToUint64(vec []float32) ([]uint64, error) {
	dims := len(vec)
	if dims > 64 {
		dims = 64
	}
	numWords := (dims + 63) / 64
	codes := make([]uint64, numWords)

	for i, v := range vec {
		if i >= dims {
			break
		}
		wordIdx := i / 64
		bitIdx := uint(i % 64)
		if v > 0 {
			codes[wordIdx] |= (1 << bitIdx)
		}
	}
	return codes, nil
}

// ConvertUint64ToFloat32 converts uint64 codes to float32 (BQ style)
func ConvertUint64ToFloat32(codes []uint64) ([]float32, error) {
	result := make([]float32, 64)
	for i := 0; i < 64; i++ {
		wordIdx := i / 64
		bitIdx := uint(i % 64)
		if (codes[wordIdx] & (1 << bitIdx)) != 0 {
			result[i] = 1.0
		} else {
			result[i] = -1.0
		}
	}
	return result, nil
}

// ==============================================================================
// Type Conversion Errors
// ==============================================================================

// ErrUnsupportedVectorType is returned when a vector type is not supported
type ErrUnsupportedVectorType struct {
	InputType     string
	ExpectedTypes []string
}

func (e *ErrUnsupportedVectorType) Error() string {
	return fmt.Sprintf("unsupported vector type: %s (expected one of: %v)",
		e.InputType, e.ExpectedTypes)
}

// ErrUnsupportedCodeType is returned when a code type is not supported
type ErrUnsupportedCodeType struct {
	CodeType      string
	ExpectedTypes []string
}

func (e *ErrUnsupportedCodeType) Error() string {
	return fmt.Sprintf("unsupported code type: %s (expected one of: %v)",
		e.CodeType, e.ExpectedTypes)
}

// ==============================================================================
// Generic Quantizer Interface
// ==============================================================================
// Quantizer[T, U] defines a generic quantizer interface
// T is the input vector type, U is the output code type
//
// This interface allows quantizers to work with different vector types while
// maintaining type safety through generic parameters.
//
// Supported combinations:
// - float32 vectors -> uint8 codes (SQ8)
// - float32 vectors -> uint64 codes (BQ)
// - float32 vectors -> byte codes (PQ)
// - Future: float16 vectors, int8 vectors
type Quantizer[T VectorType, U any] interface {
	// Encode converts a batch of vectors to quantized codes
	Encode([]T) []U
	// Decode converts quantized codes back to vectors
	Decode([]U) []T
	// Dims returns the number of dimensions
	Dims() int
}

// ==============================================================================
// Wrapper Implementations
// ==============================================================================
// These wrappers adapt existing concrete quantizers to the generic interface

// GenericSQ8Quantizer wraps SQ8Encoder to implement Quantizer interface
type GenericSQ8Quantizer struct {
	encoder *SQ8Encoder
}

// NewGenericSQ8Quantizer creates a generic SQ8 quantizer wrapper
func NewGenericSQ8Quantizer(encoder *SQ8Encoder) *GenericSQ8Quantizer {
	return &GenericSQ8Quantizer{encoder: encoder}
}

// Encode implements Quantizer interface
func (g *GenericSQ8Quantizer) Encode(vectors any) ([]uint8, error) {
	vf32, ok := vectors.([]float32)
	if !ok {
		return nil, &ErrUnsupportedVectorType{
			InputType:     fmt.Sprintf("%T", vectors),
			ExpectedTypes: []string{"[]float32", "[]float16", "[]int8"},
		}
	}
	return g.encoder.Encode(vf32), nil
}

// Decode implements Quantizer interface
func (g *GenericSQ8Quantizer) Decode(codes []uint8) ([]float32, error) {
	return g.encoder.Decode(codes), nil
}

// Dims implements Quantizer interface
func (g *GenericSQ8Quantizer) Dims() int {
	return g.encoder.Dims()
}

// GenericBQQuantizer wraps BQEncoder to implement Quantizer interface
type GenericBQQuantizer struct {
	encoder *BQEncoder
}

// NewGenericBQQuantizer creates a generic BQ quantizer wrapper
func NewGenericBQQuantizer(encoder *BQEncoder) *GenericBQQuantizer {
	return &GenericBQQuantizer{encoder: encoder}
}

// Encode implements Quantizer interface
func (g *GenericBQQuantizer) Encode(vectors any) ([]uint64, error) {
	vf32, ok := vectors.([]float32)
	if !ok {
		return nil, &ErrUnsupportedVectorType{
			InputType:     fmt.Sprintf("%T", vectors),
			ExpectedTypes: []string{"[]float32", "[]float16", "[]int8"},
		}
	}
	return g.encoder.Encode(vf32), nil
}

// Decode implements Quantizer interface
func (g *GenericBQQuantizer) Decode(codes []uint64) ([]float32, error) {
	return g.encoder.Decode(codes), nil
}

// Dims implements Quantizer interface
func (g *GenericBQQuantizer) Dims() int {
	return g.encoder.Dimensions
}

// GenericPQQuantizer wraps PQEncoder to implement Quantizer interface
type GenericPQQuantizer struct {
	encoder *pq.PQEncoder
}

// NewGenericPQQuantizer creates a generic PQ quantizer wrapper
func NewGenericPQQuantizer(encoder *pq.PQEncoder) *GenericPQQuantizer {
	return &GenericPQQuantizer{encoder: encoder}
}

// Encode implements Quantizer interface
func (g *GenericPQQuantizer) Encode(vectors any) ([]byte, error) {
	vf32, ok := vectors.([]float32)
	if !ok {
		return nil, &ErrUnsupportedVectorType{
			InputType:     fmt.Sprintf("%T", vectors),
			ExpectedTypes: []string{"[]float32", "[]float16", "[]int8"},
		}
	}
	codes, err := g.encoder.Encode(vf32)
	return codes, err
}

// Decode implements Quantizer interface
func (g *GenericBQQuantizer) Decode(codes []uint64) ([]float32, error) {
	// BQEncoder.Decode returns []float32 (not an error)
	return g.encoder.Decode(codes), nil
}

// Dims implements Quantizer interface
func (g *GenericPQQuantizer) Dims() int {
	return g.encoder.Dims
}
