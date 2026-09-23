package index

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// VectorLength returns the number of elements in any supported vector type.
// Returns 0 for unsupported types.
func VectorLength(v any) int {
	switch v.(type) {
	case []float32:
		return len(v.([]float32))
	case []float64:
		return len(v.([]float64))
	case []float16.Num:
		return len(v.([]float16.Num))
	case []int8:
		return len(v.([]int8))
	case []uint8:
		return len(v.([]uint8))
	case []int16:
		return len(v.([]int16))
	case []uint16:
		return len(v.([]uint16))
	case []int32:
		return len(v.([]int32))
	case []uint32:
		return len(v.([]uint32))
	case []int64:
		return len(v.([]int64))
	case []uint64:
		return len(v.([]uint64))
	case []complex64:
		return len(v.([]complex64))
	case []complex128:
		return len(v.([]complex128))
	default:
		return 0
	}
}

// CopyVectorToFloat32 copies elements from src into dst, converting to float32 as needed.
// For non-complex types, len(dst) must equal len(src).
// For complex types, len(dst) must equal 2*len(src) (real/imag interleaved).
// Returns an error if the type is unsupported or dst length mismatches.
func CopyVectorToFloat32(src any, dst []float32) error {
	switch v := src.(type) {
	case []float32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		copy(dst, v)
		return nil
	case []float64:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []complex128:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		for i, val := range v {
			dst[i*2] = float32(real(val))
			dst[i*2+1] = float32(imag(val))
		}
		return nil
	case []complex64:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		if len(v) == 0 {
			return nil
		}
		raw := unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
		copy(dst, raw)
		return nil
	case []float16.Num:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = val.Float32()
		}
		return nil
	case []int8:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []uint8:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []int16:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []uint16:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []int32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []uint32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []int64:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []uint64:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	default:
		return fmt.Errorf("unsupported vector type %T for float32 buffer extraction", src)
	}
}

// CopyVectorToFloat64 copies elements from src into dst, converting to float64 as needed.
// For non-complex types, len(dst) must equal len(src).
// For complex types, len(dst) must equal 2*len(src) (real/imag interleaved).
// Returns an error if the type is unsupported or dst length mismatches.
func CopyVectorToFloat64(src any, dst []float64) error {
	switch v := src.(type) {
	case []float64:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		copy(dst, v)
		return nil
	case []complex128:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		if len(v) == 0 {
			return nil
		}
		raw := unsafe.Slice((*float64)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
		copy(dst, raw)
		return nil
	case []float32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float64(val)
		}
		return nil
	default:
		return fmt.Errorf("unsupported vector type %T for float64 buffer extraction", src)
	}
}

// ConvertVectorToFloat32 allocates a new []float32 from any supported vector type.
// For complex types, the result has 2*len(src) elements (real/imag interleaved).
// Returns a copy for []float32 input to avoid aliasing.
func ConvertVectorToFloat32(src any) ([]float32, error) {
	switch v := src.(type) {
	case []float32:
		res := make([]float32, len(v))
		copy(res, v)
		return res, nil
	case []float64:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []float16.Num:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = val.Float32()
		}
		return res, nil
	case []int8:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint8:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []int16:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint16:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []int32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []int64:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint64:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []complex64:
		res := make([]float32, len(v)*2)
		for i, val := range v {
			res[i*2] = real(val)
			res[i*2+1] = imag(val)
		}
		return res, nil
	case []complex128:
		res := make([]float32, len(v)*2)
		for i, val := range v {
			res[i*2] = float32(real(val))
			res[i*2+1] = float32(imag(val))
		}
		return res, nil
	default:
		return nil, fmt.Errorf("unsupported vector type %T for float32 conversion", src)
	}
}

// DispatchVectorAny dispatches vecAny to fn if it matches []T.
// Returns nil without calling fn if vecAny is not []T.
func DispatchVectorAny[T any](vecAny any, fn func([]T) error) error {
	if v, ok := vecAny.([]T); ok {
		return fn(v)
	}
	return nil
}

// VectorVisitorFn holds optional callbacks for each supported vector type.
// Any nil callback is silently skipped.
type VectorVisitorFn struct {
	Float32    func([]float32) error
	Float64    func([]float64) error
	Float16    func([]float16.Num) error
	Int8       func([]int8) error
	Uint8      func([]uint8) error
	Int16      func([]int16) error
	Uint16     func([]uint16) error
	Int32      func([]int32) error
	Uint32     func([]uint32) error
	Int64      func([]int64) error
	Uint64     func([]uint64) error
	Complex64  func([]complex64) error
	Complex128 func([]complex128) error
}

// DispatchVectorAnyFn dispatches vecAny to the appropriate typed callback in fn.
// Returns nil if no callback matches (either type mismatch or nil callback).
func DispatchVectorAnyFn(vecAny any, fn VectorVisitorFn) error {
	switch v := vecAny.(type) {
	case []float32:
		if fn.Float32 != nil {
			return fn.Float32(v)
		}
	case []float64:
		if fn.Float64 != nil {
			return fn.Float64(v)
		}
	case []float16.Num:
		if fn.Float16 != nil {
			return fn.Float16(v)
		}
	case []int8:
		if fn.Int8 != nil {
			return fn.Int8(v)
		}
	case []uint8:
		if fn.Uint8 != nil {
			return fn.Uint8(v)
		}
	case []int16:
		if fn.Int16 != nil {
			return fn.Int16(v)
		}
	case []uint16:
		if fn.Uint16 != nil {
			return fn.Uint16(v)
		}
	case []int32:
		if fn.Int32 != nil {
			return fn.Int32(v)
		}
	case []uint32:
		if fn.Uint32 != nil {
			return fn.Uint32(v)
		}
	case []int64:
		if fn.Int64 != nil {
			return fn.Int64(v)
		}
	case []uint64:
		if fn.Uint64 != nil {
			return fn.Uint64(v)
		}
	case []complex64:
		if fn.Complex64 != nil {
			return fn.Complex64(v)
		}
	case []complex128:
		if fn.Complex128 != nil {
			return fn.Complex128(v)
		}
	}
	return nil
}
