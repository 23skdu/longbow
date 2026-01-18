package store

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// SetVectorFromFloat32 sets the vector at the given ID using the provided float32 input.
// It handles type conversion and storage selection (e.g. Float16 auxiliary, Complex interleave).
// It assumes the chunk for the ID has already been allocated (via ensureChunk).
func (gd *GraphData) SetVectorFromFloat32(id uint32, vec []float32) error {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	dims := gd.Dims // logical dims

	// Safely determine the number of elements to copy
	limit := dims
	if len(vec) < limit {
		limit = len(vec)
	}

	// 1. Try Float16 (Auxiliary or Primary)
	// We check this first because if VectorsF16 exists (Aux), we prefer it for storage if enabled.
	// Or if Type is Float16 (Primary).
	// GetVectorsF16Chunk handles both.
	if (gd.VectorsF16 != nil && int(cID) < len(gd.VectorsF16)) || gd.Type == VectorTypeFloat16 {
		f16Chunk := gd.GetVectorsF16Chunk(cID)
		if f16Chunk != nil {
			stride := gd.GetPaddedDimsForType(VectorTypeFloat16)
			start := int(cOff) * stride
			dest := f16Chunk[start : start+dims]
			for i := 0; i < limit; i++ {
				dest[i] = float16.New(vec[i])
			}
			return nil
		}
	}

	// 2. Switch on Primary Type
	paddedDims := gd.GetPaddedDims() // physical stride for primary type
	start := int(cOff) * paddedDims
	switch gd.Type {
	case VectorTypeFloat32:
		chunk := gd.GetVectorsChunk(cID)
		if chunk != nil {
			copy(chunk[start:start+dims], vec[:limit])
			return nil
		}

	case VectorTypeComplex64:
		chunk := gd.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			// Input is interleaved [r, i, r, i...]
			// Dims for complex type is number of complex numbers
			// vec len is number of float32s.
			// so vec len should be >= 2 * dims?
			// Safety check for complex logic:
			for i := 0; i < dims; i++ {
				if 2*i+1 < len(vec) {
					dest[i] = complex(vec[2*i], vec[2*i+1])
				}
			}
			return nil
		}

	case VectorTypeComplex128:
		chunk := gd.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			for i := 0; i < dims; i++ {
				if 2*i+1 < len(vec) {
					dest[i] = complex(float64(vec[2*i]), float64(vec[2*i+1]))
				}
			}
			return nil
		}

	case VectorTypeFloat64:
		chunk := gd.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			for i := 0; i < limit; i++ {
				dest[i] = float64(vec[i])
			}
			return nil
		}

	case VectorTypeInt8:
		chunk := gd.GetVectorsInt8Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			for i := 0; i < limit; i++ {
				dest[i] = int8(vec[i]) // Simple cast, maybe add saturation?
			}
			return nil
		}

		// TODO: Add other types (Uint8, Int16, etc) as needed
	}

	return fmt.Errorf("storage not available or type not supported for SetVectorFromFloat32: ID=%d Type=%s", id, gd.Type)
}

// GetVectorAsFloat32 retrieves the vector at ID and converts it to []float32.
// This is used for legacy paths, generic distance fallbacks, and public APIs.
// It avoids allocation if the underlying storage is already Float32, returning the slice directly.
func (gd *GraphData) GetVectorAsFloat32(id uint32) ([]float32, error) {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	dims := gd.Dims
	paddedDims := gd.GetPaddedDims()

	if dims <= 0 {
		return nil, fmt.Errorf("invalid dimensions 0")
	}

	// 1. Float16 (Primary or Aux) - Check this BEFORE Float32 if potentially using F16 storage backing
	if (gd.VectorsF16 != nil && int(cID) < len(gd.VectorsF16)) || gd.Type == VectorTypeFloat16 {
		chunk := gd.GetVectorsF16Chunk(cID)
		if chunk != nil {
			stride := gd.GetPaddedDimsForType(VectorTypeFloat16)
			start := int(cOff) * stride
			f16s := chunk[start : start+dims]
			res := make([]float32, dims)
			for i, v := range f16s {
				res[i] = v.Float32()
			}
			return res, nil
		}
	}

	// 2. primary Float32
	if gd.Type == VectorTypeFloat32 {
		chunk := gd.GetVectorsChunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			return chunk[start : start+dims], nil
		}
	}

	// 3. Complex64
	if gd.Type == VectorTypeComplex64 {
		chunk := gd.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			c64s := chunk[start : start+dims]
			res := make([]float32, dims*2)
			for i, v := range c64s {
				res[2*i] = real(v)
				res[2*i+1] = imag(v)
			}
			return res, nil
		}
	}

	// 4. Complex128
	if gd.Type == VectorTypeComplex128 {
		chunk := gd.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			c128s := chunk[start : start+dims]
			res := make([]float32, dims*2)
			for i, v := range c128s {
				res[2*i] = float32(real(v))
				res[2*i+1] = float32(imag(v))
			}
			return res, nil
		}
	}

	// 5. Int8
	if gd.Type == VectorTypeInt8 {
		chunk := gd.GetVectorsInt8Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims // Assumption: Int8 PaddedDims matches
			i8s := chunk[start : start+dims]
			res := make([]float32, dims)
			for i, v := range i8s {
				res[i] = float32(v)
			}
			return res, nil
		}
	}

	// 5. SQ8
	if gd.VectorsSQ8 != nil && int(cID) < len(gd.VectorsSQ8) {
		chunk := gd.GetVectorsSQ8Chunk(cID)
		if chunk != nil {
			// This requires Quantizer access which GraphData doesn't have.
			// This method is limited to raw storage access on GraphData.
			// Decoding SQ8 requires external context (ArrowHNSW).
			return nil, fmt.Errorf("cannot decode SQ8 without quantizer context")
		}
	}

	return nil, fmt.Errorf("vector data unavailable for ID %d", id)
}

// SetVector sets the vector at the given ID using the provided generic input.
// It handles type conversion and storage selection.
func (gd *GraphData) SetVector(id uint32, vec any) error {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	dims := gd.Dims

	// 1. Switch on Input Type
	switch v := vec.(type) {
	case []float32:
		return gd.SetVectorFromFloat32(id, v)
	case []float16.Num:
		// Native Float16 storage
		if (gd.VectorsF16 != nil && int(cID) < len(gd.VectorsF16)) || gd.Type == VectorTypeFloat16 {
			f16Chunk := gd.GetVectorsF16Chunk(cID)
			if f16Chunk != nil {
				start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeFloat16)
				copy(f16Chunk[start:start+dims], v)
				return nil
			}
		}
		// If main storage is Float32, convert?
		if gd.Type == VectorTypeFloat32 {
			chunk := gd.GetVectorsChunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDims()
				dest := chunk[start : start+dims]
				limit := dims
				if len(v) < limit {
					limit = len(v)
				}
				for i := 0; i < limit; i++ {
					dest[i] = v[i].Float32()
				}
				return nil
			}
		}
	case []complex64:
		if gd.Type == VectorTypeComplex64 {
			chunk := gd.GetVectorsComplex64Chunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeComplex64)
				copy(chunk[start:start+dims], v)
				return nil
			}
		}
	case []complex128:
		if gd.Type == VectorTypeComplex128 {
			chunk := gd.GetVectorsComplex128Chunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeComplex128)
				copy(chunk[start:start+dims], v)
				return nil
			}
		}
	case []float64:
		// Convert Float64 input to storage type
		switch gd.Type {
		case VectorTypeFloat64:
			chunk := gd.GetVectorsFloat64Chunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeFloat64)
				copy(chunk[start:start+dims], v)
				return nil
			}
		case VectorTypeComplex128:
			// Treat []float64 as interleaved real/imag
			chunk := gd.GetVectorsComplex128Chunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeComplex128)
				dest := chunk[start : start+dims]
				for i := 0; i < dims; i++ {
					if 2*i+1 < len(v) {
						dest[i] = complex(v[2*i], v[2*i+1])
					}
				}
				return nil
			}
		case VectorTypeFloat32:
			chunk := gd.GetVectorsChunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDims()
				dest := chunk[start : start+dims]
				limit := dims
				if len(v) < limit {
					limit = len(v)
				}
				for i := 0; i < limit; i++ {
					dest[i] = float32(v[i])
				}
				return nil
			}
		case VectorTypeComplex64:
			chunk := gd.GetVectorsComplex64Chunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeComplex64)
				dest := chunk[start : start+dims]
				for i := 0; i < dims; i++ {
					if 2*i+1 < len(v) {
						dest[i] = complex(float32(v[2*i]), float32(v[2*i+1]))
					}
				}
				return nil
			}
		}
	case []int8:
		if gd.Type == VectorTypeInt8 {
			chunk := gd.GetVectorsInt8Chunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDims()
				copy(chunk[start:start+dims], v)
				return nil
			}
		}
	}

	return fmt.Errorf("storage not available or type mismatch for SetVector: ID=%d Type=%s Input=%T", id, gd.Type, vec)
}

// GetVector retrieves the vector at ID in its native format (any).
// It switches on the GraphData type to return the correct slice type.
func (gd *GraphData) GetVector(id uint32) (any, error) {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	dims := gd.Dims

	if dims <= 0 {
		return nil, fmt.Errorf("invalid dimensions 0")
	}

	switch gd.Type {
	case VectorTypeFloat32:
		// Check for F16 override first
		if gd.VectorsF16 != nil && int(cID) < len(gd.VectorsF16) {
			chunk := gd.GetVectorsF16Chunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeFloat16)
				return chunk[start : start+dims], nil
			}
		}

		chunk := gd.GetVectorsChunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDims()
			// Return a copy or slice? Search usually wants to use it directly.
			// Let's return slice. Caller should handle safety (epoch/lock).
			return chunk[start : start+dims], nil
		}

	case VectorTypeFloat16:
		// Check F16 arena
		chunk := gd.GetVectorsF16Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeFloat16)
			return chunk[start : start+dims], nil
		}

	case VectorTypeFloat64:
		chunk := gd.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeFloat64)
			return chunk[start : start+dims], nil
		}

	case VectorTypeComplex64:
		chunk := gd.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeComplex64)
			return chunk[start : start+dims], nil
		}

	case VectorTypeComplex128:
		chunk := gd.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeComplex128)
			return chunk[start : start+dims], nil
		}

	case VectorTypeInt8:
		chunk := gd.GetVectorsInt8Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDims()
			return chunk[start : start+dims], nil
		}
	}

	// Fallback or error
	return nil, fmt.Errorf("vector data unavailable for ID %d (Type=%s)", id, gd.Type)
}
