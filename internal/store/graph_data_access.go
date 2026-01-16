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
			for i, v := range vec {
				dest[i] = float16.New(v)
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
			copy(chunk[start:start+dims], vec)
			return nil
		}

	case VectorTypeComplex64:
		chunk := gd.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			// Input is interleaved [r, i, r, i...]
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

	case VectorTypeInt8:
		chunk := gd.GetVectorsInt8Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			for i, v := range vec {
				dest[i] = int8(v) // Simple cast, maybe add saturation?
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

	// 1. primary Float32
	if gd.Type == VectorTypeFloat32 {
		chunk := gd.GetVectorsChunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			return chunk[start : start+dims], nil
		}
	}

	// 2. Float16 (Primary or Aux)
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
				for i, val := range v {
					dest[i] = val.Float32()
				}
				return nil
			}
		}
	case []complex64:
		if gd.Type == VectorTypeComplex64 {
			chunk := gd.GetVectorsComplex64Chunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDims()
				copy(chunk[start:start+dims], v)
				return nil
			}
		}
	case []complex128:
		if gd.Type == VectorTypeComplex128 {
			chunk := gd.GetVectorsComplex128Chunk(cID)
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

	case VectorTypeComplex64:
		chunk := gd.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDims() // Complex64 uses standard padding logic for itself if primary
			return chunk[start : start+dims], nil
		}

	case VectorTypeComplex128:
		chunk := gd.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDims()
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
