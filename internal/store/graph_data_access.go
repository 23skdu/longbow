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
				dest[i] = int8(vec[i])
			}
			return nil
		}
	case VectorTypeUint8:
		chunk := gd.GetVectorsUint8Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			for i := 0; i < limit; i++ {
				dest[i] = uint8(vec[i])
			}
			return nil
		}
	case VectorTypeInt16:
		chunk := gd.GetVectorsInt16Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			for i := 0; i < limit; i++ {
				dest[i] = int16(vec[i])
			}
			return nil
		}
	case VectorTypeUint16:
		chunk := gd.GetVectorsUint16Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			for i := 0; i < limit; i++ {
				dest[i] = uint16(vec[i])
			}
			return nil
		}
	case VectorTypeInt32:
		chunk := gd.GetVectorsInt32Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			for i := 0; i < limit; i++ {
				dest[i] = int32(vec[i])
			}
			return nil
		}
	case VectorTypeUint32:
		chunk := gd.GetVectorsUint32Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			for i := 0; i < limit; i++ {
				dest[i] = uint32(vec[i])
			}
			return nil
		}
	case VectorTypeInt64:
		chunk := gd.GetVectorsInt64Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			for i := 0; i < limit; i++ {
				dest[i] = int64(vec[i])
			}
			return nil
		}
	case VectorTypeUint64:
		chunk := gd.GetVectorsUint64Chunk(cID)
		if chunk != nil {
			dest := chunk[start : start+dims]
			for i := 0; i < limit; i++ {
				dest[i] = uint64(vec[i])
			}
			return nil
		}
	}

	return fmt.Errorf("storage not available or type not supported for SetVectorFromFloat32: ID=%d Type=%s", id, gd.Type)
}

// GetVectorAsFloat32 retrieves the vector at ID and converts it to []float32.
func (gd *GraphData) GetVectorAsFloat32(id uint32) ([]float32, error) {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	dims := gd.Dims
	paddedDims := gd.GetPaddedDims()

	if dims <= 0 {
		return nil, fmt.Errorf("invalid dimensions 0")
	}

	// 1. Float16 (Primary or Aux)
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

	// 3. Switch based on storage type
	switch gd.Type {
	case VectorTypeComplex64:
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
	case VectorTypeComplex128:
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
	case VectorTypeFloat64:
		chunk := gd.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			f64s := chunk[start : start+dims]
			res := make([]float32, dims)
			for i, v := range f64s {
				res[i] = float32(v)
			}
			return res, nil
		}
	case VectorTypeInt8:
		chunk := gd.GetVectorsInt8Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			i8s := chunk[start : start+dims]
			res := make([]float32, dims)
			for i, v := range i8s {
				res[i] = float32(v)
			}
			return res, nil
		}
	case VectorTypeUint8:
		chunk := gd.GetVectorsUint8Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			u8s := chunk[start : start+dims]
			res := make([]float32, dims)
			for i, v := range u8s {
				res[i] = float32(v)
			}
			return res, nil
		}
	case VectorTypeInt16:
		chunk := gd.GetVectorsInt16Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			i16s := chunk[start : start+dims]
			res := make([]float32, dims)
			for i, v := range i16s {
				res[i] = float32(v)
			}
			return res, nil
		}
	case VectorTypeUint16:
		chunk := gd.GetVectorsUint16Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			u16s := chunk[start : start+dims]
			res := make([]float32, dims)
			for i, v := range u16s {
				res[i] = float32(v)
			}
			return res, nil
		}
	case VectorTypeInt32:
		chunk := gd.GetVectorsInt32Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			i32s := chunk[start : start+dims]
			res := make([]float32, dims)
			for i, v := range i32s {
				res[i] = float32(v)
			}
			return res, nil
		}
	case VectorTypeUint32:
		chunk := gd.GetVectorsUint32Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			u32s := chunk[start : start+dims]
			res := make([]float32, dims)
			for i, v := range u32s {
				res[i] = float32(v)
			}
			return res, nil
		}
	case VectorTypeInt64:
		chunk := gd.GetVectorsInt64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			i64s := chunk[start : start+dims]
			res := make([]float32, dims)
			for i, v := range i64s {
				res[i] = float32(v)
			}
			return res, nil
		}
	case VectorTypeUint64:
		chunk := gd.GetVectorsUint64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			u64s := chunk[start : start+dims]
			res := make([]float32, dims)
			for i, v := range u64s {
				res[i] = float32(v)
			}
			return res, nil
		}
	}

	// SQ8/PQ require quantizer context (handled at store level)

	// DiskStore Fallback
	if gd.DiskStore != nil {
		vec, err := gd.DiskStore.Get(id)
		if err == nil {
			return vec, nil
		}
		return nil, fmt.Errorf("failed to retrieve vector from DiskStore: %w", err)
	}

	return nil, fmt.Errorf("vector data unavailable for ID %d (Type=%s)", id, gd.Type)
}

// GetVectorAsFloat32Into retrieves the vector at ID into the provided buffer.
func (gd *GraphData) GetVectorAsFloat32Into(id uint32, buf []float32) ([]float32, error) {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	dims := gd.Dims
	paddedDims := gd.GetPaddedDims()

	if dims <= 0 {
		return nil, fmt.Errorf("invalid dimensions 0")
	}

	// 1. Float16 (Primary or Aux)
	if (gd.VectorsF16 != nil && int(cID) < len(gd.VectorsF16)) || gd.Type == VectorTypeFloat16 {
		chunk := gd.GetVectorsF16Chunk(cID)
		if chunk != nil {
			stride := gd.GetPaddedDimsForType(VectorTypeFloat16)
			start := int(cOff) * stride
			f16s := chunk[start : start+dims]
			if cap(buf) < dims {
				buf = make([]float32, dims)
			}
			buf = buf[:dims]
			for i, v := range f16s {
				buf[i] = v.Float32()
			}
			return buf, nil
		}
	}

	// 2. primary Float32 - ZERO COPY optimization
	if gd.Type == VectorTypeFloat32 {
		chunk := gd.GetVectorsChunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			return chunk[start : start+dims], nil
		}
	}

	// 3. Switch based on storage type
	switch gd.Type {
	case VectorTypeComplex64:
		chunk := gd.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			c64s := chunk[start : start+dims]
			needed := dims * 2
			if cap(buf) < needed {
				buf = make([]float32, needed)
			}
			buf = buf[:needed]
			for i, v := range c64s {
				buf[2*i] = real(v)
				buf[2*i+1] = imag(v)
			}
			return buf, nil
		}
	case VectorTypeComplex128:
		chunk := gd.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			c128s := chunk[start : start+dims]
			needed := dims * 2
			if cap(buf) < needed {
				buf = make([]float32, needed)
			}
			buf = buf[:needed]
			for i, v := range c128s {
				buf[2*i] = float32(real(v))
				buf[2*i+1] = float32(imag(v))
			}
			return buf, nil
		}
	case VectorTypeFloat64:
		chunk := gd.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			f64s := chunk[start : start+dims]
			if cap(buf) < dims {
				buf = make([]float32, dims)
			}
			buf = buf[:dims]
			for i, v := range f64s {
				buf[i] = float32(v)
			}
			return buf, nil
		}
	case VectorTypeInt8:
		chunk := gd.GetVectorsInt8Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			i8s := chunk[start : start+dims]
			if cap(buf) < dims {
				buf = make([]float32, dims)
			}
			buf = buf[:dims]
			for i, v := range i8s {
				buf[i] = float32(v)
			}
			return buf, nil
		}
	case VectorTypeUint8:
		chunk := gd.GetVectorsUint8Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			u8s := chunk[start : start+dims]
			if cap(buf) < dims {
				buf = make([]float32, dims)
			}
			buf = buf[:dims]
			for i, v := range u8s {
				buf[i] = float32(v)
			}
			return buf, nil
		}
	case VectorTypeInt16:
		chunk := gd.GetVectorsInt16Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			i16s := chunk[start : start+dims]
			if cap(buf) < dims {
				buf = make([]float32, dims)
			}
			buf = buf[:dims]
			for i, v := range i16s {
				buf[i] = float32(v)
			}
			return buf, nil
		}
	case VectorTypeUint16:
		chunk := gd.GetVectorsUint16Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			u16s := chunk[start : start+dims]
			if cap(buf) < dims {
				buf = make([]float32, dims)
			}
			buf = buf[:dims]
			for i, v := range u16s {
				buf[i] = float32(v)
			}
			return buf, nil
		}
	case VectorTypeInt32:
		chunk := gd.GetVectorsInt32Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			i32s := chunk[start : start+dims]
			if cap(buf) < dims {
				buf = make([]float32, dims)
			}
			buf = buf[:dims]
			for i, v := range i32s {
				buf[i] = float32(v)
			}
			return buf, nil
		}
	case VectorTypeUint32:
		chunk := gd.GetVectorsUint32Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			u32s := chunk[start : start+dims]
			if cap(buf) < dims {
				buf = make([]float32, dims)
			}
			buf = buf[:dims]
			for i, v := range u32s {
				buf[i] = float32(v)
			}
			return buf, nil
		}
	case VectorTypeInt64:
		chunk := gd.GetVectorsInt64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			i64s := chunk[start : start+dims]
			if cap(buf) < dims {
				buf = make([]float32, dims)
			}
			buf = buf[:dims]
			for i, v := range i64s {
				buf[i] = float32(v)
			}
			return buf, nil
		}
	case VectorTypeUint64:
		chunk := gd.GetVectorsUint64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * paddedDims
			u64s := chunk[start : start+dims]
			if cap(buf) < dims {
				buf = make([]float32, dims)
			}
			buf = buf[:dims]
			for i, v := range u64s {
				buf[i] = float32(v)
			}
			return buf, nil
		}
	}

	// DiskStore Fallback
	if gd.DiskStore != nil {
		vec, err := gd.DiskStore.Get(id)
		if err == nil {
			return vec, nil
		}
		return nil, fmt.Errorf("failed to retrieve vector from DiskStore: %w", err)
	}

	return nil, fmt.Errorf("vector data unavailable for ID %d (Type=%s)", id, gd.Type)
}

// SetVector sets the vector at the given ID using the provided generic input.
func (gd *GraphData) SetVector(id uint32, vec any) error {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	dims := gd.Dims

	switch v := vec.(type) {
	case []float32:
		return gd.SetVectorFromFloat32(id, v)
	case []float16.Num:
		if (gd.VectorsF16 != nil && int(cID) < len(gd.VectorsF16)) || gd.Type == VectorTypeFloat16 {
			f16Chunk := gd.GetVectorsF16Chunk(cID)
			if f16Chunk != nil {
				start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeFloat16)
				copy(f16Chunk[start:start+dims], v)
				return nil
			}
		}
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
		switch gd.Type {
		case VectorTypeFloat64:
			chunk := gd.GetVectorsFloat64Chunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDimsForType(VectorTypeFloat64)
				copy(chunk[start:start+dims], v)
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
	case []int16:
		if gd.Type == VectorTypeInt16 {
			chunk := gd.GetVectorsInt16Chunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDims()
				copy(chunk[start:start+dims], v)
				return nil
			}
		}
	case []int32:
		if gd.Type == VectorTypeInt32 {
			chunk := gd.GetVectorsInt32Chunk(cID)
			if chunk != nil {
				start := int(cOff) * gd.GetPaddedDims()
				copy(chunk[start:start+dims], v)
				return nil
			}
		}
	case []int64:
		if gd.Type == VectorTypeInt64 {
			chunk := gd.GetVectorsInt64Chunk(cID)
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
func (gd *GraphData) GetVector(id uint32) (any, error) {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	dims := gd.Dims

	if dims <= 0 {
		return nil, fmt.Errorf("invalid dimensions 0")
	}

	switch gd.Type {
	case VectorTypeFloat32:
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
			return chunk[start : start+dims], nil
		}
	case VectorTypeFloat16:
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
	case VectorTypeUint8:
		chunk := gd.GetVectorsUint8Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDims()
			return chunk[start : start+dims], nil
		}
	case VectorTypeInt16:
		chunk := gd.GetVectorsInt16Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDims()
			return chunk[start : start+dims], nil
		}
	case VectorTypeUint16:
		chunk := gd.GetVectorsUint16Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDims()
			return chunk[start : start+dims], nil
		}
	case VectorTypeInt32:
		chunk := gd.GetVectorsInt32Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDims()
			return chunk[start : start+dims], nil
		}
	case VectorTypeUint32:
		chunk := gd.GetVectorsUint32Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDims()
			return chunk[start : start+dims], nil
		}
	case VectorTypeInt64:
		chunk := gd.GetVectorsInt64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDims()
			return chunk[start : start+dims], nil
		}
	case VectorTypeUint64:
		chunk := gd.GetVectorsUint64Chunk(cID)
		if chunk != nil {
			start := int(cOff) * gd.GetPaddedDims()
			return chunk[start : start+dims], nil
		}
	}
	return nil, fmt.Errorf("vector data unavailable for ID %d (Type=%s)", id, gd.Type)
}
