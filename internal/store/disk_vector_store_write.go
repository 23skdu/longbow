package store

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/pierrec/lz4/v4"
)

// BatchAppendArrow appends vectors directly from an Arrow RecordBatch.
// This is significantly faster than BatchAppend as it avoids row-by-row extraction.
func (dvs *DiskVectorStore) BatchAppendArrow(rec arrow.RecordBatch, colIdx int) (int, error) {
	if rec == nil {
		return 0, nil
	}

	numRows := int(rec.NumRows())
	if numRows == 0 {
		return 0, nil
	}

	col := rec.Column(colIdx)
	listArr, ok := col.(*array.FixedSizeList)
	if !ok {
		return 0, fmt.Errorf("column %d is not a FixedSizeList", colIdx)
	}

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	offset := listArr.Data().Offset()

	var dataSlice []byte
	var elemSize int

	var inferredType types.VectorDataType
	switch valuesArr := listArr.ListValues().(type) {
	case *array.Float32:
		inferredType = types.VectorTypeFloat32
		elemSize = 4
		vals := valuesArr.Float32Values()
		start := offset * width
		end := start + numRows*width
		if start < 0 || end > len(vals) {
			return 0, fmt.Errorf("index out of bounds")
		}
		slice := vals[start:end]
		if len(slice) > 0 {
			dataSlice = unsafe.Slice((*byte)(unsafe.Pointer(&slice[0])), len(slice)*4) // #nosec G103
		}
	case *array.Float64:
		inferredType = types.VectorTypeFloat64
		elemSize = 8
		vals := valuesArr.Float64Values()
		start := offset * width
		end := start + numRows*width
		if start < 0 || end > len(vals) {
			return 0, fmt.Errorf("index out of bounds")
		}
		slice := vals[start:end]
		if len(slice) > 0 {
			dataSlice = unsafe.Slice((*byte)(unsafe.Pointer(&slice[0])), len(slice)*8) // #nosec G103
		}
	case *array.Int8:
		inferredType = types.VectorTypeInt8
		elemSize = 1
		vals := valuesArr.Int8Values()
		start := offset * width
		end := start + numRows*width
		if start < 0 || end > len(vals) {
			return 0, fmt.Errorf("index out of bounds")
		}
		slice := vals[start:end]
		if len(slice) > 0 {
			dataSlice = unsafe.Slice((*byte)(unsafe.Pointer(&slice[0])), len(slice)*1) // #nosec G103
		}
	case *array.Uint8:
		inferredType = types.VectorTypeUint8
		elemSize = 1
		vals := valuesArr.Uint8Values()
		start := offset * width
		end := start + numRows*width
		if start < 0 || end > len(vals) {
			return 0, fmt.Errorf("index out of bounds")
		}
		slice := vals[start:end]
		if len(slice) > 0 {
			dataSlice = unsafe.Slice((*byte)(unsafe.Pointer(&slice[0])), len(slice)*1) // #nosec G103
		}
	case *array.Float16:
		inferredType = types.VectorTypeFloat16
		elemSize = 2
		vals := valuesArr.Values()
		start := offset * width
		end := start + numRows*width
		if start < 0 || end > len(vals) {
			return 0, fmt.Errorf("index out of bounds")
		}
		slice := vals[start:end]
		if len(slice) > 0 {
			dataSlice = unsafe.Slice((*byte)(unsafe.Pointer(&slice[0])), len(slice)*2) // #nosec G103
		}
	case *array.Int16:
		inferredType = types.VectorTypeInt16
		elemSize = 2
		vals := valuesArr.Int16Values()
		start := offset * width
		end := start + numRows*width
		if start < 0 || end > len(vals) {
			return 0, fmt.Errorf("index out of bounds")
		}
		slice := vals[start:end]
		if len(slice) > 0 {
			dataSlice = unsafe.Slice((*byte)(unsafe.Pointer(&slice[0])), len(slice)*2) // #nosec G103
		}
	case *array.Uint16:
		inferredType = types.VectorTypeUint16
		elemSize = 2
		vals := valuesArr.Uint16Values()
		start := offset * width
		end := start + numRows*width
		if start < 0 || end > len(vals) {
			return 0, fmt.Errorf("index out of bounds")
		}
		slice := vals[start:end]
		if len(slice) > 0 {
			dataSlice = unsafe.Slice((*byte)(unsafe.Pointer(&slice[0])), len(slice)*2) // #nosec G103
		}
	case *array.Int32:
		inferredType = types.VectorTypeInt32
		elemSize = 4
		vals := valuesArr.Int32Values()
		start := offset * width
		end := start + numRows*width
		if start < 0 || end > len(vals) {
			return 0, fmt.Errorf("index out of bounds")
		}
		slice := vals[start:end]
		if len(slice) > 0 {
			dataSlice = unsafe.Slice((*byte)(unsafe.Pointer(&slice[0])), len(slice)*4) // #nosec G103
		}
	case *array.Uint32:
		inferredType = types.VectorTypeUint32
		elemSize = 4
		vals := valuesArr.Uint32Values()
		start := offset * width
		end := start + numRows*width
		if start < 0 || end > len(vals) {
			return 0, fmt.Errorf("index out of bounds")
		}
		slice := vals[start:end]
		if len(slice) > 0 {
			dataSlice = unsafe.Slice((*byte)(unsafe.Pointer(&slice[0])), len(slice)*4) // #nosec G103
		}
	case *array.Int64:
		inferredType = types.VectorTypeInt64
		elemSize = 8
		vals := valuesArr.Int64Values()
		start := offset * width
		end := start + numRows*width
		if start < 0 || end > len(vals) {
			return 0, fmt.Errorf("index out of bounds")
		}
		slice := vals[start:end]
		if len(slice) > 0 {
			dataSlice = unsafe.Slice((*byte)(unsafe.Pointer(&slice[0])), len(slice)*8) // #nosec G103
		}
	case *array.Uint64:
		inferredType = types.VectorTypeUint64
		elemSize = 8
		vals := valuesArr.Uint64Values()
		start := offset * width
		end := start + numRows*width
		if start < 0 || end > len(vals) {
			return 0, fmt.Errorf("index out of bounds")
		}
		slice := vals[start:end]
		if len(slice) > 0 {
			dataSlice = unsafe.Slice((*byte)(unsafe.Pointer(&slice[0])), len(slice)*8) // #nosec G103
		}
	default:
		return 0, fmt.Errorf("unsupported list element type: %T", valuesArr)
	}

	lenBytes := numRows * width * elemSize

	dvs.writeMu.Lock()
	defer dvs.writeMu.Unlock()

	dvs.mu.RLock()
	compAlg := dvs.compression
	tqBits := dvs.tqBits
	tqEnc := dvs.tqEnc
	dvs.mu.RUnlock()

	var dataToWrite []byte
	var compType byte // 0: none, 1: zstd, 2: lz4

	// 2. Compress
	if tqBits > 0 && tqEnc != nil && elemSize == 4 {
		compType = 3 // TurboQuant
		stride := tqEnc.PackedSize()
		dataToWrite = make([]byte, 0, numRows*stride)
		for i := 0; i < numRows; i++ {
			start := i * width * 4
			end := start + width*4
			vecSlice := dataSlice[start:end]
			vecFloat := unsafe.Slice((*float32)(unsafe.Pointer(&vecSlice[0])), width) // #nosec G103
			encoded, err := tqEnc.Encode(vecFloat)
			if err != nil {
				return 0, err
			}
			dataToWrite = append(dataToWrite, encoded...)
		}
	} else {
		switch compAlg {
		case "zstd":
			dataToWrite = dvs.zstdEnc.EncodeAll(dataSlice, nil)
			compType = 1
		case "lz4":
			maxLen := lz4.CompressBlockBound(len(dataSlice))
			compressed := make([]byte, maxLen)
			n, err := lz4.CompressBlock(dataSlice, compressed, nil)
			if err != nil {
				return 0, fmt.Errorf("lz4 compression failed: %w", err)
			}
			dataToWrite = compressed[:n]
			compType = 2
		default:
			dataToWrite = dataSlice
			compType = 0
		}
	}

	// 3. Write Block
	header := make([]byte, 13)
	binary.LittleEndian.PutUint32(header[0:4], 0x56434D50)
	header[4] = compType
	binary.LittleEndian.PutUint32(header[5:9], uint32(lenBytes))          // #nosec G115
	binary.LittleEndian.PutUint32(header[9:13], uint32(len(dataToWrite))) // #nosec G115

	writeOffset, _ := dvs.backend.Size()
	if _, err := dvs.backend.WriteAt(header, writeOffset); err != nil {
		return 0, err
	}
	if _, err := dvs.backend.WriteAt(dataToWrite, writeOffset+13); err != nil {
		return 0, err
	}

	if err := dvs.backend.Sync(); err != nil {
		return 0, err
	}

	// Update metadata under short in-memory mutex lock
	dvs.mu.Lock()
	dvs.dataType = inferredType
	dvs.blocks = append(dvs.blocks, BlockEntry{
		Offset:     writeOffset,
		CompSize:   uint32(len(dataToWrite)), // #nosec G115
		RawSize:    uint32(lenBytes),         // #nosec G115
		NumVectors: numRows,
		StartIdx:   dvs.totalCount,
		CompType:   compType,
		Tier:       storage.TierHot,
		HotKey:     hotCacheKey(len(dvs.blocks)),
		CreatedAt:  time.Now(),
	})
	dvs.totalCount += numRows
	dvs.mu.Unlock()

	return numRows, nil
}

// BatchAppend appends a list of raw vectors to the disk store.
func (dvs *DiskVectorStore) BatchAppend(vectors [][]float32) (int, error) {
	if len(vectors) == 0 {
		return 0, nil
	}

	dvs.writeMu.Lock()
	defer dvs.writeMu.Unlock()

	dvs.mu.RLock()
	dim := dvs.dim
	compAlg := dvs.compression
	dvs.mu.RUnlock()

	// 1. Serialize vectors to raw bytes (Little Endian Float32)
	raw := make([]byte, len(vectors)*dim*4)
	for i, v := range vectors {
		for j, f := range v {
			binary.LittleEndian.PutUint32(raw[(i*dim+j)*4:], math.Float32bits(f))
		}
	}

	var dataToWrite []byte
	var compType byte // 0: none, 1: zstd, 2: lz4

	// 2. Compress
	switch compAlg {
	case "zstd":
		dataToWrite = dvs.zstdEnc.EncodeAll(raw, nil)
		compType = 1
	case "lz4":
		maxLen := lz4.CompressBlockBound(len(raw))
		compressed := make([]byte, maxLen)
		n, err := lz4.CompressBlock(raw, compressed, nil)
		if err != nil {
			return 0, fmt.Errorf("lz4 compression failed: %w", err)
		}
		dataToWrite = compressed[:n]
		compType = 2
	default:
		dataToWrite = raw
		compType = 0
	}

	// 3. Write Block: [Magic:4b][CompType:1b][RawSize:4b][CompSize:4b][Data...]
	header := make([]byte, 13)
	binary.LittleEndian.PutUint32(header[0:4], 0x56434D50)
	header[4] = compType
	binary.LittleEndian.PutUint32(header[5:9], uint32(len(raw)))          // #nosec G115
	binary.LittleEndian.PutUint32(header[9:13], uint32(len(dataToWrite))) // #nosec G115

	offset, _ := dvs.backend.Size()

	if _, err := dvs.backend.WriteAt(header, offset); err != nil {
		return 0, err
	}
	if _, err := dvs.backend.WriteAt(dataToWrite, offset+13); err != nil {
		return 0, err
	}

	if err := dvs.backend.Sync(); err != nil {
		return 0, err
	}

	// Update metadata under short in-memory mutex lock
	dvs.mu.Lock()
	dvs.blocks = append(dvs.blocks, BlockEntry{
		Offset:     offset,
		CompSize:   uint32(len(dataToWrite)), // #nosec G115
		RawSize:    uint32(len(raw)),         // #nosec G115
		NumVectors: len(vectors),
		StartIdx:   dvs.totalCount,
		CompType:   compType,
		Tier:       storage.TierHot,
		HotKey:     hotCacheKey(len(dvs.blocks)),
		CreatedAt:  time.Now(),
	})
	dvs.totalCount += len(vectors)
	dvs.mu.Unlock()

	return len(vectors), nil
}

// BatchAppendAny appends a slice of typed vector slices to the disk store.
func (dvs *DiskVectorStore) BatchAppendAny(vectors any) (int, error) {
	if vectors == nil {
		return 0, nil
	}

	var numVectors int
	var inferredType types.VectorDataType
	var elemSize int
	var raw []byte

	dvs.mu.RLock()
	dim := dvs.dim
	compAlg := dvs.compression
	dvs.mu.RUnlock()

	switch v := vectors.(type) {
	case [][]float32:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeFloat32
		elemSize = 4
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*elemSize)) // #nosec G103
			}
		}
	case [][]float64:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeFloat64
		elemSize = 8
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*elemSize)) // #nosec G103
			}
		}
	case [][]int8:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeInt8
		elemSize = 1
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*elemSize)) // #nosec G103
			}
		}
	case [][]uint8:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeUint8
		elemSize = 1
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], vec)
			}
		}
	case [][]float16.Num:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeFloat16
		elemSize = 2
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*elemSize)) // #nosec G103
			}
		}
	case [][]int16:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeInt16
		elemSize = 2
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*elemSize)) // #nosec G103
			}
		}
	case [][]uint16:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeUint16
		elemSize = 2
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*elemSize)) // #nosec G103
			}
		}
	case [][]int32:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeInt32
		elemSize = 4
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*elemSize)) // #nosec G103
			}
		}
	case [][]uint32:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeUint32
		elemSize = 4
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*elemSize)) // #nosec G103
			}
		}
	case [][]int64:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeInt64
		elemSize = 8
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*elemSize)) // #nosec G103
			}
		}
	case [][]uint64:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeUint64
		elemSize = 8
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*elemSize)) // #nosec G103
			}
		}
	case [][]complex64:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeComplex64
		elemSize = 8
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*elemSize)) // #nosec G103
			}
		}
	case [][]complex128:
		numVectors = len(v)
		if numVectors == 0 {
			return 0, nil
		}
		inferredType = types.VectorTypeComplex128
		elemSize = 16
		raw = make([]byte, numVectors*dim*elemSize)
		for i, vec := range v {
			if len(vec) > 0 {
				copy(raw[i*dim*elemSize:], unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*elemSize)) // #nosec G103
			}
		}
	default:
		return 0, fmt.Errorf("unsupported vector slice type: %T", vectors)
	}

	dvs.writeMu.Lock()
	defer dvs.writeMu.Unlock()

	var dataToWrite []byte
	var compType byte // 0: none, 1: zstd, 2: lz4

	switch compAlg {
	case "zstd":
		dataToWrite = dvs.zstdEnc.EncodeAll(raw, nil)
		compType = 1
	case "lz4":
		maxLen := lz4.CompressBlockBound(len(raw))
		compressed := make([]byte, maxLen)
		n, err := lz4.CompressBlock(raw, compressed, nil)
		if err != nil {
			return 0, fmt.Errorf("lz4 compression failed: %w", err)
		}
		dataToWrite = compressed[:n]
		compType = 2
	default:
		dataToWrite = raw
		compType = 0
	}

	header := make([]byte, 13)
	binary.LittleEndian.PutUint32(header[0:4], 0x56434D50)
	header[4] = compType
	binary.LittleEndian.PutUint32(header[5:9], uint32(len(raw)))          // #nosec G115
	binary.LittleEndian.PutUint32(header[9:13], uint32(len(dataToWrite))) // #nosec G115

	offset, _ := dvs.backend.Size()
	if _, err := dvs.backend.WriteAt(header, offset); err != nil {
		return 0, err
	}
	if _, err := dvs.backend.WriteAt(dataToWrite, offset+13); err != nil {
		return 0, err
	}
	if err := dvs.backend.Sync(); err != nil {
		return 0, err
	}

	dvs.mu.Lock()
	dvs.dataType = inferredType
	dvs.blocks = append(dvs.blocks, BlockEntry{
		Offset:     offset,
		CompSize:   uint32(len(dataToWrite)), // #nosec G115
		RawSize:    uint32(len(raw)),         // #nosec G115
		NumVectors: numVectors,
		StartIdx:   dvs.totalCount,
		CompType:   compType,
		Tier:       storage.TierHot,
		HotKey:     hotCacheKey(len(dvs.blocks)),
		CreatedAt:  time.Now(),
	})
	dvs.totalCount += numVectors
	dvs.mu.Unlock()

	return numVectors, nil
}
