package store

import (
	"fmt"
	"sort"
	"unsafe"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// PrefetchBatch informs the underlying storage backend to prefetch data for blocks
// containing the specified vector indices.
func (dvs *DiskVectorStore) PrefetchBatch(indices []int) error {
	dvs.mu.RLock()
	backend := dvs.backend
	prefetcher, ok := backend.(storage.Prefetcher)
	if !ok {
		dvs.mu.RUnlock()
		return nil
	}

	blockMap := make(map[int]struct{})
	for _, idx := range indices {
		if !dvs.deleted[idx] {
			if bIdx := dvs.findBlock(idx); bIdx != -1 {
				blockMap[bIdx] = struct{}{}
			}
		}
	}

	blocksToPrefetch := make([]BlockEntry, 0, len(blockMap))
	for bIdx := range blockMap {
		blocksToPrefetch = append(blocksToPrefetch, dvs.blocks[bIdx])
	}
	dvs.mu.RUnlock()

	for _, b := range blocksToPrefetch {
		if b.Tier != storage.TierWarm {
			_ = prefetcher.Prefetch(b.Offset, int64(13+int(b.CompSize)))
		}
	}
	return nil
}

// GetVector decodes a single vector directly into dst with zero heap allocations.
func (dvs *DiskVectorStore) GetVector(idx int, dst []float32) error {
	return dvs.GetVectorInto(idx, dst)
}

// GetVectorInto decodes a single float32 vector directly into dst with zero heap allocations.
func (dvs *DiskVectorStore) GetVectorInto(idx int, dst []float32) error {
	dvs.mu.RLock()
	if dvs.deleted[idx] {
		dvs.mu.RUnlock()
		return fmt.Errorf("vector index %d is deleted", idx)
	}
	bIdx := dvs.findBlock(idx)
	if bIdx == -1 {
		dvs.mu.RUnlock()
		return fmt.Errorf("vector index %d out of bounds", idx)
	}
	block := dvs.blocks[bIdx]
	dim := dvs.dim
	tqEnc := dvs.tqEnc
	dvs.mu.RUnlock()

	if len(dst) < dim {
		return fmt.Errorf("dst buffer too small: got %d, need %d", len(dst), dim)
	}

	raw, err := dvs.fetchBlockDataWithEntry(bIdx, block)
	if err != nil {
		return err
	}

	localIdx := idx - block.StartIdx
	if localIdx < 0 || localIdx >= int(block.NumVectors) {
		return fmt.Errorf("vector %d: local index %d outside block", idx, localIdx)
	}

	if block.CompType == 3 && tqEnc != nil {
		return decodeTQRowInto(raw, localIdx, dim, tqEnc, dst)
	}

	offset := localIdx * dim * 4
	if offset+dim*4 > len(raw) {
		return fmt.Errorf("vector %d: offset exceeds payload", idx)
	}

	rawF32 := unsafe.Slice((*float32)(unsafe.Pointer(&raw[offset])), dim) // #nosec G103
	copy(dst[:dim], rawF32)
	return nil
}

// GetBatchInto retrieves multiple vectors into caller-provided/pooled row buffers dst with zero heap allocations.
func (dvs *DiskVectorStore) GetBatchInto(indices []int, dst [][]float32) error {
	if len(indices) == 0 {
		return nil
	}
	if len(dst) < len(indices) {
		return fmt.Errorf("dst slice has len %d, need at least %d", len(dst), len(indices))
	}
	for i, idx := range indices {
		if err := dvs.GetVectorInto(idx, dst[i]); err != nil {
			return err
		}
	}
	return nil
}

// GetVectorAnyInto decodes a single vector of any supported dtype directly into dst.
func (dvs *DiskVectorStore) GetVectorAnyInto(idx int, dst any) error {
	dvs.mu.RLock()
	if dvs.deleted[idx] {
		dvs.mu.RUnlock()
		return fmt.Errorf("vector index %d is deleted", idx)
	}
	bIdx := dvs.findBlock(idx)
	if bIdx == -1 {
		dvs.mu.RUnlock()
		return fmt.Errorf("vector index %d out of bounds", idx)
	}
	block := dvs.blocks[bIdx]
	dim := dvs.dim
	dataType := dvs.dataType
	tqEnc := dvs.tqEnc
	dvs.mu.RUnlock()

	raw, err := dvs.fetchBlockDataWithEntry(bIdx, block)
	if err != nil {
		return err
	}

	localIdx := idx - block.StartIdx
	if localIdx < 0 || localIdx >= int(block.NumVectors) {
		return fmt.Errorf("vector %d: local index %d outside block", idx, localIdx)
	}

	if block.CompType == 3 && tqEnc != nil {
		if d, ok := dst.([]float32); ok {
			return decodeTQRowInto(raw, localIdx, dim, tqEnc, d)
		}
		return fmt.Errorf("turboquant requires []float32 dst buffer, got %T", dst)
	}

	return decodeRawRowInto(raw, localIdx, dim, dataType, dst)
}

func decodeRawRowInto(raw []byte, localIdx, dim int, dataType types.VectorDataType, dst any) error {
	switch d := dst.(type) {
	case []float32:
		return copyRowGeneric(raw, localIdx, dim, d)
	case []float64:
		return copyRowGeneric(raw, localIdx, dim, d)
	case []int8:
		return copyRowGeneric(raw, localIdx, dim, d)
	case []uint8:
		return copyRowGeneric(raw, localIdx, dim, d)
	case []float16.Num:
		return copyRowGeneric(raw, localIdx, dim, d)
	case []int16:
		return copyRowGeneric(raw, localIdx, dim, d)
	case []uint16:
		return copyRowGeneric(raw, localIdx, dim, d)
	case []int32:
		return copyRowGeneric(raw, localIdx, dim, d)
	case []uint32:
		return copyRowGeneric(raw, localIdx, dim, d)
	case []int64:
		return copyRowGeneric(raw, localIdx, dim, d)
	case []uint64:
		return copyRowGeneric(raw, localIdx, dim, d)
	case []complex64:
		return copyRowGeneric(raw, localIdx, dim, d)
	case []complex128:
		return copyRowGeneric(raw, localIdx, dim, d)
	default:
		return fmt.Errorf("unsupported dst slice type %T for vector type %v", dst, dataType)
	}
}

func copyRowGeneric[T any](raw []byte, localIdx, dim int, dst []T) error {
	if len(dst) < dim {
		return fmt.Errorf("dst buffer too small: got %d, need %d", len(dst), dim)
	}
	var zero T
	elemSize := int(unsafe.Sizeof(zero))
	offset := localIdx * dim * elemSize
	if offset+dim*elemSize > len(raw) {
		return fmt.Errorf("row offset [%d,%d) exceeds block payload of %d bytes",
			offset, offset+dim*elemSize, len(raw))
	}
	copy(dst[:dim], unsafe.Slice((*T)(unsafe.Pointer(&raw[offset])), dim)) // #nosec G103
	return nil
}

// ScanRows traverses the specified indices with zero heap allocations,
// invoking fn for each decoded vector using a pooled row buffer.
func (dvs *DiskVectorStore) ScanRows(indices []int, fn func(idx int, row []float32) bool) error {
	dvs.mu.RLock()
	dim := dvs.dim
	dvs.mu.RUnlock()

	buf := GetFloat32RowBuffer(dim)
	defer PutFloat32RowBuffer(buf)

	for _, idx := range indices {
		if err := dvs.GetVectorInto(idx, buf); err != nil {
			return err
		}
		if !fn(idx, buf) {
			break
		}
	}
	return nil
}

// ScanRowsAny traverses the specified indices with zero heap allocations for any vector type.
func (dvs *DiskVectorStore) ScanRowsAny(indices []int, fn func(idx int, row any) bool) error {
	dvs.mu.RLock()
	dim := dvs.dim
	dataType := dvs.dataType
	dvs.mu.RUnlock()

	elemSize := dataType.ElementSize()
	if elemSize <= 0 {
		elemSize = 4
	}
	buf := GetByteRowBuffer(dim * elemSize)
	defer PutByteRowBuffer(buf)

	typedBuf := makeTypedSlice(dataType, buf, dim)
	for _, idx := range indices {
		if err := dvs.GetVectorAnyInto(idx, typedBuf); err != nil {
			return err
		}
		if !fn(idx, typedBuf) {
			break
		}
	}
	return nil
}

func makeTypedSlice(dt types.VectorDataType, raw []byte, dim int) any {
	switch dt {
	case types.VectorTypeFloat64:
		return unsafe.Slice((*float64)(unsafe.Pointer(&raw[0])), dim) // #nosec G103
	case types.VectorTypeInt8:
		return unsafe.Slice((*int8)(unsafe.Pointer(&raw[0])), dim) // #nosec G103
	case types.VectorTypeUint8:
		return raw[:dim]
	case types.VectorTypeFloat16:
		return unsafe.Slice((*float16.Num)(unsafe.Pointer(&raw[0])), dim) // #nosec G103
	case types.VectorTypeInt16:
		return unsafe.Slice((*int16)(unsafe.Pointer(&raw[0])), dim) // #nosec G103
	case types.VectorTypeUint16:
		return unsafe.Slice((*uint16)(unsafe.Pointer(&raw[0])), dim) // #nosec G103
	case types.VectorTypeInt32:
		return unsafe.Slice((*int32)(unsafe.Pointer(&raw[0])), dim) // #nosec G103
	case types.VectorTypeUint32:
		return unsafe.Slice((*uint32)(unsafe.Pointer(&raw[0])), dim) // #nosec G103
	case types.VectorTypeInt64:
		return unsafe.Slice((*int64)(unsafe.Pointer(&raw[0])), dim) // #nosec G103
	case types.VectorTypeUint64:
		return unsafe.Slice((*uint64)(unsafe.Pointer(&raw[0])), dim) // #nosec G103
	case types.VectorTypeComplex64:
		return unsafe.Slice((*complex64)(unsafe.Pointer(&raw[0])), dim) // #nosec G103
	case types.VectorTypeComplex128:
		return unsafe.Slice((*complex128)(unsafe.Pointer(&raw[0])), dim) // #nosec G103
	default:
		return unsafe.Slice((*float32)(unsafe.Pointer(&raw[0])), dim) // #nosec G103
	}
}

// GetBatch retrieves multiple vectors by their absolute indices.
func (dvs *DiskVectorStore) GetBatch(indices []int) ([][]float32, error) {
	if len(indices) == 0 {
		return nil, nil
	}

	dvs.mu.RLock()
	filtered := make([]int, 0, len(indices))
	for _, idx := range indices {
		if !dvs.deleted[idx] {
			filtered = append(filtered, idx)
		}
	}

	if len(filtered) == 0 {
		dvs.mu.RUnlock()
		return [][]float32{}, nil
	}

	blockRequestMap := make(map[int][]int)
	blockOf := make([]int, len(filtered))
	for i, idx := range filtered {
		bIdx := dvs.findBlock(idx)
		if bIdx == -1 {
			dvs.mu.RUnlock()
			return nil, fmt.Errorf("vector index %d out of bounds", idx)
		}
		blockOf[i] = bIdx
		blockRequestMap[bIdx] = append(blockRequestMap[bIdx], idx)
	}

	sortedBlockIdxs := make([]int, 0, len(blockRequestMap))
	for bIdx := range blockRequestMap {
		sortedBlockIdxs = append(sortedBlockIdxs, bIdx)
	}
	sort.Ints(sortedBlockIdxs)

	blockCopies := make(map[int]BlockEntry, len(sortedBlockIdxs))
	for _, bIdx := range sortedBlockIdxs {
		blockCopies[bIdx] = dvs.blocks[bIdx]
	}
	dim := dvs.dim
	tqEnc := dvs.tqEnc
	backend := dvs.backend
	dvs.mu.RUnlock()

	if prefetcher, ok := backend.(storage.Prefetcher); ok {
		for _, bIdx := range sortedBlockIdxs {
			b := blockCopies[bIdx]
			if b.Tier != storage.TierWarm {
				_ = prefetcher.Prefetch(b.Offset, int64(13+int(b.CompSize)))
			}
		}
	}

	blockData := make(map[int][]byte, len(sortedBlockIdxs))
	for _, bIdx := range sortedBlockIdxs {
		raw, err := dvs.fetchBlockDataWithEntry(bIdx, blockCopies[bIdx])
		if err != nil {
			return nil, err
		}
		blockData[bIdx] = raw
	}

	flatArena := make([]float32, len(filtered)*dim)
	results := make([][]float32, len(filtered))
	for i := range results {
		results[i] = flatArena[i*dim : (i+1)*dim]
	}

	for i, idx := range filtered {
		bIdx := blockOf[i]
		raw := blockData[bIdx]
		block := blockCopies[bIdx]
		localIdx := idx - block.StartIdx
		vec := results[i]

		if block.CompType == 3 && tqEnc != nil {
			stride := tqEnc.PackedSize()
			offset := localIdx * stride
			encoded := raw[offset : offset+stride]
			recon, err := tqEnc.Decode(encoded)
			if err == nil {
				copy(vec, recon)
			}
		} else {
			offset := localIdx * dim * 4
			rawF32 := unsafe.Slice((*float32)(unsafe.Pointer(&raw[offset])), dim) // #nosec G103
			copy(vec, rawF32)
		}
	}

	return results, nil
}

// GetBatchAny retrieves multiple vectors of any supported type by their absolute indices.
func (dvs *DiskVectorStore) GetBatchAny(indices []int) (any, error) {
	if len(indices) == 0 {
		return nil, nil
	}

	dvs.mu.RLock()
	dataType := dvs.dataType

	filtered := make([]int, 0, len(indices))
	for _, idx := range indices {
		if !dvs.deleted[idx] {
			filtered = append(filtered, idx)
		}
	}

	if len(filtered) == 0 {
		dvs.mu.RUnlock()
		return nil, nil
	}

	blockRequestMap := make(map[int][]int)
	blockOf := make([]int, len(filtered))
	for i, idx := range filtered {
		bIdx := dvs.findBlock(idx)
		if bIdx == -1 {
			dvs.mu.RUnlock()
			return nil, fmt.Errorf("vector index %d out of bounds", idx)
		}
		blockOf[i] = bIdx
		blockRequestMap[bIdx] = append(blockRequestMap[bIdx], idx)
	}

	sortedBlockIdxs := make([]int, 0, len(blockRequestMap))
	for bIdx := range blockRequestMap {
		sortedBlockIdxs = append(sortedBlockIdxs, bIdx)
	}
	sort.Ints(sortedBlockIdxs)

	blockCopies := make(map[int]BlockEntry, len(sortedBlockIdxs))
	for _, bIdx := range sortedBlockIdxs {
		blockCopies[bIdx] = dvs.blocks[bIdx]
	}
	dim := dvs.dim
	tqEnc := dvs.tqEnc
	backend := dvs.backend
	dvs.mu.RUnlock()

	if prefetcher, ok := backend.(storage.Prefetcher); ok {
		for _, bIdx := range sortedBlockIdxs {
			b := blockCopies[bIdx]
			if b.Tier != storage.TierWarm {
				_ = prefetcher.Prefetch(b.Offset, int64(13+int(b.CompSize)))
			}
		}
	}

	blockData := make(map[int][]byte, len(sortedBlockIdxs))
	for _, bIdx := range sortedBlockIdxs {
		raw, err := dvs.fetchBlockDataWithEntry(bIdx, blockCopies[bIdx])
		if err != nil {
			return nil, err
		}
		blockData[bIdx] = raw
	}

	switch dataType {
	case types.VectorTypeFloat64:
		return extractTypedRows[float64](blockData, blockOf, blockCopies, filtered, dim)
	case types.VectorTypeInt8:
		return extractTypedRows[int8](blockData, blockOf, blockCopies, filtered, dim)
	case types.VectorTypeUint8:
		return extractTypedRows[uint8](blockData, blockOf, blockCopies, filtered, dim)
	case types.VectorTypeFloat16:
		return extractTypedRows[float16.Num](blockData, blockOf, blockCopies, filtered, dim)
	case types.VectorTypeInt16:
		return extractTypedRows[int16](blockData, blockOf, blockCopies, filtered, dim)
	case types.VectorTypeUint16:
		return extractTypedRows[uint16](blockData, blockOf, blockCopies, filtered, dim)
	case types.VectorTypeInt32:
		return extractTypedRows[int32](blockData, blockOf, blockCopies, filtered, dim)
	case types.VectorTypeUint32:
		return extractTypedRows[uint32](blockData, blockOf, blockCopies, filtered, dim)
	case types.VectorTypeInt64:
		return extractTypedRows[int64](blockData, blockOf, blockCopies, filtered, dim)
	case types.VectorTypeUint64:
		return extractTypedRows[uint64](blockData, blockOf, blockCopies, filtered, dim)
	case types.VectorTypeComplex64:
		return extractTypedRows[complex64](blockData, blockOf, blockCopies, filtered, dim)
	case types.VectorTypeComplex128:
		return extractTypedRows[complex128](blockData, blockOf, blockCopies, filtered, dim)

	case types.VectorTypeFloat32:
		for _, bIdx := range sortedBlockIdxs {
			if blockCopies[bIdx].CompType != 3 {
				continue
			}
			flat := make([]float32, len(filtered)*dim)
			results := make([][]float32, len(filtered))
			for i := range results {
				results[i] = flat[i*dim : (i+1)*dim]
			}
			for i, idx := range filtered {
				b := blockOf[i]
				if err := decodeTQRowInto(blockData[b], idx-blockCopies[b].StartIdx, dim, tqEnc, results[i]); err != nil {
					return nil, err
				}
			}
			return results, nil
		}
		return extractTypedRows[float32](blockData, blockOf, blockCopies, filtered, dim)

	case types.VectorTypeTQ:
		flat := make([]float32, len(filtered)*dim)
		results := make([][]float32, len(filtered))
		for i := range results {
			results[i] = flat[i*dim : (i+1)*dim]
		}
		for i, idx := range filtered {
			b := blockOf[i]
			if err := decodeTQRowInto(blockData[b], idx-blockCopies[b].StartIdx, dim, tqEnc, results[i]); err != nil {
				return nil, err
			}
		}
		return results, nil

	default:
		return nil, fmt.Errorf("GetBatchAny: unsupported vector data type %v", dataType)
	}
}

func extractTypedRows[T any](
	blockData map[int][]byte,
	blockOf []int,
	blockCopies map[int]BlockEntry,
	filtered []int,
	dim int,
) ([][]T, error) {
	var zero T
	elemSize := int(unsafe.Sizeof(zero))
	if elemSize == 0 {
		return nil, fmt.Errorf("extractTypedRows: %T has zero size", zero)
	}
	rowBytes := dim * elemSize

	flatArena := make([]T, len(filtered)*dim)
	results := make([][]T, len(filtered))
	for i := range results {
		results[i] = flatArena[i*dim : (i+1)*dim]
	}

	for i, idx := range filtered {
		if i >= len(blockOf) {
			return nil, fmt.Errorf("vector %d: no block mapping for index %d of %d", idx, i, len(filtered))
		}
		bIdx := blockOf[i]
		raw := blockData[bIdx]
		block := blockCopies[bIdx]

		localIdx := idx - block.StartIdx
		if localIdx < 0 || localIdx >= int(block.NumVectors) {
			return nil, fmt.Errorf("vector %d: local index %d outside block of %d vectors",
				idx, localIdx, block.NumVectors)
		}
		offset := localIdx * rowBytes
		if offset+rowBytes > len(raw) {
			return nil, fmt.Errorf("vector %d: row [%d,%d) exceeds block payload of %d bytes",
				idx, offset, offset+rowBytes, len(raw))
		}

		copy(results[i], unsafe.Slice((*T)(unsafe.Pointer(&raw[offset])), dim)) // #nosec G103
	}
	return results, nil
}

func decodeTQRowInto(raw []byte, localIdx, dim int, tqEnc *index.TurboQuantEncoder, dst []float32) error {
	if tqEnc == nil {
		return fmt.Errorf("turboquant block but no encoder available")
	}
	stride := tqEnc.PackedSize()
	offset := localIdx * stride
	if offset < 0 || offset+stride > len(raw) {
		return fmt.Errorf("turboquant row %d exceeds block payload of %d bytes", localIdx, len(raw))
	}
	recon, err := tqEnc.Decode(raw[offset : offset+stride])
	if err != nil {
		return fmt.Errorf("turboquant decode of row %d: %w", localIdx, err)
	}
	if len(recon) < dim {
		return fmt.Errorf("turboquant decode of row %d returned %d components, need %d",
			localIdx, len(recon), dim)
	}
	copy(dst[:dim], recon[:dim])
	return nil
}
