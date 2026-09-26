package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// BlockEntry tracks a compressed block in the disk store.
type BlockEntry struct {
	Offset     int64
	CompSize   uint32
	RawSize    uint32
	NumVectors int
	StartIdx   int
	CompType   byte
	Tier       storage.StorageTier
	RemoteKey  string
	CreatedAt  time.Time
}

// DiskVectorStore provides append-only persistent storage for vectors with block-level compression.
type DiskVectorStore struct {
	path        string
	dim         int
	backend     storage.StorageBackend
	mu          sync.RWMutex
	writeMu     sync.Mutex // isolates disk writes and flushes from read traversal (RCU/double-buffering)
	compression string // "zstd", "lz4", "none"
	zstdEnc     *zstd.Encoder
	zstdDec     *zstd.Decoder
	blocks      []BlockEntry
	totalCount  int
	dataType    types.VectorDataType
	tqBits      int
	tqEnc       *index.TurboQuantEncoder

	// Tombstone deletion - tracks deleted indices (consistent with ArrowHNSW)
	deleted map[int]bool

	// Tiered Storage
	remote     storage.RemoteStorage
	blockCache *storage.LRUCache
}

// NewDiskVectorStore creates a new DiskVectorStore with default settings.
func NewDiskVectorStore(path string, dim int) (*DiskVectorStore, error) {
	return NewDiskVectorStoreWithConfig(path, dim, false, false)
}

// NewDiskVectorStoreWithConfig creates a new DiskVectorStore with specific I/O settings.
func NewDiskVectorStoreWithConfig(path string, dim int, useUring, useDirect bool) (*DiskVectorStore, error) {
	backend, err := storage.NewStorageBackend(path, useUring, useDirect)
	if err != nil {
		return nil, err
	}

	z, _ := zstd.NewWriter(nil)
	zd, _ := zstd.NewReader(nil)

	dvs := &DiskVectorStore{
		path:        path,
		dim:         dim,
		backend:     backend,
		compression: "zstd",
		zstdEnc:     z,
		zstdDec:     zd,
		deleted:     make(map[int]bool),
		// Default hot-block cache for temporal lookups re-reading the same
		// compressed blocks. SetTieredConfig replaces it with the tier cache.
		blockCache: storage.NewLRUCache(16 * 1024 * 1024),
	}

	return dvs, nil
}

// SetTieredConfig configures the remote storage backend and cache size for tiered storage.
// The same LRU also caches hot (local) blocks under synthetic "hot:" keys.
func (dvs *DiskVectorStore) SetTieredConfig(remote storage.RemoteStorage, cacheMB int) {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()
	dvs.remote = remote
	if cacheMB > 0 {
		dvs.blockCache = storage.NewLRUCache(int64(cacheMB) * 1024 * 1024)
	}
}

// SetCompression updates the compression algorithm used for new blocks.
func (dvs *DiskVectorStore) SetCompression(c string) {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()
	dvs.compression = c
}

// SetTurboQuant configures the store to use TurboQuant encoding.
func (dvs *DiskVectorStore) SetTurboQuant(bits int) {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()
	dvs.tqBits = bits
	dvs.tqEnc = index.NewTurboQuantEncoder(dvs.dim, bits, 42)
}

// Close releases resources and closes the underlying storage backend.
func (dvs *DiskVectorStore) Close() error {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()
	if dvs.backend != nil {
		return dvs.backend.Close()
	}
	return nil
}

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
		CreatedAt:  time.Now(),
	})
	dvs.totalCount += len(vectors)
	dvs.mu.Unlock()

	return len(vectors), nil
}

func (dvs *DiskVectorStore) findBlock(idx int) int {
	l, r := 0, len(dvs.blocks)-1
	res := -1
	for l <= r {
		mid := (l + r) / 2
		if dvs.blocks[mid].StartIdx <= idx {
			res = mid
			l = mid + 1
		} else {
			r = mid - 1
		}
	}
	return res
}

// PrefetchBatch advises the OS kernel to asynchronously read-ahead blocks for the specified vector indices.
func (dvs *DiskVectorStore) PrefetchBatch(indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	prefetcher, ok := dvs.backend.(storage.Prefetcher)
	if !ok {
		return nil
	}

	dvs.mu.RLock()
	blockSeen := make(map[int]struct{}, len(indices))
	for _, idx := range indices {
		if !dvs.deleted[idx] {
			bIdx := dvs.findBlock(idx)
			if bIdx >= 0 && bIdx < len(dvs.blocks) {
				blockSeen[bIdx] = struct{}{}
			}
		}
	}
	blocksToPrefetch := make([]BlockEntry, 0, len(blockSeen))
	for bIdx := range blockSeen {
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
	// Pre-compute block membership once; reused by the result assembly loop
	// so findBlock is not re-run per element.
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

	// Fetch blocks in ascending offset order for sequential I/O locality.
	sortedBlockIdxs := make([]int, 0, len(blockRequestMap))
	for bIdx := range blockRequestMap {
		sortedBlockIdxs = append(sortedBlockIdxs, bIdx)
	}
	sort.Ints(sortedBlockIdxs)

	// Snapshot block metadata and state under lock, then release before disk I/O
	blockCopies := make(map[int]BlockEntry, len(sortedBlockIdxs))
	for _, bIdx := range sortedBlockIdxs {
		blockCopies[bIdx] = dvs.blocks[bIdx]
	}
	dim := dvs.dim
	tqEnc := dvs.tqEnc
	backend := dvs.backend
	dvs.mu.RUnlock()

	// Asynchronous kernel read-ahead for local blocks to eliminate page faults
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

	results := make([][]float32, len(filtered))
	for i, idx := range filtered {
		bIdx := blockOf[i]
		raw := blockData[bIdx]
		block := blockCopies[bIdx]
		localIdx := idx - block.StartIdx

		vec := make([]float32, dim)
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
			for j := 0; j < dim; j++ {
				bits := binary.LittleEndian.Uint32(raw[offset+j*4 : offset+(j+1)*4])
				vec[j] = math.Float32frombits(bits)
			}
		}
		results[i] = vec
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

	// Block request map: which global indices belong to which block.
	// findBlock is computed once per index here, then reused by the result
	// assembly loop (previously it was re-run per element — O(n log b) waste).
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

	// Fetch in ascending block order for sequential I/O locality.
	sortedBlockIdxs := make([]int, 0, len(blockRequestMap))
	for bIdx := range blockRequestMap {
		sortedBlockIdxs = append(sortedBlockIdxs, bIdx)
	}
	sort.Ints(sortedBlockIdxs)

	// Snapshot block metadata and state under lock, then release before disk I/O
	blockCopies := make(map[int]BlockEntry, len(sortedBlockIdxs))
	for _, bIdx := range sortedBlockIdxs {
		blockCopies[bIdx] = dvs.blocks[bIdx]
	}
	dim := dvs.dim
	tqEnc := dvs.tqEnc
	backend := dvs.backend
	dvs.mu.RUnlock()

	// Asynchronous kernel read-ahead for local blocks to eliminate page faults
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

	elemSize := 4
	switch dataType {
	case types.VectorTypeFloat64:
		elemSize = 8
	case types.VectorTypeInt8, types.VectorTypeUint8:
		elemSize = 1
	case types.VectorTypeFloat16:
		elemSize = 2
	}

	switch dataType {
	case types.VectorTypeFloat64:
		results := make([][]float64, len(filtered))
		for i, idx := range filtered {
			bIdx := blockOf[i]
			raw := blockData[bIdx]
			block := blockCopies[bIdx]
			localIdx := idx - block.StartIdx

			vec := make([]float64, dim)
			offset := localIdx * dim * elemSize
			for j := 0; j < dim; j++ {
				bits := binary.LittleEndian.Uint64(raw[offset+j*8 : offset+(j+1)*8])
				vec[j] = math.Float64frombits(bits)
			}
			results[i] = vec
		}
		return results, nil

	case types.VectorTypeInt8:
		results := make([][]int8, len(filtered))
		for i, idx := range filtered {
			bIdx := blockOf[i]
			raw := blockData[bIdx]
			block := blockCopies[bIdx]
			localIdx := idx - block.StartIdx

			vec := make([]int8, dim)
			offset := localIdx * dim * elemSize
			for j := 0; j < dim; j++ {
				vec[j] = int8(raw[offset+j]) // #nosec G115 -- bit reinterpretation of int8 stored as byte on disk
			}
			results[i] = vec
		}
		return results, nil

	case types.VectorTypeUint8:
		results := make([][]uint8, len(filtered))
		for i, idx := range filtered {
			bIdx := blockOf[i]
			raw := blockData[bIdx]
			block := blockCopies[bIdx]
			localIdx := idx - block.StartIdx

			vec := make([]uint8, dim)
			offset := localIdx * dim * elemSize
			copy(vec, raw[offset:offset+dim])
			results[i] = vec
		}
		return results, nil

	case types.VectorTypeFloat16:
		results := make([][]float16.Num, len(filtered))
		for i, idx := range filtered {
			bIdx := blockOf[i]
			raw := blockData[bIdx]
			block := blockCopies[bIdx]
			localIdx := idx - block.StartIdx

			vec := make([]float16.Num, dim)
			offset := localIdx * dim * elemSize
			for j := 0; j < dim; j++ {
				vec[j] = float16.FromLEBytes(raw[offset+j*2 : offset+(j+1)*2])
			}
			results[i] = vec
		}
		return results, nil

	default:
		results := make([][]float32, len(filtered))
		for i, idx := range filtered {
			bIdx := blockOf[i]
			raw := blockData[bIdx]
			block := blockCopies[bIdx]
			localIdx := idx - block.StartIdx

			vec := make([]float32, dim)
			if block.CompType == 3 && tqEnc != nil {
				stride := tqEnc.PackedSize()
				offset := localIdx * stride
				encoded := raw[offset : offset+stride]
				recon, err := tqEnc.Decode(encoded)
				if err == nil {
					copy(vec, recon)
				}
			} else {
				offset := localIdx * dim * elemSize
				for j := 0; j < dim; j++ {
					bits := binary.LittleEndian.Uint32(raw[offset+j*4 : offset+(j+1)*4])
					vec[j] = math.Float32frombits(bits)
				}
			}
			results[i] = vec
		}
		return results, nil
	}
}

// readBufPool reuses the 13+CompSize header+payload buffer used for local
// block reads. The buffer is only held for the duration of ReadAt +
// decompressBlock; decompressBlock either returns a fresh slice (zstd/lz4)
// or a subslice of buf (none/TQ) which is copied before the buffer is
// returned to the pool.
var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 64*1024)
		return &b
	},
}

// hotCacheKey is the synthetic LRU key for a local (hot-tier) block.
func hotCacheKey(bIdx int) string {
	return "hot:" + strconv.Itoa(bIdx)
}

// FetchBlockData retrieves the decompressed data for the block at bIdx.
func (dvs *DiskVectorStore) FetchBlockData(bIdx int) ([]byte, error) {
	dvs.mu.RLock()
	if bIdx < 0 || bIdx >= len(dvs.blocks) {
		dvs.mu.RUnlock()
		return nil, fmt.Errorf("invalid block index %d", bIdx)
	}
	block := dvs.blocks[bIdx]
	dvs.mu.RUnlock()
	return dvs.fetchBlockDataWithEntry(bIdx, block)
}

func (dvs *DiskVectorStore) fetchBlockDataWithEntry(bIdx int, block BlockEntry) ([]byte, error) {
	if block.Tier == storage.TierWarm {
		// Check Cache
		if dvs.blockCache != nil {
			if data, ok := dvs.blockCache.Get(block.RemoteKey); ok {
				return data, nil
			}
		}

		// Fetch from Remote
		if dvs.remote == nil {
			return nil, fmt.Errorf("remote storage not configured for block %d", bIdx)
		}
		rc, err := dvs.remote.Get(context.Background(), block.RemoteKey)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch from remote: %w", err)
		}
		defer rc.Close()
		compressed, err := io.ReadAll(rc)
		if err != nil {
			return nil, err
		}

		raw, err := dvs.decompressBlock(compressed)
		if err != nil {
			return nil, err
		}

		// Cache decompressed payload under the remote key.
		if dvs.blockCache != nil {
			dvs.blockCache.Put(block.RemoteKey, raw)
		}
		return raw, nil
	}

	// Hot-tier LRU: temporal lookups re-read the same compressed blocks.
	cacheKey := hotCacheKey(bIdx)
	if dvs.blockCache != nil {
		if data, ok := dvs.blockCache.Get(cacheKey); ok {
			return data, nil
		}
	}

	// Local Read via pooled buffer.
	need := 13 + int(block.CompSize)
	bp := readBufPool.Get().(*[]byte)
	buf := *bp
	if cap(buf) < need {
		buf = make([]byte, need)
	}
	buf = buf[:need]
	_, err := dvs.backend.ReadAt(buf, block.Offset)
	if err != nil {
		*bp = buf
		readBufPool.Put(bp)
		return nil, err
	}
	raw, derr := dvs.decompressBlock(buf)
	if derr != nil {
		*bp = buf
		readBufPool.Put(bp)
		return nil, derr
	}
	// zstd/lz4 already returned a fresh slice; none/TQ alias buf — copy so
	// the pooled buffer can be reused. Detect alias by pointer range.
	if isSubsliceOf(raw, buf) {
		out := make([]byte, len(raw))
		copy(out, raw)
		raw = out
	}
	*bp = buf
	readBufPool.Put(bp)
	if dvs.blockCache != nil {
		dvs.blockCache.Put(cacheKey, raw)
	}
	return raw, nil
}

// isSubsliceOf reports whether a shares backing storage with whole.
//
// #nosec G103 -- pointer arithmetic is confined to this range check; no dereference.
func isSubsliceOf(a, whole []byte) bool {
	if len(a) == 0 || cap(a) == 0 {
		return false
	}
	a0 := uintptr(unsafe.Pointer(unsafe.SliceData(a)))     // #nosec G103
	w0 := uintptr(unsafe.Pointer(unsafe.SliceData(whole))) // #nosec G103
	w1 := w0 + uintptr(cap(whole))
	return a0 >= w0 && a0 < w1
}

// OffloadBlock moves a local block to remote storage.
func (dvs *DiskVectorStore) OffloadBlock(ctx context.Context, bIdx int) error {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()

	if bIdx < 0 || bIdx >= len(dvs.blocks) {
		return fmt.Errorf("invalid block index")
	}

	block := &dvs.blocks[bIdx]
	if block.Tier != storage.TierHot {
		return nil // Already offloaded
	}

	if dvs.remote == nil {
		return fmt.Errorf("remote storage not configured")
	}

	// 1. Read local block
	buf := make([]byte, 13+block.CompSize)
	if _, err := dvs.backend.ReadAt(buf, block.Offset); err != nil {
		return err
	}

	// 2. Upload to Remote
	key := fmt.Sprintf("blocks/%s/%d", dvs.path, bIdx)
	if err := dvs.remote.Put(ctx, key, bytes.NewReader(buf)); err != nil {
		return err
	}

	// 3. Update Block Info
	block.Tier = storage.TierWarm
	block.RemoteKey = key

	// but for this POC we just mark it as Warm.
	return nil
}

// EnforcePolicy applies retention/offloading policies based on block age.
func (dvs *DiskVectorStore) EnforcePolicy(ctx context.Context, maxAge time.Duration) (int, error) {
	dvs.mu.RLock()
	var hotBlockIdxs []int
	for i, b := range dvs.blocks {
		if b.Tier == storage.TierHot && time.Since(b.CreatedAt) > maxAge {
			hotBlockIdxs = append(hotBlockIdxs, i)
		}
	}
	dvs.mu.RUnlock()

	offloaded := 0
	for _, idx := range hotBlockIdxs {
		if err := dvs.OffloadBlock(ctx, idx); err != nil {
			return offloaded, err
		}
		offloaded++
	}
	return offloaded, nil
}

func (dvs *DiskVectorStore) decompressBlock(buf []byte) ([]byte, error) {
	if len(buf) < 13 {
		return nil, io.ErrUnexpectedEOF
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != 0x56434D50 {
		return nil, fmt.Errorf("invalid magic")
	}

	compType := buf[4]
	rawSize := binary.LittleEndian.Uint32(buf[5:9])
	compSize := binary.LittleEndian.Uint32(buf[9:13])
	data := buf[13 : 13+compSize]

	var raw []byte
	var err error
	switch compType {
	case 1: // Zstd
		raw, err = dvs.zstdDec.DecodeAll(data, nil)
	case 2: // LZ4
		raw = make([]byte, rawSize)
		_, err = lz4.UncompressBlock(data, raw)
	case 3: // TurboQuant
		raw = data
	default: // None
		raw = data
	}
	return raw, err
}

// Delete marks a vector as deleted in the store's tombstone map.
func (dvs *DiskVectorStore) Delete(idx int) bool {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()

	if idx < 0 || idx >= dvs.totalCount {
		return false
	}

	if dvs.deleted[idx] {
		return false
	}

	dvs.deleted[idx] = true
	return true
}

// DeleteBatch marks multiple vectors as deleted.
func (dvs *DiskVectorStore) DeleteBatch(indices []int) int {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()

	deleted := 0
	for _, idx := range indices {
		if idx >= 0 && idx < dvs.totalCount && !dvs.deleted[idx] {
			dvs.deleted[idx] = true
			deleted++
		}
	}
	return deleted
}

// IsDeleted checks if a vector has been marked as deleted.
func (dvs *DiskVectorStore) IsDeleted(idx int) bool {
	dvs.mu.RLock()
	defer dvs.mu.RUnlock()
	return dvs.deleted[idx]
}

// Compact removes deleted markers (currently a simplified implementation).
func (dvs *DiskVectorStore) Compact() (int, error) {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()

	if len(dvs.deleted) == 0 {
		return 0, nil
	}

	compacted := 0
	dvs.deleted = make(map[int]bool)
	return compacted, nil
}
