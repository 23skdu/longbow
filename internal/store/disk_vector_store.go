package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	compression string // "zstd", "lz4", "none"
	zstdEnc     *zstd.Encoder
	zstdDec     *zstd.Decoder
	blocks      []BlockEntry
	totalCount  int

	// Tombstone deletion - tracks deleted indices (consistent with ArrowHNSW)
	deleted map[int]bool

	// Tiered Storage
	remote     storage.RemoteStorage
	blockCache *storage.LRUCache
}

func NewDiskVectorStore(path string, dim int) (*DiskVectorStore, error) {
	return NewDiskVectorStoreWithConfig(path, dim, false, false)
}

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
	}

	return dvs, nil
}

func (dvs *DiskVectorStore) SetTieredConfig(remote storage.RemoteStorage, cacheMB int) {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()
	dvs.remote = remote
	if cacheMB > 0 {
		dvs.blockCache = storage.NewLRUCache(int64(cacheMB) * 1024 * 1024)
	}
}

func (dvs *DiskVectorStore) SetCompression(c string) {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()
	dvs.compression = c
}

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

	// Get the underlying values array (e.g. Float32Array)
	values := listArr.Data().Children()[0]
	if len(values.Buffers()) < 2 || values.Buffers()[1] == nil {
		return 0, fmt.Errorf("invalid arrow data: missing value buffer")
	}

	// Get the specific range for this batch
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	offset := listArr.Data().Offset()
	elemSize := 4 // float32
	startBytes := offset * width * elemSize
	lenBytes := numRows * width * elemSize

	raw := values.Buffers()[1].Bytes()
	if startBytes+lenBytes > len(raw) {
		return 0, fmt.Errorf("arrow buffer out of bounds")
	}

	dataSlice := raw[startBytes : startBytes+lenBytes]

	dvs.mu.Lock()
	defer dvs.mu.Unlock()

	var dataToWrite []byte
	var compType byte // 0: none, 1: zstd, 2: lz4

	// 2. Compress
	switch dvs.compression {
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

	// 3. Write Block
	header := make([]byte, 13)
	binary.LittleEndian.PutUint32(header[0:4], 0x56434D50)
	header[4] = compType
	binary.LittleEndian.PutUint32(header[5:9], uint32(lenBytes))           // #nosec G115
	binary.LittleEndian.PutUint32(header[9:13], uint32(len(dataToWrite))) // #nosec G115

	writeOffset, _ := dvs.backend.Size()
	if _, err := dvs.backend.WriteAt(header, writeOffset); err != nil {
		return 0, err
	}
	if _, err := dvs.backend.WriteAt(dataToWrite, writeOffset+13); err != nil {
		return 0, err
	}

	dvs.blocks = append(dvs.blocks, BlockEntry{
		Offset:     writeOffset,
		CompSize:   uint32(len(dataToWrite)), // #nosec G115
		RawSize:    uint32(lenBytes),         // #nosec G115
		NumVectors: numRows,
		StartIdx:   dvs.totalCount,
		CompType:   compType,
		CreatedAt:  time.Now(),
	})
	dvs.totalCount += numRows

	if err := dvs.backend.Sync(); err != nil {
		return 0, err
	}

	return numRows, nil
}

func (dvs *DiskVectorStore) BatchAppend(vectors [][]float32) (int, error) {
	if len(vectors) == 0 {
		return 0, nil
	}

	dvs.mu.Lock()
	defer dvs.mu.Unlock()

	// 1. Serialize vectors to raw bytes (Little Endian Float32)
	raw := make([]byte, len(vectors)*dvs.dim*4)
	for i, v := range vectors {
		for j, f := range v {
			binary.LittleEndian.PutUint32(raw[(i*dvs.dim+j)*4:], math.Float32bits(f))
		}
	}

	var dataToWrite []byte
	var compType byte // 0: none, 1: zstd, 2: lz4

	// 2. Compress
	switch dvs.compression {
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
	binary.LittleEndian.PutUint32(header[5:9], uint32(len(raw))) // #nosec G115
	binary.LittleEndian.PutUint32(header[9:13], uint32(len(dataToWrite))) // #nosec G115

	offset, _ := dvs.backend.Size()

	if _, err := dvs.backend.WriteAt(header, offset); err != nil {
		return 0, err
	}
	if _, err := dvs.backend.WriteAt(dataToWrite, offset+13); err != nil {
		return 0, err
	}

	dvs.blocks = append(dvs.blocks, BlockEntry{
		Offset:     offset,
		CompSize:   uint32(len(dataToWrite)), // #nosec G115
		RawSize:    uint32(len(raw)), // #nosec G115
		NumVectors: len(vectors),
		StartIdx:   dvs.totalCount,
		CompType:   compType,
		CreatedAt:  time.Now(),
	})
	dvs.totalCount += len(vectors)

	if err := dvs.backend.Sync(); err != nil {
		return 0, err
	}

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

func (dvs *DiskVectorStore) GetBatch(indices []int) ([][]float32, error) {
	if len(indices) == 0 {
		return nil, nil
	}

	dvs.mu.RLock()
	defer dvs.mu.RUnlock()

	filtered := make([]int, 0, len(indices))
	for _, idx := range indices {
		if !dvs.deleted[idx] {
			filtered = append(filtered, idx)
		}
	}

	if len(filtered) == 0 {
		return [][]float32{}, nil
	}

	blockRequestMap := make(map[int][]int)
	for _, idx := range filtered {
		bIdx := dvs.findBlock(idx)
		if bIdx == -1 {
			return nil, fmt.Errorf("vector index %d out of bounds", idx)
		}
		blockRequestMap[bIdx] = append(blockRequestMap[bIdx], idx)
	}

	sortedBlockIdxs := make([]int, 0, len(blockRequestMap))
	for bIdx := range blockRequestMap {
		sortedBlockIdxs = append(sortedBlockIdxs, bIdx)
	}
	sort.Ints(sortedBlockIdxs)

	blockData := make(map[int][]byte)

	for bIdx := range blockRequestMap {
		raw, err := dvs.fetchBlockData(bIdx)
		if err != nil {
			return nil, err
		}
		blockData[bIdx] = raw
	}

	results := make([][]float32, len(filtered))
	for i, idx := range filtered {
		bIdx := dvs.findBlock(idx)
		raw := blockData[bIdx]
		block := dvs.blocks[bIdx]
		localIdx := idx - block.StartIdx

		vec := make([]float32, dvs.dim)
		offset := localIdx * dvs.dim * 4
		for j := 0; j < dvs.dim; j++ {
			bits := binary.LittleEndian.Uint32(raw[offset+j*4 : offset+(j+1)*4])
			vec[j] = math.Float32frombits(bits)
		}
		results[i] = vec
	}

	return results, nil
}

func (dvs *DiskVectorStore) fetchBlockData(bIdx int) ([]byte, error) {
	block := dvs.blocks[bIdx]

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

		// Cache
		if dvs.blockCache != nil {
			dvs.blockCache.Put(block.RemoteKey, raw)
		}
		return raw, nil
	}

	// Local Read
	buf := make([]byte, 13+block.CompSize)
	_, err := dvs.backend.ReadAt(buf, block.Offset)
	if err != nil {
		return nil, err
	}
	return dvs.decompressBlock(buf)
}

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
	default: // None
		raw = data
	}
	return raw, err
}

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

func (dvs *DiskVectorStore) IsDeleted(idx int) bool {
	dvs.mu.RLock()
	defer dvs.mu.RUnlock()
	return dvs.deleted[idx]
}

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
