package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/pierrec/lz4/v4"
)

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
	cacheKey := block.HotKey
	if cacheKey == "" {
		cacheKey = hotCacheKey(bIdx)
	}
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

// OffloadBlock uploads a local block to remote storage and transitions it to TierWarm.
func (dvs *DiskVectorStore) OffloadBlock(ctx context.Context, bIdx int) error {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()

	if dvs.remote == nil {
		return fmt.Errorf("remote storage not configured")
	}

	if bIdx < 0 || bIdx >= len(dvs.blocks) {
		return fmt.Errorf("invalid block index %d", bIdx)
	}

	block := dvs.blocks[bIdx]
	if block.Tier != storage.TierHot {
		return nil // already offloaded
	}

	// 1. Read raw compressed data from disk
	need := 13 + int(block.CompSize)
	buf := make([]byte, need)
	if _, err := dvs.backend.ReadAt(buf, block.Offset); err != nil {
		return fmt.Errorf("read block data: %w", err)
	}

	// 2. Upload to remote storage
	remoteKey := fmt.Sprintf("blocks/%s_%d.bin", dvs.path, bIdx)
	if err := dvs.remote.Put(ctx, remoteKey, bytes.NewReader(buf)); err != nil {
		return fmt.Errorf("upload to remote: %w", err)
	}

	// 3. Update block entry
	block.Tier = storage.TierWarm
	block.RemoteKey = remoteKey
	dvs.blocks[bIdx] = block

	return nil
}

// EnforcePolicy checks for blocks that exceed the maxAge and offloads them to warm tier.
func (dvs *DiskVectorStore) EnforcePolicy(ctx context.Context, maxAge time.Duration) (int, error) {
	dvs.mu.RLock()
	var toOffload []int
	now := time.Now()
	for i, block := range dvs.blocks {
		if block.Tier == storage.TierHot && now.Sub(block.CreatedAt) > maxAge {
			toOffload = append(toOffload, i)
		}
	}
	dvs.mu.RUnlock()

	offloaded := 0
	for _, bIdx := range toOffload {
		if err := dvs.OffloadBlock(ctx, bIdx); err != nil {
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
