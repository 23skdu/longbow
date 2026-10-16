package store

import (
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/klauspost/compress/zstd"
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
	HotKey     string
	CreatedAt  time.Time
}

// DiskVectorStore provides append-only persistent storage for vectors with block-level compression.
type DiskVectorStore struct {
	path        string
	dim         int
	backend     storage.StorageBackend
	mu          sync.RWMutex
	writeMu     sync.Mutex // isolates disk writes and flushes from read traversal (RCU/double-buffering)
	compression string     // "zstd", "lz4", "none"
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

func (dvs *DiskVectorStore) findBlock(idx int) int {
	if idx < 0 || idx >= dvs.totalCount {
		return -1
	}
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
