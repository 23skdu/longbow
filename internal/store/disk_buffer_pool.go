package store

import (
	"sync"

	"github.com/23skdu/longbow/internal/store/index"
)

// globalRowPools caches index.Float32RowPool instances by dimension.
var (
	rowPoolMu sync.RWMutex
	f32Pools  = make(map[int]*index.Float32RowPool)
	bytePools = make(map[int]*index.ByteRowPool)
)

// getFloat32Pool returns or creates a pooled allocator for dimension dim.
func getFloat32Pool(dim int) *index.Float32RowPool {
	rowPoolMu.RLock()
	p, ok := f32Pools[dim]
	rowPoolMu.RUnlock()
	if ok {
		return p
	}

	rowPoolMu.Lock()
	defer rowPoolMu.Unlock()
	if p, ok = f32Pools[dim]; ok {
		return p
	}
	p = index.NewFloat32RowPool(dim)
	f32Pools[dim] = p
	return p
}

// getBytePool returns or creates a pooled allocator for byte size.
func getBytePool(size int) *index.ByteRowPool {
	rowPoolMu.RLock()
	p, ok := bytePools[size]
	rowPoolMu.RUnlock()
	if ok {
		return p
	}

	rowPoolMu.Lock()
	defer rowPoolMu.Unlock()
	if p, ok = bytePools[size]; ok {
		return p
	}
	p = index.NewByteRowPool(size)
	bytePools[size] = p
	return p
}

// GetFloat32RowBuffer acquires a float32 slice of length dim from the pool.
func GetFloat32RowBuffer(dim int) []float32 {
	return getFloat32Pool(dim).Get()
}

// PutFloat32RowBuffer returns a float32 slice to the pool.
func PutFloat32RowBuffer(s []float32) {
	if len(s) == 0 {
		return
	}
	getFloat32Pool(len(s)).Put(s)
}

// GetByteRowBuffer acquires a byte slice of length size from the pool.
func GetByteRowBuffer(size int) []byte {
	return getBytePool(size).Get()
}

// PutByteRowBuffer returns a byte slice to the pool.
func PutByteRowBuffer(b []byte) {
	if len(b) == 0 {
		return
	}
	getBytePool(len(b)).Put(b)
}
