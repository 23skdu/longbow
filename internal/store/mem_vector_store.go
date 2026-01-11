package store

import (
	"errors"
	"sync"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
)

type MemStoreOptions struct {
	UseArena bool
	Dim      int
}

// MemVectorStore is an in-memory vector storage engine.
// It supports both legacy [][]float32 and optimized SlabArena storage.
type MemVectorStore struct {
	mu       sync.RWMutex
	useArena bool
	dim      int

	// Legacy storage
	vectors map[string][]float32

	// Arena storage
	baseArena  *memory.SlabArena
	floatArena *memory.TypedArena[float32]
	indices    map[string]memory.SliceRef
}

func NewMemVectorStore(opts MemStoreOptions) (*MemVectorStore, error) {
	ms := &MemVectorStore{
		useArena: opts.UseArena,
		dim:      opts.Dim,
		vectors:  make(map[string][]float32),
		indices:  make(map[string]memory.SliceRef),
	}

	if opts.UseArena {
		// Initialize arena with default large page size (e.g. 1MB)
		ms.baseArena = memory.NewSlabArena(1024 * 1024)
		ms.floatArena = memory.NewTypedArena[float32](ms.baseArena)
	}

	return ms, nil
}

func (ms *MemVectorStore) Set(key string, vec []float32) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.dim > 0 && len(vec) != ms.dim {
		return errors.New("vector dimension mismatch")
	}

	if ms.useArena {
		// Check for overwrite to track metrics accurately
		_, exists := ms.indices[key]

		// AllocSlice
		ref, err := ms.floatArena.AllocSlice(len(vec))
		if err != nil {
			return err
		}

		// Copy data
		dest := ms.floatArena.Get(ref)
		if dest == nil || len(dest) != len(vec) {
			return errors.New("allocated slice invalid")
		}
		copy(dest, vec)

		ms.indices[key] = ref

		if !exists {
			metrics.StoreVectorsManagedCount.Inc()
		}
	} else {
		// Legacy
		vCopy := make([]float32, len(vec))
		copy(vCopy, vec)
		ms.vectors[key] = vCopy
	}
	return nil
}

func (ms *MemVectorStore) Get(key string) ([]float32, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if ms.useArena {
		ref, ok := ms.indices[key]
		if !ok {
			return nil, false
		}
		// Return a copy? Or view?
		// Usually stores return view for zero-copy read.
		// But SliceRef data is volatile if we supported compaction (we don't yet).
		// For safety in this high-level API, let's return copy or view?
		// Test expects `assert.Equal(t, vec, out)`.
		// Let's return view for performance (Zero-Copy Goal).
		return ms.floatArena.Get(ref), true
	}

	vec, ok := ms.vectors[key]
	return vec, ok
}
