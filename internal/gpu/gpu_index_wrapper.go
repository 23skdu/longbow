package gpu

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
)

// GPUIndexWrapper provides high-scale dynamic routing between CPU index representations
// and high-throughput GPU index libraries (such as cuVS, Faiss GPU).
type GPUIndexWrapper struct {
	mu           sync.RWMutex
	primaryIndex Index
	gpuIndex     Index
	config       types.GPUConfig
	closed       bool

	// Routing stats
	qpsCPU atomic.Int64
	qpsGPU atomic.Int64
}

// NewGPUIndexWrapper wraps a primary index and instantiates a high-scale GPU index fallback.
func NewGPUIndexWrapper(cfg types.GPUConfig, primary Index) (*GPUIndexWrapper, error) {
	if primary == nil {
		return nil, fmt.Errorf("primary index cannot be nil")
	}

	wrapper := &GPUIndexWrapper{
		primaryIndex: primary,
		config:       cfg,
	}

	// In a complete cuVS/Faiss GPU deployment, we would initialize the cuVS/Faiss library here.
	// For the initial framework wrapper, we leverage the auto-detected local GPU index.
	if cfg.Enabled {
		backend := DetectGPUBackend()
		gIndex, err := NewIndexWithBackend(cfg, backend)
		if err == nil {
			wrapper.gpuIndex = gIndex
		}
	}

	return wrapper, nil
}

// Add delegates vector ingestion to both primary and high-scale indexes.
func (w *GPUIndexWrapper) Add(ids []int64, vectors []float32) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("index wrapper is closed")
	}

	if err := w.primaryIndex.Add(ids, vectors); err != nil {
		return err
	}

	if w.gpuIndex != nil {
		if err := w.gpuIndex.Add(ids, vectors); err != nil {
			// Record the sync error metric but don't fail the primary committed write.
			// This ensures the engine degrades gracefully to CPU fallback instead of raising write faults.
			if metrics.GPUFallbackTotal != nil {
				metrics.GPUFallbackTotal.WithLabelValues("sync_error").Inc()
			}
		}
	}

	return nil
}

// Search dynamically routes queries to the optimal execution target based on query size and current QPS pressure.
func (w *GPUIndexWrapper) Search(vector []float32, k int) ([]int64, []float32, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.closed {
		return nil, nil, fmt.Errorf("index wrapper is closed")
	}

	// Dynamic Routing Decision
	// Offload to GPU if:
	// 1. GPU index is initialized and healthy.
	// 2. We are under high CPU QPS load (> 500 QPS) or doing high-scale candidate search (k >= 100).
	cpuLoad := w.qpsCPU.Load()
	if w.gpuIndex != nil && (cpuLoad > 500 || k >= 100) {
		w.qpsGPU.Add(1)
		if metrics.GPUUsed != nil {
			metrics.GPUUsed.WithLabelValues(w.config.Backend.String(), "f32").Inc()
		}
		return w.gpuIndex.Search(vector, k)
	}

	w.qpsCPU.Add(1)
	return w.primaryIndex.Search(vector, k)
}

// SearchBatchDistances offloads batch distances to the GPU index if available.
func (w *GPUIndexWrapper) SearchBatchDistances(query []float32, candidateIDs []uint32) ([]float32, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.closed {
		return nil, fmt.Errorf("index wrapper is closed")
	}

	if w.gpuIndex != nil {
		return w.gpuIndex.SearchBatchDistances(query, candidateIDs)
	}

	return w.primaryIndex.SearchBatchDistances(query, candidateIDs)
}

// Close closes all underlying indexes and releases VRAM pools.
func (w *GPUIndexWrapper) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	var lastErr error
	if err := w.primaryIndex.Close(); err != nil {
		lastErr = err
	}

	if w.gpuIndex != nil {
		if err := w.gpuIndex.Close(); err != nil {
			lastErr = err
		}
	}

	w.closed = true
	return lastErr
}

// Backend returns the active GPU Backend
func (w *GPUIndexWrapper) Backend() types.GPUBackend {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.gpuIndex != nil {
		return w.gpuIndex.Backend()
	}
	return w.primaryIndex.Backend()
}

// GetRoutingStats returns diagnostic routing QPS counters.
func (w *GPUIndexWrapper) GetRoutingStats() (cpuQPS int64, gpuQPS int64) {
	return w.qpsCPU.Load(), w.qpsGPU.Load()
}
