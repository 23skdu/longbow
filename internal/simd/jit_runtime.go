package simd

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

type JitRuntime struct {
	runtime wazero.Runtime
	ctx     context.Context
	mod     api.Module
	mu      sync.Mutex

	euclidean      api.Function
	euclideanBatch api.Function
}

var (
	jitEnabled bool
	jitRT      *JitRuntime
)

func initJIT() error {
	ctx := context.Background()
	start := time.Now()
	r := wazero.NewRuntime(ctx)

	// Select and generate kernel
	// Generate kernels with SIMD enabled
	wasm := GenerateKernels(true)

	mod, err := r.Instantiate(ctx, wasm)
	if err != nil {
		return fmt.Errorf("wazero instantiate failed: %w", err)
	}
	metrics.JitCompilationDurationSeconds.Observe(time.Since(start).Seconds())

	jitRT = &JitRuntime{
		runtime:        r,
		ctx:            ctx,
		mod:            mod,
		euclidean:      mod.ExportedFunction("dist"),
		euclideanBatch: mod.ExportedFunction("dist_batch"),
	}
	jitEnabled = true
	return nil
}

func (rt *JitRuntime) Euclidean(a, b []float32) float32 {
	n := uint32(len(a))
	if n == 0 {
		return 0
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	mem := rt.mod.Memory()

	size := n * 4
	ptrA := uint32(0)
	ptrB := uint32(size)

	if mem.Size() < size*2 {
		_, _ = mem.Grow(1) // grow by 64KB
	}

	mem.Write(ptrA, float32SliceToBytes(a))
	mem.Write(ptrB, float32SliceToBytes(b))

	results, err := rt.euclidean.Call(rt.ctx, uint64(ptrA), uint64(ptrB), uint64(n))
	if err != nil {
		metrics.JitKernelErrorsTotal.Inc()
		return 0
	}
	metrics.JitKernelCallsTotal.WithLabelValues("euclidean").Inc()

	return api.DecodeF32(results[0])
}

// Helper for results
func bytesToFloat32Slice(b []byte) []float32 {
	if len(b) == 0 {
		return nil
	}
	n := len(b) / 4
	res := make([]float32, n)
	for i := 0; i < n; i++ {
		res[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return res
}

func (rt *JitRuntime) EuclideanBatch(query []float32, vectors [][]float32) ([]float32, error) {
	if len(vectors) == 0 {
		return nil, nil
	}
	dim := uint32(len(query))
	nVecs := uint32(len(vectors))

	rt.mu.Lock()
	defer rt.mu.Unlock()

	mem := rt.mod.Memory()

	qSize := dim * 4
	totalVecSize := nVecs * dim * 4
	resultsSize := nVecs * 4

	ptrQ := uint32(0)
	ptrVecs := ptrQ + qSize
	ptrResults := ptrVecs + totalVecSize
	totalSize := ptrResults + resultsSize

	if mem.Size() < totalSize {
		pages := (totalSize + 65535) / 65536
		if uint32(mem.Size()) < totalSize {
			currentPages := mem.Size() / 65536
			if pages > currentPages {
				mem.Grow(pages - currentPages)
			}
		}
	}
	// Safety check again
	if uint32(mem.Size()) < totalSize {
		mem.Grow(1 + (totalSize-uint32(mem.Size()))/65536)
	}

	mem.Write(ptrQ, float32SliceToBytes(query))

	for i, v := range vectors {
		mem.Write(ptrVecs+uint32(i)*dim*4, float32SliceToBytes(v))
	}

	_, err := rt.euclideanBatch.Call(rt.ctx, uint64(ptrQ), uint64(ptrVecs), uint64(nVecs), uint64(dim), uint64(ptrResults))
	if err != nil {
		metrics.JitKernelErrorsTotal.Inc()
		return nil, err
	}
	metrics.JitKernelCallsTotal.WithLabelValues("euclidean_batch").Inc()

	resBytes, _ := mem.Read(ptrResults, resultsSize)
	return bytesToFloat32Slice(resBytes), nil
}

func (rt *JitRuntime) EuclideanBatchInto(query []float32, vectors [][]float32, results []float32) {
	if len(vectors) == 0 {
		return
	}
	if len(results) < len(vectors) {
		panic("simd: results slice too small")
	}

	dim := uint32(len(query))
	nVecs := uint32(len(vectors))

	rt.mu.Lock()
	defer rt.mu.Unlock()

	mem := rt.mod.Memory()

	qSize := dim * 4
	totalVecSize := nVecs * dim * 4
	resultsSize := nVecs * 4

	ptrQ := uint32(0)
	ptrVecs := ptrQ + qSize
	ptrResults := ptrVecs + totalVecSize
	totalSize := ptrResults + resultsSize

	if uint32(mem.Size()) < totalSize {
		growPages := (totalSize - uint32(mem.Size()) + 65535) / 65536
		if _, ok := mem.Grow(growPages); !ok {
			// Panic or log? Fallback?
			// For now panic to signal JIT OOM
			panic(fmt.Sprintf("jit: oom growing memory: failed to grow by %d pages", growPages))
		}
	}

	mem.Write(ptrQ, float32SliceToBytes(query))

	for i, v := range vectors {
		mem.Write(ptrVecs+uint32(i)*dim*4, float32SliceToBytes(v))
	}

	_, err := rt.euclideanBatch.Call(rt.ctx, uint64(ptrQ), uint64(ptrVecs), uint64(nVecs), uint64(dim), uint64(ptrResults))
	if err != nil {
		metrics.JitKernelErrorsTotal.Inc()
		return
	}
	metrics.JitKernelCallsTotal.WithLabelValues("euclidean_batch").Inc()

	resBytes, _ := mem.Read(ptrResults, resultsSize)

	// Copy output
	// Ensure safety
	src := bytesToFloat32Slice(resBytes)
	copy(results, src)
}
