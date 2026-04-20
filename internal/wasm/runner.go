package wasm

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// Runner handle WASM model execution
type Runner struct {
	runtime wazero.Runtime
	mod     api.Module
	mu      sync.Mutex
	name    string
}

// NewRunner creates a new WASM runner for the given model path
func NewRunner(ctx context.Context, modelPath string) (*Runner, error) {
	data, err := os.ReadFile(modelPath)
	if err != nil {
		return nil, err
	}

	r := wazero.NewRuntime(ctx)
	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	mod, err := r.InstantiateWithConfig(ctx, data, wazero.NewModuleConfig().WithStdout(os.Stdout).WithStderr(os.Stderr))
	if err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("failed to instantiate WASM module: %w", err)
	}

	return &Runner{
		runtime: r,
		mod:     mod,
		name:    modelPath,
	}, nil
}

// Inference executes the model's inference function using shared memory
func (r *Runner) Inference(ctx context.Context, input []float32) ([]float32, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	start := time.Now()
	defer func() {
		status := "success"
		if ctx.Err() != nil {
			status = "error"
		}
		metrics.WasmInferenceTotal.WithLabelValues(r.name, status).Inc()
		metrics.WasmInferenceDurationSeconds.WithLabelValues(r.name).Observe(time.Since(start).Seconds())
	}()

	infFunc := r.mod.ExportedFunction("inference")
	malloc := r.mod.ExportedFunction("malloc")
	mem := r.mod.Memory()

	if infFunc == nil || malloc == nil || mem == nil {
		return nil, fmt.Errorf("WASM module missing required exports (inference, malloc, or memory)")
	}

	// 1. Allocate space for input
	inputSize := uint64(len(input) * 4)
	results, err := malloc.Call(ctx, inputSize)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate WASM memory: %w", err)
	}
	inputPtr := uint32(results[0])

	// 2. Copy input to WASM memory
	inputBytes := make([]byte, inputSize)
	for i, f := range input {
		*(*float32)(unsafe.Pointer(&inputBytes[i*4])) = f
	}
	if !mem.Write(inputPtr, inputBytes) {
		return nil, fmt.Errorf("failed to write to WASM memory")
	}

	// 3. Run inference
	results, err = infFunc.Call(ctx, uint64(inputPtr), uint64(len(input)))
	if err != nil {
		return nil, fmt.Errorf("WASM inference failed: %w", err)
	}

	// 4. Extract output (assuming result is a pointer to float32 array)
	outputPtr := uint32(results[0])
	outputLen := uint32(results[1])
	outputBytes, ok := mem.Read(outputPtr, outputLen*4)
	if !ok {
		return nil, fmt.Errorf("failed to read WASM memory")
	}

	output := make([]float32, outputLen)
	for i := range output {
		output[i] = *(*float32)(unsafe.Pointer(&outputBytes[i*4]))
	}

	return output, nil
}

func (r *Runner) Close(ctx context.Context) error {
	return r.runtime.Close(ctx)
}
