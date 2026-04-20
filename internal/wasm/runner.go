package wasm

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

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
	free := r.mod.ExportedFunction("free")
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
	if free != nil {
		defer free.Call(ctx, uint64(inputPtr))
	}

	// 2. Copy input to WASM memory
	inputBytes := make([]byte, inputSize)
	for i, f := range input {
		bits := math.Float32bits(f)
		inputBytes[i*4] = byte(bits)
		inputBytes[i*4+1] = byte(bits >> 8)
		inputBytes[i*4+2] = byte(bits >> 16)
		inputBytes[i*4+3] = byte(bits >> 24)
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
	if len(results) < 2 {
		return nil, fmt.Errorf("WASM inference returned insufficient result data")
	}
	outputPtr := uint32(results[0])
	outputLen := uint32(results[1])
	outputBytes, ok := mem.Read(outputPtr, outputLen*4)
	if !ok {
		return nil, fmt.Errorf("failed to read WASM memory")
	}

	output := make([]float32, outputLen)
	for i := range output {
		bits := uint32(outputBytes[i*4]) | uint32(outputBytes[i*4+1])<<8 | uint32(outputBytes[i*4+2])<<16 | uint32(outputBytes[i*4+3])<<24
		output[i] = math.Float32frombits(bits)
	}

	return output, nil
}

// InferenceWithTokens executes the model's inference function using tokenized int64 inputs
func (r *Runner) InferenceWithTokens(ctx context.Context, inputIds []int64, mask []int64) ([]float32, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	infFunc := r.mod.ExportedFunction("inference_with_tokens")
	if infFunc == nil {
		// Fallback to generic inference if specific token func doesn't exist
		infFunc = r.mod.ExportedFunction("inference")
	}
	malloc := r.mod.ExportedFunction("malloc")
	free := r.mod.ExportedFunction("free")
	mem := r.mod.Memory()

	if infFunc == nil || malloc == nil || mem == nil {
		return nil, fmt.Errorf("WASM module missing required exports")
	}

	// Allocate and copy data
	idSize := uint64(len(inputIds) * 8)
	maskSize := uint64(len(mask) * 8)
	
	idResults, _ := malloc.Call(ctx, idSize)
	idPtr := uint32(idResults[0])
	maskResults, _ := malloc.Call(ctx, maskSize)
	maskPtr := uint32(maskResults[0])
	
	if free != nil {
		defer free.Call(ctx, uint64(idPtr))
		defer free.Call(ctx, uint64(maskPtr))
	}

	// Helper to write int64 slice
	idBytes := make([]byte, idSize)
	for i, v := range inputIds {
		for b := 0; b < 8; b++ {
			idBytes[i*8+b] = byte(v >> (b * 8))
		}
	}
	mem.Write(idPtr, idBytes)

	maskBytes := make([]byte, maskSize)
	for i, v := range mask {
		for b := 0; b < 8; b++ {
			maskBytes[i*8+b] = byte(v >> (b * 8))
		}
	}
	mem.Write(maskPtr, maskBytes)

	// Call inference
	results, err := infFunc.Call(ctx, uint64(idPtr), uint64(maskPtr), uint64(len(inputIds)))
	if err != nil {
		return nil, err
	}

	outputPtr := uint32(results[0])
	outputLen := uint32(results[1])
	outputBytes, _ := mem.Read(outputPtr, outputLen*4)

	output := make([]float32, outputLen)
	for i := range output {
		bits := uint32(outputBytes[i*4]) | uint32(outputBytes[i*4+1])<<8 | uint32(outputBytes[i*4+2])<<16 | uint32(outputBytes[i*4+3])<<24
		output[i] = math.Float32frombits(bits)
	}

	return output, nil
}

func (r *Runner) Close(ctx context.Context) error {
	return r.runtime.Close(ctx)
}
