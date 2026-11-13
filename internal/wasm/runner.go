package wasm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

const (
	// MaxInputFloats bounds the size of a single inference input to avoid OOMs.
	MaxInputFloats = 1 << 20 // ~4 MiB of float32
	// MaxOutputFloats bounds the size of a single inference output.
	MaxOutputFloats = 1 << 20 // ~4 MiB of float32
)

var (
	ErrInvalidModule   = errors.New("wasm: invalid module")
	ErrMissingExport   = errors.New("wasm: missing required export")
	ErrMemoryBounds    = errors.New("wasm: memory bounds exceeded")
	ErrInferenceFailed = errors.New("wasm: inference failed")
	ErrModuleClosed    = errors.New("wasm: module closed")
	ErrInputTooLarge   = errors.New("wasm: input too large")
	ErrOutputTooLarge  = errors.New("wasm: output too large")
)

// Runner handles WASM model execution
type Runner struct {
	runtime wazero.Runtime
	mod     api.Module
	mu      sync.Mutex
	closed  bool
	name    string
}

// NewRunner creates a new WASM runner for the given model path
func NewRunner(ctx context.Context, modelPath string) (*Runner, error) { // #nosec G304
	if ctx == nil {
		ctx = context.Background()
	}
	data, err := os.ReadFile(filepath.Clean(modelPath)) // #nosec G304
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty module", ErrInvalidModule)
	}

	r := wazero.NewRuntime(ctx)
	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	mod, err := r.InstantiateWithConfig(ctx, data, wazero.NewModuleConfig().
		WithStdout(nil).
		WithStderr(nil))
	if err != nil {
		_ = r.Close(ctx)
		return nil, fmt.Errorf("failed to instantiate WASM module: %w", err)
	}

	runner := &Runner{
		runtime: r,
		mod:     mod,
		name:    modelPath,
	}

	if err := runner.validateExports(); err != nil {
		_ = runner.Close(ctx)
		return nil, err
	}

	return runner, nil
}

func (r *Runner) validateExports() error {
	if r.mod == nil || r.mod.IsClosed() {
		return ErrInvalidModule
	}
	if r.mod.ExportedFunction("inference") == nil {
		return fmt.Errorf("%w: inference", ErrMissingExport)
	}
	if r.mod.ExportedFunction("malloc") == nil {
		return fmt.Errorf("%w: malloc", ErrMissingExport)
	}
	if r.mod.Memory() == nil {
		return fmt.Errorf("%w: memory", ErrMissingExport)
	}
	return nil
}

// Inference executes the model's inference function using shared memory
func (r *Runner) Inference(ctx context.Context, input []float32) ([]float32, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(input) > MaxInputFloats {
		return nil, ErrInputTooLarge
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, ErrModuleClosed
	}

	start := time.Now()
	status := "success"
	defer func() {
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
		status = "error"
		return nil, fmt.Errorf("%w: missing required exports", ErrMissingExport)
	}

	inputSize := uint64(len(input)) * 4
	if len(input) == 0 {
		inputSize = 4
	}

	results, err := malloc.Call(ctx, inputSize)
	if err != nil || len(results) == 0 {
		status = "error"
		return nil, fmt.Errorf("%w: malloc failed: %v", ErrInferenceFailed, err)
	}
	inputPtr := uint32(results[0]) // #nosec G115
	if free != nil {
		defer func() { _, _ = free.Call(ctx, uint64(inputPtr)) }()
	}

	inputBytes := make([]byte, inputSize)
	for i, f := range input {
		bits := math.Float32bits(f)
		inputBytes[i*4] = byte(bits)
		inputBytes[i*4+1] = byte(bits >> 8)
		inputBytes[i*4+2] = byte(bits >> 16)
		inputBytes[i*4+3] = byte(bits >> 24)
	}
	if !mem.Write(inputPtr, inputBytes) {
		status = "error"
		return nil, fmt.Errorf("%w: failed to write input", ErrMemoryBounds)
	}

	callResults, err := infFunc.Call(ctx, uint64(inputPtr), uint64(len(input)))
	if err != nil {
		status = "error"
		return nil, fmt.Errorf("%w: %v", ErrInferenceFailed, err)
	}
	if len(callResults) < 2 {
		status = "error"
		return nil, fmt.Errorf("%w: insufficient result data", ErrInferenceFailed)
	}

	outputPtr := uint32(callResults[0]) // #nosec G115
	outputLen := uint32(callResults[1]) // #nosec G115
	if outputLen > MaxOutputFloats {
		status = "error"
		return nil, ErrOutputTooLarge
	}
	if uint64(outputPtr)+uint64(outputLen)*4 > uint64(mem.Size()) {
		status = "error"
		return nil, ErrMemoryBounds
	}

	outputBytes, ok := mem.Read(outputPtr, outputLen*4)
	if !ok {
		status = "error"
		return nil, fmt.Errorf("%w: failed to read output", ErrMemoryBounds)
	}

	if free != nil && outputPtr != inputPtr {
		defer func() { _, _ = free.Call(ctx, uint64(outputPtr)) }()
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
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(inputIds) > MaxInputFloats {
		return nil, ErrInputTooLarge
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, ErrModuleClosed
	}

	infFunc := r.mod.ExportedFunction("inference_with_tokens")
	if infFunc == nil {
		infFunc = r.mod.ExportedFunction("inference")
	}
	malloc := r.mod.ExportedFunction("malloc")
	free := r.mod.ExportedFunction("free")
	mem := r.mod.Memory()

	if infFunc == nil || malloc == nil || mem == nil {
		return nil, fmt.Errorf("%w: missing required exports", ErrMissingExport)
	}

	n := len(inputIds)
	if n == 0 {
		n = 1
	}
	idSize := uint64(n * 8)
	maskSize := uint64(len(mask) * 8)
	if len(mask) == 0 {
		maskSize = 8
	}

	idResults, err := malloc.Call(ctx, idSize)
	if err != nil || len(idResults) == 0 {
		return nil, fmt.Errorf("%w: malloc ids failed: %v", ErrInferenceFailed, err)
	}
	idPtr := uint32(idResults[0]) // #nosec G115
	if free != nil {
		defer func() { _, _ = free.Call(ctx, uint64(idPtr)) }()
	}

	maskResults, err := malloc.Call(ctx, maskSize)
	if err != nil || len(maskResults) == 0 {
		return nil, fmt.Errorf("%w: malloc mask failed: %v", ErrInferenceFailed, err)
	}
	maskPtr := uint32(maskResults[0]) // #nosec G115
	if free != nil {
		defer func() { _, _ = free.Call(ctx, uint64(maskPtr)) }()
	}

	idBytes := make([]byte, idSize)
	for i := 0; i < len(inputIds); i++ {
		v := inputIds[i]
		for b := 0; b < 8; b++ {
			idBytes[i*8+b] = byte(v >> (b * 8))
		}
	}
	if !mem.Write(idPtr, idBytes) {
		return nil, fmt.Errorf("%w: failed to write ids", ErrMemoryBounds)
	}

	maskBytes := make([]byte, maskSize)
	for i := 0; i < len(mask); i++ {
		v := mask[i]
		for b := 0; b < 8; b++ {
			maskBytes[i*8+b] = byte(v >> (b * 8))
		}
	}
	if !mem.Write(maskPtr, maskBytes) {
		return nil, fmt.Errorf("%w: failed to write mask", ErrMemoryBounds)
	}

	results, err := infFunc.Call(ctx, uint64(idPtr), uint64(maskPtr), uint64(len(inputIds)))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInferenceFailed, err)
	}
	if len(results) < 2 {
		return nil, fmt.Errorf("%w: insufficient result data", ErrInferenceFailed)
	}

	outputPtr := uint32(results[0]) // #nosec G115
	outputLen := uint32(results[1]) // #nosec G115
	if outputLen > MaxOutputFloats {
		return nil, ErrOutputTooLarge
	}
	if uint64(outputPtr)+uint64(outputLen)*4 > uint64(mem.Size()) {
		return nil, ErrMemoryBounds
	}

	outputBytes, ok := mem.Read(outputPtr, outputLen*4)
	if !ok {
		return nil, fmt.Errorf("%w: failed to read output", ErrMemoryBounds)
	}

	if free != nil && outputPtr != idPtr && outputPtr != maskPtr {
		defer func() { _, _ = free.Call(ctx, uint64(outputPtr)) }()
	}

	output := make([]float32, outputLen)
	for i := range output {
		bits := uint32(outputBytes[i*4]) | uint32(outputBytes[i*4+1])<<8 | uint32(outputBytes[i*4+2])<<16 | uint32(outputBytes[i*4+3])<<24
		output[i] = math.Float32frombits(bits)
	}

	return output, nil
}

func (r *Runner) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true

	var firstErr error
	if r.mod != nil {
		if err := r.mod.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		r.mod = nil
	}
	if r.runtime != nil {
		if err := r.runtime.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		r.runtime = nil
	}
	return firstErr
}
