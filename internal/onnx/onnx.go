//go:build onnx
// +build onnx

package onnx

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/23skdu/longbow/internal/onnx/metal"
	ort "github.com/yalue/onnxruntime_go"
)

var (
	once     sync.Once
	initErr  error
	ortReady bool
)

// Init initializes the ONNX Runtime environment
func Init() error {
	once.Do(func() {
		// Try to find ONNX runtime library
		libPath := os.Getenv("ONNX_RUNTIME_LIB_PATH")
		if libPath == "" {
			if runtime.GOOS == "darwin" {
				libPath = "/usr/local/lib/libonnxruntime.dylib"
				if _, err := os.Stat(libPath); err != nil {
					libPath = "/opt/homebrew/lib/libonnxruntime.dylib"
				}
			} else if runtime.GOOS == "linux" {
				libPath = "/usr/local/lib/libonnxruntime.so"
				if _, err := os.Stat(libPath); err != nil {
					libPath = "/usr/lib/x86_64-linux-gnu/libonnxruntime.so"
				}
			} else {
				libPath = "libonnxruntime.so"
			}
		}

		ort.SetSharedLibraryPath(libPath)
		if err := ort.InitializeEnvironment(); err != nil {
			initErr = fmt.Errorf("failed to initialize ONNX Runtime: %w", err)
		} else {
			ortReady = true
		}
	})
	return initErr
}

// Session wraps an ONNX runtime session
type Session struct {
	ortSession  *ort.DynamicAdvancedSession
	metalEngine *metal.Engine
	isMetal     bool
	inputNames  []string
	outputNames []string
}

// NewSession creates a new ONNX session
func NewSession(modelPath string) (*Session, error) {
	// Try Metal first if on Mac ARM64
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" && metal.IsAvailable() {
		engine, err := metal.NewEngine()
		if err == nil {
			if err := engine.LoadModel(modelPath); err == nil {
				return &Session{
					metalEngine: engine,
					isMetal:     true,
				}, nil
			}
			engine.Close()
		}
	}

	// Fallback to ONNX Runtime
	if err := Init(); err != nil {
		return nil, err
	}

	// Create session options with CUDA if available
	options, err := ort.NewSessionOptions()
	if err != nil {
		return nil, err
	}
	defer options.Destroy()

	if runtime.GOOS == "linux" {
		// Try to enable CUDA
		_ = options.AppendExecutionProviderCUDA(nil)
	}

	// Load model
	inputNames := []string{"input_ids", "attention_mask", "token_type_ids"}
	outputNames := []string{"logits", "output"} // Adjust based on expected model outputs
	session, err := ort.NewDynamicAdvancedSession(modelPath, inputNames, outputNames, options)
	if err != nil {
		return nil, fmt.Errorf("failed to create ONNX session: %w", err)
	}

	s := &Session{
		ortSession: session,
		isMetal:    false,
	}

	s.inputNames = inputNames
	s.outputNames = outputNames

	return s, nil
}

// Score computes scores for query-document pairs.
// Note: This implementation assumes the model takes tokenized inputs if it's a standard cross-encoder,
// but for this remediation, we provide a placeholder tokenizer or assume the model has a text-input op.
func (s *Session) Score(ctx context.Context, query string, docs []string) ([]float32, error) {
	if s.isMetal {
		return s.metalEngine.Score(ctx, query, docs)
	}

	if s.ortSession == nil {
		return nil, fmt.Errorf("session not initialized")
	}

	numDocs := len(docs)
	if numDocs == 0 {
		return []float32{}, nil
	}

	// Placeholder: Simplified tokenization/tensor creation
	// Real implementation would use a proper tokenizer.
	// For now, we'll create dummy tensors to satisfy the library interface
	// if we don't have a tokenizer integrated yet.
	// But to make it "work" in a way that doesn't crash:
	
	// Assume max seq len 512
	maxLen := 512
	inputIds := make([]int64, numDocs*maxLen)
	mask := make([]int64, numDocs*maxLen)
	
	// Very basic whitespace "tokenizer" for demo purposes
	for i, doc := range docs {
		combined := query + " " + doc
		words := strings.Fields(combined)
		for j := 0; j < len(words) && j < maxLen; j++ {
			inputIds[i*maxLen+j] = int64(len(words[j])) // Mock ID
			mask[i*maxLen+j] = 1
		}
	}

	// Create tensors
	shape := []int64{int64(numDocs), int64(maxLen)}
	inputTensor, _ := ort.NewTensor(shape, inputIds)
	maskTensor, _ := ort.NewTensor(shape, mask)
	defer inputTensor.Destroy()
	defer maskTensor.Destroy()

	// Run inference
	inputValues := map[string]ort.Value{
		"input_ids":      inputTensor,
		"attention_mask": maskTensor,
	}
	
	// Add token_type_ids if required by model
	hasTokenTypeIds := false
	for _, name := range s.inputNames {
		if name == "token_type_ids" {
			hasTokenTypeIds = true
			break
		}
	}
	if hasTokenTypeIds {
		tokenTypeIds := make([]int64, numDocs*maxLen)
		ttTensor, _ := ort.NewTensor(shape, tokenTypeIds)
		defer ttTensor.Destroy()
		inputValues["token_type_ids"] = ttTensor
	}

	inputs := make([]ort.Value, 0, len(inputValues))
	// Re-order inputs to match s.inputNames exactly as expected by libraries that use positional slices
	for _, name := range s.inputNames {
		if v, ok := inputValues[name]; ok {
			inputs = append(inputs, v)
		}
	}

	outputs := make([]ort.Value, len(s.outputNames))
	err := s.ortSession.Run(inputs, outputs)
	if err != nil {
		return nil, fmt.Errorf("inference failed: %w", err)
	}
	defer func() {
		for _, v := range outputs {
			if v != nil {
				v.Destroy()
			}
		}
	}()

	// Extract scores from first output
	if len(outputs) == 0 {
		return nil, fmt.Errorf("no output from model")
	}
	
	scoresTensor := outputs[0].(*ort.Tensor[float32])
	return scoresTensor.GetData(), nil
}

func (s *Session) Close() error {
	if s.isMetal {
		return s.metalEngine.Close()
	}
	if s.ortSession != nil {
		return s.ortSession.Destroy()
	}
	return nil
}
