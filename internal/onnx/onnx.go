package onnx

import (
	"context"
	"fmt"
	"os"
	"runtime"
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
			} else {
				libPath = "/usr/local/lib/libonnxruntime.so"
			}
		}

		if _, err := os.Stat(libPath); err == nil {
			ort.SetSharedLibraryPath(libPath)
			if err := ort.InitializeEnvironment(); err == nil {
				ortReady = true
			} else {
				initErr = fmt.Errorf("failed to initialize ONNX Runtime: %w", err)
			}
		} else {
			initErr = fmt.Errorf("ONNX Runtime library not found at %s. Please set ONNX_RUNTIME_LIB_PATH", libPath)
		}
	})
	return initErr
}

// Session wraps an ONNX runtime session
type Session struct {
	ortSession *ort.AdvancedSession
	metalEngine *metal.Engine
	isMetal bool
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

	// Note: Basic onnxruntime_go doesn't provide a simple way to get input/output names from model file
	// without a session. We'll use a simplified wrapper or assume names for now.
	// In a real implementation, we'd use NewDynamicSession if available or inspect the model.
	
	// For now, return error if we can't easily initialize a generic session
	return nil, fmt.Errorf("generic ONNX session creation not yet fully implemented for %s", modelPath)
}

func (s *Session) Score(ctx context.Context, query string, docs []string) ([]float32, error) {
	if s.isMetal {
		return s.metalEngine.Score(ctx, query, docs)
	}
	return nil, fmt.Errorf("Score not implemented for ONNX Runtime backend")
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
