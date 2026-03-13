//go:build gpu && darwin && arm64
// +build gpu,darwin,arm64

package metal

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Metal -framework MetalPerformanceShaders -framework Foundation -framework Accelerate

#include <stdlib.h>
#include <stdbool.h>

// Forward declarations
typedef struct MetalEngine MetalEngine;

MetalEngine* metal_engine_create();
void metal_engine_destroy(MetalEngine* engine);
bool metal_engine_available();
bool metal_engine_load_model(MetalEngine* engine, const char* path);
float* metal_engine_score(MetalEngine* engine, const char* query, const char** docs, int doc_count, int* out_count);
void metal_engine_free_scores(float* scores);

*/
import "C"
import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
)

var (
	available   bool
	availableMu sync.RWMutex
)

func init() {
	availableMu.Lock()
	available = bool(C.metal_engine_available())
	availableMu.Unlock()
}

// IsAvailable checks if Metal is available
func IsAvailable() bool {
	availableMu.RLock()
	defer availableMu.RUnlock()
	return available
}

// MetalEngine provides ONNX inference via Metal
type MetalEngine struct {
	engine     *C.MetalEngine
	deviceName string
	loaded     bool
	mu         sync.RWMutex
}

// ModelInfo contains information about a loaded model
type ModelInfo struct {
	Name       string
	InputLen   int
	OutputLen  int
	Parameters int64
}

// NewMetalEngine creates a new Metal ONNX engine
func NewMetalEngine() (*MetalEngine, error) {
	if !IsAvailable() {
		return nil, errors.New("Metal is not available")
	}

	engine := &MetalEngine{
		engine: C.metal_engine_create(),
	}

	if engine.engine == nil {
		return nil, errors.New("failed to create Metal engine")
	}

	runtime.SetFinalizer(engine, func(e *MetalEngine) {
		e.Close()
	})

	return engine, nil
}

// LoadModel loads an ONNX model from file
func (e *MetalEngine) LoadModel(path string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.engine == nil {
		return errors.New("engine is closed")
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	ok := bool(C.metal_engine_load_model(e.engine, cPath))
	if !ok {
		return fmt.Errorf("failed to load model from %s", path)
	}

	e.loaded = true
	return nil
}

// Score computes relevance scores for query-document pairs
func (e *MetalEngine) Score(ctx context.Context, query string, documents []string) ([]float32, error) {
	if !e.loaded {
		return nil, errors.New("model not loaded")
	}

	if len(documents) == 0 {
		return []float32{}, nil
	}

	// Convert to C strings
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	cDocs := make([]*C.char, len(documents))
	for i, doc := range documents {
		cDocs[i] = C.CString(doc)
		defer C.free(unsafe.Pointer(cDocs[i]))
	}

	var outCount C.int
	scoresPtr := C.metal_engine_score(e.engine, cQuery, &cDocs[0], C.int(len(documents)), &outCount)
	if scoresPtr == nil {
		metrics.OnnxMetalInferenceErrors.Inc()
		return nil, errors.New("inference failed")
	}
	defer C.metal_engine_free_scores(scoresPtr)

	// Convert to Go slice
	_scores := (*[1 << 30]C.float)(unsafe.Pointer(scoresPtr))[:outCount:outCount]
	scores := make([]float32, outCount)
	for i := 0; i < int(outCount); i++ {
		scores[i] = float32(_scores[i])
	}

	// Record metrics
	metrics.OnnxMetalInferenceDuration.WithLabelValues("single").Observe(float64(len(documents)) * 0.001)
	metrics.OnnxMetalBatchSize.Observe(float64(len(documents)))

	return scores, nil
}

// ScoreBatch scores multiple queries against documents
func (e *MetalEngine) ScoreBatch(ctx context.Context, queries, documents []string) ([][]float32, error) {
	if len(queries) == 0 || len(documents) == 0 {
		return [][]float32{}, nil
	}

	results := make([][]float32, len(queries))

	// Simple sequential processing for now
	for i, query := range queries {
		scores, err := e.Score(ctx, query, documents)
		if err != nil {
			return nil, err
		}
		results[i] = scores
	}

	return results, nil
}

// Warmup performs warmup inference
func (e *MetalEngine) Warmup() error {
	dummyDocs := []string{"warmup document"}
	_, err := e.Score(context.Background(), "warmup query", dummyDocs)
	return err
}

// ModelInfo returns information about the loaded model
func (e *MetalEngine) ModelInfo() (*ModelInfo, error) {
	if !e.loaded {
		return nil, errors.New("model not loaded")
	}

	return &ModelInfo{
		Name:       "cross-encoder",
		InputLen:   512,
		OutputLen:  1,
		Parameters: 0,
	}, nil
}

// Close releases Metal resources
func (e *MetalEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.engine != nil {
		C.metal_engine_destroy(e.engine)
		e.engine = nil
	}

	return nil
}
