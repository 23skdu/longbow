package simd

import (
	"testing"
)

func TestMetricType_String(t *testing.T) {
	tests := []struct {
		metric   MetricType
		expected string
	}{
		{MetricEuclidean, "euclidean"},
		{MetricCosine, "cosine"},
		{MetricDotProduct, "dot"},
		{MetricType(99), "unknown"},
		{MetricType(-1), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.metric.String()
			if result != tt.expected {
				t.Errorf("MetricType(%d).String() = %q, want %q", tt.metric, result, tt.expected)
			}
		})
	}
}

func TestGenerateBatchBody(t *testing.T) {
	body := GenerateBatchBody()

	if len(body) == 0 {
		t.Error("GenerateBatchBody returned empty byte slice")
	}

	t.Logf("GenerateBatchBody generated %d bytes of WASM code", len(body))
}

func TestEuclideanBatchInto_EmptyVectors(t *testing.T) {
	rt := getJitRuntime()
	if rt == nil {
		t.Skip("JIT runtime not available")
	}

	query := []float32{1.0, 2.0, 3.0}
	vectors := [][]float32{}
	results := make([]float32, 0)

	err := rt.EuclideanBatchInto(query, vectors, results)
	if err != nil {
		t.Errorf("EuclideanBatchInto with empty vectors returned error: %v", err)
	}
}

func TestEuclideanBatchInto_ResultsTooSmall(t *testing.T) {
	rt := getJitRuntime()
	if rt == nil {
		t.Skip("JIT runtime not available")
	}

	query := []float32{1.0, 2.0, 3.0}
	vectors := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
	}
	results := make([]float32, 1)

	err := rt.EuclideanBatchInto(query, vectors, results)
	if err == nil {
		t.Error("EuclideanBatchInto with results too small should return error")
	}
}

func TestEuclideanBatchInto_SingleVector(t *testing.T) {
	rt := getJitRuntime()
	if rt == nil {
		t.Skip("JIT runtime not available")
	}

	query := []float32{1.0, 0.0, 0.0}
	vectors := [][]float32{
		{1.0, 0.0, 0.0},
	}
	results := make([]float32, 1)

	err := rt.EuclideanBatchInto(query, vectors, results)
	if err != nil {
		t.Errorf("EuclideanBatchInto returned error: %v", err)
	}

	if results[0] != 0.0 {
		t.Errorf("EuclideanBatchInto expected 0.0 for identical vectors, got %f", results[0])
	}
}

func TestEuclideanBatchInto_MultipleVectors(t *testing.T) {
	rt := getJitRuntime()
	if rt == nil {
		t.Skip("JIT runtime not available")
	}

	query := []float32{0.0, 0.0, 0.0}
	vectors := [][]float32{
		{3.0, 4.0, 0.0},
		{0.0, 0.0, 5.0},
		{1.0, 1.0, 1.0},
	}
	results := make([]float32, 3)

	err := rt.EuclideanBatchInto(query, vectors, results)
	if err != nil {
		t.Errorf("EuclideanBatchInto returned error: %v", err)
	}

	if results[0] != 5.0 {
		t.Errorf("EuclideanBatchInto expected 5.0 for first vector, got %f", results[0])
	}
	if results[1] != 5.0 {
		t.Errorf("EuclideanBatchInto expected 5.0 for second vector, got %f", results[1])
	}
	if results[2] < 1.732 || results[2] > 1.733 {
		t.Errorf("EuclideanBatchInto expected ~1.732 for third vector, got %f", results[2])
	}
}

func TestEuclideanBatchInto_128Dimensions(t *testing.T) {
	rt := getJitRuntime()
	if rt == nil {
		t.Skip("JIT runtime not available")
	}

	dim := 128
	query := make([]float32, dim)
	vectors := make([][]float32, 2)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range dim {
			query[j] = float32(j) * 0.01
			vectors[i][j] = float32(j) * 0.01
		}
	}
	results := make([]float32, 2)

	err := rt.EuclideanBatchInto(query, vectors, results)
	if err != nil {
		t.Errorf("EuclideanBatchInto returned error: %v", err)
	}

	if results[0] != 0.0 {
		t.Errorf("EuclideanBatchInto expected 0.0 for identical vectors, got %f", results[0])
	}
}

func TestEuclideanBatchInto_ReuseResults(t *testing.T) {
	rt := getJitRuntime()
	if rt == nil {
		t.Skip("JIT runtime not available")
	}

	query := []float32{1.0, 2.0, 3.0}
	vectors := [][]float32{
		{1.0, 2.0, 3.0},
	}
	results := make([]float32, 2)
	results[0] = 999.0
	results[1] = 888.0

	err := rt.EuclideanBatchInto(query, vectors, results)
	if err != nil {
		t.Errorf("EuclideanBatchInto returned error: %v", err)
	}

	if results[0] != 0.0 {
		t.Errorf("EuclideanBatchInto should overwrite first result, got %f", results[0])
	}
	if results[1] != 888.0 {
		t.Errorf("EuclideanBatchInto should not modify unused result, got %f", results[1])
	}
}

func getJitRuntime() *JitRuntime {
	if !jitEnabled {
		return nil
	}
	return jitRT
}
