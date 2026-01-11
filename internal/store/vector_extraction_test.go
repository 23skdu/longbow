package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractVectors_F32_InPlace(t *testing.T) {
	h := &HNSWIndex{}
	h.vectorColIdx.Store(0)

	// Complex mock needed because h.extractVectors calls h.locationStore.Get and h.dataset.Records
	// For now, test that it handles empty/nil cases gracefully without panic
	results := []SearchResult{{ID: 0}}
	h.extractVectors(results, "f32")
	assert.Nil(t, results[0].Vector)
}

func TestFloat32ConversionHelpers(t *testing.T) {
	vec := []float32{1.0, 2.0, 3.0, 4.0}

	// F32 Conversion
	bytes := float32SliceToBytes(vec)
	assert.Equal(t, 16, len(bytes))

	// F16 Conversion
	f16Bytes := float32SliceToF16Bytes(vec)
	assert.Equal(t, 8, len(f16Bytes))
}

func TestArrowHNSW_ExtractVectors_Empty(t *testing.T) {
	h := &ArrowHNSW{}
	results := []SearchResult{{ID: 0}}
	h.extractVectors(results, "quantized")
	assert.Nil(t, results[0].Vector)
}

func BenchmarkExtractVectors(b *testing.B) {
	dim := 384
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = 1.0
	}

	b.Run("F32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = float32SliceToBytes(vec)
		}
	})

	b.Run("F16", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = float32SliceToF16Bytes(vec)
		}
	})
}
