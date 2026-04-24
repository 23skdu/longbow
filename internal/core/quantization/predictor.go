package quantization

import (
	"math/rand"
)

// BitWidthPredictor estimates the optimal bit-depth for a vector dataset
type BitWidthPredictor struct {
	sampleSize int
	seed       int64
}

func NewBitWidthPredictor(sampleSize int) *BitWidthPredictor {
	return &BitWidthPredictor{
		sampleSize: sampleSize,
		seed:       42,
	}
}

// Predict determines if a vector batch should use 1, 2, 4, or 8 bits
// based on the variance of a randomly projected sample.
func (p *BitWidthPredictor) Predict(vectors [][]float32) int {
	if len(vectors) == 0 {
		return 8
	}

	// Simple heuristic: calculate mean variance across dimensions
	dim := len(vectors[0])
	sampleCount := p.sampleSize
	if len(vectors) < sampleCount {
		sampleCount = len(vectors)
	}

	rng := rand.New(rand.NewSource(p.seed))
	
	// Sample variance
	var totalVar float64
	for d := 0; d < dim; d++ {
		var sum, sumSq float64
		for i := 0; i < sampleCount; i++ {
			idx := rng.Intn(len(vectors))
			val := float64(vectors[idx][d])
			sum += val
			sumSq += val * val
		}
		mean := sum / float64(sampleCount)
		variance := (sumSq / float64(sampleCount)) - (mean * mean)
		totalVar += variance
	}
	
	avgVar := totalVar / float64(dim)

	// Bit-depth threshold heuristic
	if avgVar < 0.01 {
		return 1 // Binary
	} else if avgVar < 0.05 {
		return 2
	} else if avgVar < 0.2 {
		return 4
	}
	return 8
}
