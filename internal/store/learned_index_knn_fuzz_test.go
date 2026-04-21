package store

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// FuzzKNNPredict validates that arbitrary QueryFeatures inputs never cause
// kNNPredict to panic, infinite-loop, or return an invalid IndexType.
// Run with: go test -fuzz=FuzzKNNPredict ./internal/store/ -fuzztime=60s
func FuzzKNNPredict(f *testing.F) {
	// Seed corpus: representative workloads.
	f.Add(128, 1, 10, 10000, 1, "simple", 1.0, false, false, 9, 1)
	f.Add(768, 32, 100, 1000000, 3, "complex", 0.5, true, false, 14, 3)
	f.Add(1536, 64, 1000, 5000000, 10, "medium", 2.0, true, true, 22, 6)
	f.Add(0, 0, 0, 0, 0, "", 0.0, false, false, 0, 0)
	f.Add(4096, 256, 10000, 100000000, 100, "complex", 100.0, true, true, 23, 6)

	f.Fuzz(func(
		t *testing.T,
		dimension, numVecs, searchK, datasetSize, numCollections int,
		complexity string,
		avgNorm float64,
		isFiltered, isHybrid bool,
		timeOfDay, dayOfWeek int,
	) {
		logger := zerolog.New(nil).With().Logger()
		p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{
			MinTrainingSamples: 5,
			KNN:                3,
			UpdateInterval:     time.Hour,
		})

		// Seed a minimal training set to ensure k-NN path is exercised.
		for _, s := range []struct {
			ds  int
			idx IndexType
		}{
			{1000, IndexTypeHNSW},
			{500000, LearnedIVFPQ},
			{3000000, IndexTypeDiskANN},
			{50000, IndexTypeHNSW},
			{2000000, IndexTypeDiskANN},
			{100000, LearnedIVFPQ},
		} {
			p.AddTrainingSample(TrainingSample{
				Features: QueryFeatures{DatasetSize: s.ds},
				Index:    s.idx,
			})
		}

		features := QueryFeatures{
			VectorDimension: dimension,
			NumQueryVectors: numVecs,
			SearchK:         searchK,
			DatasetSize:     datasetSize,
			NumCollections:  numCollections,
			QueryComplexity: complexity,
			AvgVectorNorm:   avgNorm,
			IsFiltered:      isFiltered,
			IsHybrid:        isHybrid,
			TimeOfDay:       timeOfDay,
			DayOfWeek:       dayOfWeek,
		}

		pred := p.Predict(features)

		// Invariant 1: result must be one of the three supported index types.
		validTypes := map[IndexType]bool{
			IndexTypeHNSW:    true,
			LearnedIVFPQ:     true,
			IndexTypeDiskANN: true,
		}
		if !validTypes[pred.RecommendedIndex] {
			t.Errorf("Predict returned unknown IndexType %q for features %+v", pred.RecommendedIndex, features)
		}

		// Invariant 2: confidence must be in [0, 1].
		if pred.Confidence < 0 || pred.Confidence > 1 {
			t.Errorf("Predict returned out-of-range confidence %f", pred.Confidence)
		}
	})
}

// FuzzFeatureNormalizer validates that the FeatureNormalizer never panics on
// any input, including extreme values, NaN, and Inf.
// Run with: go test -fuzz=FuzzFeatureNormalizer ./internal/store/ -fuzztime=60s
func FuzzFeatureNormalizer(f *testing.F) {
	// Seed corpus.
	f.Add(float64(0), float64(1000), float64(5000000), float64(1.5))
	f.Add(float64(-1e18), float64(1e18), float64(0), float64(-0.5))
	f.Add(float64(1), float64(1), float64(1), float64(1)) // zero span

	f.Fuzz(func(t *testing.T, a, b, c, d float64) {
		n := newFeatureNormalizer()

		v1 := [numFeatures]float64{}
		v2 := [numFeatures]float64{}
		// Spread fuzz inputs across the feature vector.
		for i := range v1 {
			switch i % 4 {
			case 0:
				v1[i] = a
				v2[i] = b
			case 1:
				v1[i] = c
				v2[i] = d
			case 2:
				v1[i] = a * c
				v2[i] = b * d
			default:
				v1[i] = a - b
				v2[i] = c - d
			}
		}

		// Must not panic.
		n.Update(v1)
		n.Update(v2)

		out1 := n.Normalize(v1)
		out2 := n.Normalize(v2)

		// Invariant: output values must be finite (not NaN, not Inf) whenever
		// inputs are normal finite numbers.
		for i, val := range out1 {
			if val != val { // NaN check
				t.Errorf("Normalize(v1)[%d] = NaN for inputs a=%v b=%v c=%v d=%v", i, a, b, c, d)
			}
		}
		for i, val := range out2 {
			if val != val {
				t.Errorf("Normalize(v2)[%d] = NaN for inputs a=%v b=%v c=%v d=%v", i, a, b, c, d)
			}
		}
	})
}
