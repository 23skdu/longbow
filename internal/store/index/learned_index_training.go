package index

import (
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// AddTrainingSample incorporates a new observation into the predictor's training set.
func (p *IndexPerformancePredictor) AddTrainingSample(sample TrainingSample) {
	// Correctness tracking: if the last k-NN prediction matched this observed outcome, count it.
	if v := p.lastPredictedIdx.Load(); v != nil {
		if last, ok := v.(IndexType); ok && last == sample.Index {
			p.stats.PredictionCorrect.Add(1)
			metrics.LearnedIndexPredictionCorrectTotal.Inc()
		}
	}

	// Update the feature normaliser with the raw feature vector for this sample.
	fv := extractFeatureVector(sample.Features)
	p.normalizer.Update(fv)

	p.samplesMu.Lock()
	p.samples = append(p.samples, sample)
	if len(p.samples) > 10000 {
		metrics.LearnedIndexSampleOverflowTotal.Inc()
		p.samples = p.samples[len(p.samples)-10000:]
	}
	count := len(p.samples)
	p.samplesMu.Unlock()

	p.stats.TrainingSamplesCollected.Add(1)
	metrics.LearnedIndexTrainingSamplesTotal.Set(float64(count))

	// Trigger an async weight update when: enough samples collected, no update running,
	// and at least UpdateInterval has elapsed since the last one.
	if count >= p.config.MinTrainingSamples &&
		!p.updateInProgress.Load() &&
		time.Since(p.lastWeightUpdate) >= p.config.UpdateInterval {
		p.updateInProgress.Store(true)
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer p.updateInProgress.Store(false)
			p.updateWeights()
		}()
	}
}

// GetStats returns the current performance statistics of the predictor.
func (p *IndexPerformancePredictor) GetStats() (samples, predictions, correct int64) {
	return p.stats.TrainingSamplesCollected.Load(),
		p.stats.PredictionsMade.Load(),
		p.stats.PredictionCorrect.Load()
}

// GetConfig returns the current configuration of the learned index predictor.
func (p *IndexPerformancePredictor) GetConfig() LearnedIndexConfig {
	return p.config
}

// SetConfig updates the configuration of the learned index predictor.
func (p *IndexPerformancePredictor) SetConfig(config LearnedIndexConfig) {
	p.config = config
}

// GetTrainingSampleCount returns the number of samples currently in the training set.
func (p *IndexPerformancePredictor) GetTrainingSampleCount() int {
	p.samplesMu.RLock()
	defer p.samplesMu.RUnlock()
	return len(p.samples)
}

// ClearTrainingData removes all training samples and resets the predictor stats.
func (p *IndexPerformancePredictor) ClearTrainingData() {
	p.samplesMu.Lock()
	defer p.samplesMu.Unlock()
	p.samples = make([]TrainingSample, 0, 10000)
}
