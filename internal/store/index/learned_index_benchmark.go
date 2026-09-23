package index

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// IndexBenchmark manages performance comparisons between learned and fixed index configurations.
type IndexBenchmark struct {
	logger            zerolog.Logger
	predictor         *IndexPerformancePredictor
	fixedIndexConfigs []IndexType
	results           []LearnedBenchmarkResult
	resultsMu         sync.RWMutex
	stats             BenchmarkStats
}

// LearnedBenchmarkResult holds the outcome of a single performance comparison.
type LearnedBenchmarkResult struct {
	Features       QueryFeatures
	LearnedIndex   IndexType
	FixedIndex     IndexType
	LearnedLatency time.Duration
	FixedLatency   time.Duration
	LearnedRecall  float64
	FixedRecall    float64
	SpeedupFactor  float64
	RecallDiff     float64
	IndexType      IndexType
}

// BenchmarkStats tracks cumulative performance across multiple benchmark runs.
type BenchmarkStats struct {
	BenchmarksRun      atomic.Int64
	LearnedWins        atomic.Int64
	FixedWins          atomic.Int64
	AvgSpeedup         atomic.Int64
	TotalQueriesTested atomic.Int64
}

// NewIndexBenchmark creates a new IndexBenchmark for comparing learned index performance against fixed configurations.
func NewIndexBenchmark(logger zerolog.Logger, predictor *IndexPerformancePredictor, fixedIndices []IndexType) *IndexBenchmark {
	if len(fixedIndices) == 0 {
		fixedIndices = []IndexType{IndexTypeHNSW, LearnedIVFPQ, IndexTypeDiskANN}
	}

	return &IndexBenchmark{
		logger:            logger,
		predictor:         predictor,
		fixedIndexConfigs: fixedIndices,
	}
}

// RunComparison executes a performance comparison between the recommended learned index and a fixed configuration.
func (b *IndexBenchmark) RunComparison(features QueryFeatures, numIterations int) LearnedBenchmarkResult {
	b.stats.BenchmarksRun.Add(1)

	prediction := b.predictor.Predict(features)
	learnedIndex := prediction.RecommendedIndex

	fixedIndex := b.selectFixedIndex(features)

	learnedLatency := b.simulateQuery(learnedIndex, features)
	fixedLatency := b.simulateQuery(fixedIndex, features)

	learnedRecall := b.simulateRecall(learnedIndex, features)
	fixedRecall := b.simulateRecall(fixedIndex, features)

	speedupFactor := float64(fixedLatency) / float64(learnedLatency+1)
	recallDiff := learnedRecall - fixedRecall

	result := LearnedBenchmarkResult{
		Features:       features,
		LearnedIndex:   learnedIndex,
		FixedIndex:     fixedIndex,
		LearnedLatency: learnedLatency,
		FixedLatency:   fixedLatency,
		LearnedRecall:  learnedRecall,
		FixedRecall:    fixedRecall,
		SpeedupFactor:  speedupFactor,
		RecallDiff:     recallDiff,
		IndexType:      learnedIndex,
	}

	b.resultsMu.Lock()
	b.results = append(b.results, result)
	b.resultsMu.Unlock()

	if speedupFactor > 1.0 {
		b.stats.LearnedWins.Add(1)
	} else {
		b.stats.FixedWins.Add(1)
	}

	b.stats.AvgSpeedup.Add(int64(speedupFactor * 100))
	b.stats.TotalQueriesTested.Add(int64(numIterations))

	return result
}

func (b *IndexBenchmark) selectFixedIndex(features QueryFeatures) IndexType {
	if features.DatasetSize < 100000 {
		return IndexTypeHNSW
	} else if features.DatasetSize < 5000000 {
		return LearnedIVFPQ
	}
	return IndexTypeDiskANN
}

func (b *IndexBenchmark) simulateQuery(index IndexType, features QueryFeatures) time.Duration {
	baseLatency := time.Millisecond * 50

	switch index {
	case IndexTypeHNSW:
		baseLatency = time.Millisecond * 10
		baseLatency += time.Duration(features.SearchK) * time.Microsecond * 5
	case LearnedIVFPQ:
		baseLatency = time.Millisecond * 20
		baseLatency += time.Duration(features.SearchK) * time.Microsecond * 3
	case IndexTypeDiskANN:
		baseLatency = time.Millisecond * 30
		baseLatency += time.Duration(features.SearchK) * time.Microsecond * 2
	}

	baseLatency += time.Duration(features.NumQueryVectors) * time.Millisecond * 2

	variance := float64(baseLatency) * 0.1 * (rand.Float64()*2 - 1) // #nosec G404
	return time.Duration(float64(baseLatency) + variance)
}

func (b *IndexBenchmark) simulateRecall(index IndexType, _ QueryFeatures) float64 {
	switch index {
	case IndexTypeHNSW:
		return 0.98
	case LearnedIVFPQ:
		return 0.90
	case IndexTypeDiskANN:
		return 0.95
	}
	return 0.95
}

// RunBatchBenchmark runs a series of benchmarks across multiple feature sets.
func (b *IndexBenchmark) RunBatchBenchmark(featureSets []QueryFeatures, iterationsPerSet int) []LearnedBenchmarkResult {
	results := make([]LearnedBenchmarkResult, 0, len(featureSets)*iterationsPerSet)

	for _, features := range featureSets {
		for i := 0; i < iterationsPerSet; i++ {
			result := b.RunComparison(features, iterationsPerSet)
			results = append(results, result)
		}
	}

	return results
}

// GetAggregatedStats computes summary statistics across all benchmark results.
func (b *IndexBenchmark) GetAggregatedStats() BenchmarkSummary {
	b.resultsMu.RLock()
	defer b.resultsMu.RUnlock()

	var totalSpeedup float64
	var totalRecallDiff float64
	learnedWins := 0
	fixedWins := 0

	for _, r := range b.results {
		totalSpeedup += r.SpeedupFactor
		totalRecallDiff += r.RecallDiff
		if r.SpeedupFactor > 1.0 {
			learnedWins++
		} else {
			fixedWins++
		}
	}

	count := len(b.results)
	if count == 0 {
		return BenchmarkSummary{}
	}

	return BenchmarkSummary{
		TotalBenchmarks:     count,
		LearnedIndexWins:    learnedWins,
		FixedIndexWins:      fixedWins,
		AvgSpeedupFactor:    totalSpeedup / float64(count),
		AvgRecallDifference: totalRecallDiff / float64(count),
		WinRateLearned:      float64(learnedWins) / float64(count),
		TotalQueriesTested:  int(b.stats.TotalQueriesTested.Load()),
	}
}

// GetResults returns a copy of all individual benchmark results.
func (b *IndexBenchmark) GetResults() []LearnedBenchmarkResult {
	b.resultsMu.RLock()
	defer b.resultsMu.RUnlock()

	result := make([]LearnedBenchmarkResult, len(b.results))
	copy(result, b.results)
	return result
}

// ClearResults removes all accumulated benchmark results.
func (b *IndexBenchmark) ClearResults() {
	b.resultsMu.Lock()
	defer b.resultsMu.Unlock()
	b.results = make([]LearnedBenchmarkResult, 0)
}

// GetStats returns high-level counters for the benchmark runner.
func (b *IndexBenchmark) GetStats() (runs, learnedWins, fixedWins, totalQueries int64) {
	return b.stats.BenchmarksRun.Load(),
		b.stats.LearnedWins.Load(),
		b.stats.FixedWins.Load(),
		b.stats.TotalQueriesTested.Load()
}

// BenchmarkSummary provides a high-level overview of benchmark outcomes.
type BenchmarkSummary struct {
	TotalBenchmarks     int
	LearnedIndexWins    int
	FixedIndexWins      int
	AvgSpeedupFactor    float64
	AvgRecallDifference float64
	WinRateLearned      float64
	TotalQueriesTested  int
}
