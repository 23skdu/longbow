package tuning

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type AutoTuner struct {
	logger *zerolog.Logger

	config AutoTunerConfig

	mu           sync.RWMutex
	tunings      map[string]*TuningParameter
	measurements []Measurement

	enabled atomic.Bool

	stopCh chan struct{}
	wg     sync.WaitGroup
}

type AutoTunerConfig struct {
	Enabled        bool
	SampleInterval time.Duration
	MinSamples     int
	Confidence     float64
	LearningRate   float64
}

type TuningParameter struct {
	Name     string
	MinValue float64
	MaxValue float64

	tuningType TuningType
	history    []float64
	mu         sync.RWMutex

	value atomic.Value
}

type TuningType int

const (
	TuningTypeContinuous TuningType = iota
	TuningTypeDiscrete
)

type Measurement struct {
	Timestamp time.Time
	Parameter string
	Value     float64
	Metric    float64
}

func NewAutoTuner(config AutoTunerConfig, logger *zerolog.Logger) *AutoTuner {
	if config.SampleInterval == 0 {
		config.SampleInterval = 10 * time.Second
	}
	if config.MinSamples == 0 {
		config.MinSamples = 10
	}
	if config.Confidence == 0 {
		config.Confidence = 0.95
	}
	if config.LearningRate == 0 {
		config.LearningRate = 0.1
	}

	return &AutoTuner{
		logger:  logger,
		config:  config,
		tunings: make(map[string]*TuningParameter),
		stopCh:  make(chan struct{}),
	}
}

func (a *AutoTuner) RegisterParameter(name string, minVal, maxVal float64, tType TuningType) *TuningParameter {
	param := &TuningParameter{
		Name:       name,
		MinValue:   minVal,
		MaxValue:   maxVal,
		tuningType: tType,
		history:    make([]float64, 0, 100),
	}
	param.value.Store((minVal + maxVal) / 2)

	a.mu.Lock()
	a.tunings[name] = param
	a.mu.Unlock()

	return param
}

func (a *AutoTuner) Start(ctx context.Context) error {
	if !a.config.Enabled {
		a.logger.Info().Msg("Auto-tuner disabled")
		return nil
	}

	if !a.enabled.CompareAndSwap(false, true) {
		return nil
	}

	a.wg.Add(1)
	go a.tuningLoop(ctx)

	a.logger.Info().Msg("Auto-tuner started")
	return nil
}

func (a *AutoTuner) Stop() error {
	if !a.enabled.CompareAndSwap(true, false) {
		return nil
	}

	close(a.stopCh)
	a.wg.Wait()

	a.logger.Info().Msg("Auto-tuner stopped")
	return nil
}

func (a *AutoTuner) tuningLoop(ctx context.Context) {
	defer a.wg.Done()

	ticker := time.NewTicker(a.config.SampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		case <-ticker.C:
			a.collectAndAdjust()
		}
	}
}

func (a *AutoTuner) collectAndAdjust() {
	a.mu.RLock()
	tunings := make(map[string]*TuningParameter)
	for k, v := range a.tunings {
		tunings[k] = v
	}
	a.mu.RUnlock()

	for _, param := range tunings {
		a.adjustParameter(param)
	}
}

func (a *AutoTuner) adjustParameter(param *TuningParameter) {
	param.mu.Lock()
	history := make([]float64, len(param.history))
	copy(history, param.history)
	param.mu.Unlock()

	if len(history) < a.config.MinSamples {
		return
	}

	mean := calculateMean(history)
	stdDev := calculateStdDev(history, mean)

	gradient := a.estimateGradient(history)
	stepSize := a.config.LearningRate * stdDev

	currentVal := param.value.Load().(float64)
	newValue := currentVal + gradient*stepSize

	if newValue < param.MinValue {
		newValue = param.MinValue
	}
	if newValue > param.MaxValue {
		newValue = param.MaxValue
	}

	param.value.Store(newValue)

	param.mu.Lock()
	param.history = append(param.history, newValue)
	if len(param.history) > 100 {
		param.history = param.history[1:]
	}
	param.mu.Unlock()
}

func (a *AutoTuner) estimateGradient(history []float64) float64 {
	if len(history) < 2 {
		return 0
	}

	recent := history[len(history)-10:]
	if len(recent) < 2 {
		return 0
	}

	meanRecent := calculateMean(recent)
	meanAll := calculateMean(history)

	return meanRecent - meanAll
}

func calculateMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func calculateStdDev(values []float64, mean float64) float64 {
	if len(values) == 0 {
		return 0
	}

	variance := 0.0
	for _, v := range values {
		diff := v - mean
		variance += diff * diff
	}
	variance /= float64(len(values))

	return math.Sqrt(variance)
}

func (a *AutoTuner) RecordMeasurement(paramName string, value, metric float64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.measurements = append(a.measurements, Measurement{
		Timestamp: time.Now(),
		Parameter: paramName,
		Value:     value,
		Metric:    metric,
	})

	if len(a.measurements) > 1000 {
		a.measurements = a.measurements[1:]
	}
}

func (a *AutoTuner) GetParameterValue(name string) float64 {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if param, ok := a.tunings[name]; ok {
		return param.value.Load().(float64)
	}
	return 0
}

func (a *AutoTuner) GetAllTunings() map[string]float64 {
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make(map[string]float64)
	for name, param := range a.tunings {
		result[name] = param.value.Load().(float64)
	}
	return result
}

type WorkloadAnalyzer struct {
	mu         sync.RWMutex
	samples    []WorkloadSample
	maxSamples int
}

type WorkloadSample struct {
	Timestamp  time.Time
	QueryRate  float64
	LatencyP50 float64
	LatencyP99 float64
	CPUUsage   float64
}

func NewWorkloadAnalyzer(maxSamples int) *WorkloadAnalyzer {
	if maxSamples == 0 {
		maxSamples = 1000
	}
	return &WorkloadAnalyzer{
		maxSamples: maxSamples,
		samples:    make([]WorkloadSample, 0, maxSamples),
	}
}

func (w *WorkloadAnalyzer) RecordSample(rate, latP50, latP99, cpu float64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.samples = append(w.samples, WorkloadSample{
		Timestamp:  time.Now(),
		QueryRate:  rate,
		LatencyP50: latP50,
		LatencyP99: latP99,
		CPUUsage:   cpu,
	})

	if len(w.samples) > w.maxSamples {
		w.samples = w.samples[1:]
	}
}

type WorkloadProfile struct {
	Classification   string
	AverageQueryRate float64
}

func (w *WorkloadAnalyzer) Analyze() WorkloadProfile {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if len(w.samples) == 0 {
		return WorkloadProfile{Classification: "unknown"}
	}

	avgRate := 0.0
	for _, s := range w.samples {
		avgRate += s.QueryRate
	}
	avgRate /= float64(len(w.samples))

	classification := "balanced"
	if avgRate > 5000 {
		classification = "throughput"
	} else if avgRate < 100 {
		classification = "latency"
	}

	return WorkloadProfile{
		Classification:   classification,
		AverageQueryRate: avgRate,
	}
}
