package tuning

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestNewAutoTuner runs the test new auto tuner operation.
func TestNewAutoTuner(t *testing.T) {
	logger := zerolog.New(nil)
	config := AutoTunerConfig{
		Enabled:        true,
		SampleInterval: time.Millisecond,
		MinSamples:     5,
		Confidence:     0.95,
		LearningRate:   0.1,
	}

	at := NewAutoTuner(config, &logger)
	if at == nil {
		t.Fatal("NewAutoTuner returned nil")
	}

	if at.config.SampleInterval != time.Millisecond {
		t.Errorf("expected SampleInterval %v, got %v", time.Millisecond, at.config.SampleInterval)
	}
}

// TestNewAutoTuner_Defaults runs the test new auto tuner_defaults operation.
func TestNewAutoTuner_Defaults(t *testing.T) {
	logger := zerolog.New(nil)
	config := AutoTunerConfig{
		Enabled: true,
	}

	at := NewAutoTuner(config, &logger)
	if at.config.SampleInterval != 10*time.Second {
		t.Errorf("expected default SampleInterval 10s, got %v", at.config.SampleInterval)
	}
	if at.config.MinSamples != 10 {
		t.Errorf("expected default MinSamples 10, got %d", at.config.MinSamples)
	}
	if at.config.Confidence != 0.95 {
		t.Errorf("expected default Confidence 0.95, got %f", at.config.Confidence)
	}
	if at.config.LearningRate != 0.1 {
		t.Errorf("expected default LearningRate 0.1, got %f", at.config.LearningRate)
	}
}

// TestAutoTuner_RegisterParameter runs the test auto tuner_register parameter operation.
func TestAutoTuner_RegisterParameter(t *testing.T) {
	logger := zerolog.New(nil)
	at := NewAutoTuner(AutoTunerConfig{Enabled: false}, &logger)

	param := at.RegisterParameter("test_param", 0, 100, TuningTypeContinuous)
	if param == nil {
		t.Fatal("RegisterParameter returned nil")
	}

	if param.Name != "test_param" {
		t.Errorf("expected name 'test_param', got '%s'", param.Name)
	}
	if param.MinValue != 0 {
		t.Errorf("expected MinValue 0, got %f", param.MinValue)
	}
	if param.MaxValue != 100 {
		t.Errorf("expected MaxValue 100, got %f", param.MaxValue)
	}
}

// TestAutoTuner_GetParameterValue runs the test auto tuner_get parameter value operation.
func TestAutoTuner_GetParameterValue(t *testing.T) {
	logger := zerolog.New(nil)
	at := NewAutoTuner(AutoTunerConfig{Enabled: false}, &logger)

	at.RegisterParameter("test_param", 0, 100, TuningTypeContinuous)

	value := at.GetParameterValue("test_param")
	if value == 0 {
		t.Error("expected non-zero initial value")
	}
}

// TestAutoTuner_GetParameterValue_NotFound runs the test auto tuner_get parameter value_not found operation.
func TestAutoTuner_GetParameterValue_NotFound(t *testing.T) {
	logger := zerolog.New(nil)
	at := NewAutoTuner(AutoTunerConfig{Enabled: false}, &logger)

	value := at.GetParameterValue("nonexistent")
	if value != 0 {
		t.Errorf("expected 0 for nonexistent param, got %f", value)
	}
}

// TestAutoTuner_GetAllTunings runs the test auto tuner_get all tunings operation.
func TestAutoTuner_GetAllTunings(t *testing.T) {
	logger := zerolog.New(nil)
	at := NewAutoTuner(AutoTunerConfig{Enabled: false}, &logger)

	at.RegisterParameter("param1", 0, 100, TuningTypeContinuous)
	at.RegisterParameter("param2", 0, 50, TuningTypeDiscrete)

	tunings := at.GetAllTunings()
	if len(tunings) != 2 {
		t.Errorf("expected 2 tunings, got %d", len(tunings))
	}

	if _, ok := tunings["param1"]; !ok {
		t.Error("expected param1 in tunings")
	}
	if _, ok := tunings["param2"]; !ok {
		t.Error("expected param2 in tunings")
	}
}

// TestAutoTuner_Start_Disabled runs the test auto tuner_start_disabled operation.
func TestAutoTuner_Start_Disabled(t *testing.T) {
	logger := zerolog.New(nil)
	at := NewAutoTuner(AutoTunerConfig{Enabled: false}, &logger)

	err := at.Start(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAutoTuner_Start_Enabled runs the test auto tuner_start_enabled operation.
func TestAutoTuner_Start_Enabled(t *testing.T) {
	logger := zerolog.New(nil)
	at := NewAutoTuner(AutoTunerConfig{
		Enabled:        true,
		SampleInterval: 10 * time.Millisecond,
	}, &logger)

	err := at.Start(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	err = at.Stop()
	if err != nil {
		t.Errorf("unexpected error on stop: %v", err)
	}
}

// TestAutoTuner_Start_Idempotent runs the test auto tuner_start_idempotent operation.
func TestAutoTuner_Start_Idempotent(t *testing.T) {
	logger := zerolog.New(nil)
	at := NewAutoTuner(AutoTunerConfig{
		Enabled:        true,
		SampleInterval: 10 * time.Millisecond,
	}, &logger)

	_ = at.Start(context.Background())
	err := at.Start(context.Background())
	if err != nil {
		t.Error("expected idempotent start")
	}

	_ = at.Stop()
}

// TestAutoTuner_Stop_Idempotent runs the test auto tuner_stop_idempotent operation.
func TestAutoTuner_Stop_Idempotent(t *testing.T) {
	logger := zerolog.New(nil)
	at := NewAutoTuner(AutoTunerConfig{
		Enabled:        true,
		SampleInterval: 10 * time.Millisecond,
	}, &logger)

	_ = at.Start(context.Background())
	_ = at.Stop()
	err := at.Stop()
	if err != nil {
		t.Error("expected idempotent stop")
	}
}

// TestAutoTuner_RecordMeasurement runs the test auto tuner_record measurement operation.
func TestAutoTuner_RecordMeasurement(t *testing.T) {
	logger := zerolog.New(nil)
	at := NewAutoTuner(AutoTunerConfig{Enabled: false}, &logger)

	at.RecordMeasurement("param1", 10.0, 0.5)
	at.RecordMeasurement("param1", 20.0, 0.6)
	at.RecordMeasurement("param2", 5.0, 0.3)

	if len(at.measurements) != 3 {
		t.Errorf("expected 3 measurements, got %d", len(at.measurements))
	}
}

// TestAutoTuner_RecordMeasurement_Limit runs the test auto tuner_record measurement_limit operation.
func TestAutoTuner_RecordMeasurement_Limit(t *testing.T) {
	logger := zerolog.New(nil)
	at := NewAutoTuner(AutoTunerConfig{Enabled: false}, &logger)

	for i := 0; i < 1500; i++ {
		at.RecordMeasurement("param1", float64(i), 0.5)
	}

	if len(at.measurements) > 1000 {
		t.Errorf("expected measurements to be limited to 1000, got %d", len(at.measurements))
	}
}

// TestNewWorkloadAnalyzer runs the test new workload analyzer operation.
func TestNewWorkloadAnalyzer(t *testing.T) {
	wa := NewWorkloadAnalyzer(100)
	if wa == nil {
		t.Fatal("NewWorkloadAnalyzer returned nil")
	}

	if wa.maxSamples != 100 {
		t.Errorf("expected maxSamples 100, got %d", wa.maxSamples)
	}
}

// TestNewWorkloadAnalyzer_Default runs the test new workload analyzer_default operation.
func TestNewWorkloadAnalyzer_Default(t *testing.T) {
	wa := NewWorkloadAnalyzer(0)
	if wa.maxSamples != 1000 {
		t.Errorf("expected default maxSamples 1000, got %d", wa.maxSamples)
	}
}

// TestWorkloadAnalyzer_RecordSample runs the test workload analyzer_record sample operation.
func TestWorkloadAnalyzer_RecordSample(t *testing.T) {
	wa := NewWorkloadAnalyzer(100)

	wa.RecordSample(100, 10.0, 50.0, 0.5)
	wa.RecordSample(200, 20.0, 60.0, 0.6)

	if len(wa.samples) != 2 {
		t.Errorf("expected 2 samples, got %d", len(wa.samples))
	}
}

// TestWorkloadAnalyzer_RecordSample_Limit runs the test workload analyzer_record sample_limit operation.
func TestWorkloadAnalyzer_RecordSample_Limit(t *testing.T) {
	wa := NewWorkloadAnalyzer(100)

	for i := 0; i < 150; i++ {
		wa.RecordSample(float64(i), 10.0, 50.0, 0.5)
	}

	if len(wa.samples) > 100 {
		t.Errorf("expected samples to be limited to 100, got %d", len(wa.samples))
	}
}

// TestWorkloadAnalyzer_Analyze_Empty runs the test workload analyzer_analyze_empty operation.
func TestWorkloadAnalyzer_Analyze_Empty(t *testing.T) {
	wa := NewWorkloadAnalyzer(100)

	profile := wa.Analyze()
	if profile.Classification != "unknown" {
		t.Errorf("expected 'unknown' classification, got '%s'", profile.Classification)
	}
}

// TestWorkloadAnalyzer_Analyze_Throughput runs the test workload analyzer_analyze_throughput operation.
func TestWorkloadAnalyzer_Analyze_Throughput(t *testing.T) {
	wa := NewWorkloadAnalyzer(100)

	for i := 0; i < 10; i++ {
		wa.RecordSample(6000, 10.0, 50.0, 0.5)
	}

	profile := wa.Analyze()
	if profile.Classification != "throughput" {
		t.Errorf("expected 'throughput' classification, got '%s'", profile.Classification)
	}
}

// TestWorkloadAnalyzer_Analyze_Latency runs the test workload analyzer_analyze_latency operation.
func TestWorkloadAnalyzer_Analyze_Latency(t *testing.T) {
	wa := NewWorkloadAnalyzer(100)

	for i := 0; i < 10; i++ {
		wa.RecordSample(50, 10.0, 50.0, 0.5)
	}

	profile := wa.Analyze()
	if profile.Classification != "latency" {
		t.Errorf("expected 'latency' classification, got '%s'", profile.Classification)
	}
}

// TestWorkloadAnalyzer_Analyze_Balanced runs the test workload analyzer_analyze_balanced operation.
func TestWorkloadAnalyzer_Analyze_Balanced(t *testing.T) {
	wa := NewWorkloadAnalyzer(100)

	for i := 0; i < 10; i++ {
		wa.RecordSample(500, 10.0, 50.0, 0.5)
	}

	profile := wa.Analyze()
	if profile.Classification != "balanced" {
		t.Errorf("expected 'balanced' classification, got '%s'", profile.Classification)
	}
}

// TestWorkloadAnalyzer_Analyze_AverageQueryRate runs the test workload analyzer_analyze_average query rate operation.
func TestWorkloadAnalyzer_Analyze_AverageQueryRate(t *testing.T) {
	wa := NewWorkloadAnalyzer(100)

	wa.RecordSample(100, 10.0, 50.0, 0.5)
	wa.RecordSample(200, 10.0, 50.0, 0.5)

	profile := wa.Analyze()
	if profile.AverageQueryRate != 150 {
		t.Errorf("expected AverageQueryRate 150, got %f", profile.AverageQueryRate)
	}
}

// TestWorkloadAnalyzer_Concurrent runs the test workload analyzer_concurrent operation.
func TestWorkloadAnalyzer_Concurrent(t *testing.T) {
	wa := NewWorkloadAnalyzer(1000)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				wa.RecordSample(100, 10.0, 50.0, 0.5)
			}
		}()
	}

	wg.Wait()

	profile := wa.Analyze()
	if profile.AverageQueryRate == 0 {
		t.Error("expected non-zero average query rate")
	}
}

// TestCalculateMean runs the test calculate mean operation.
func TestCalculateMean(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5}
	mean := calculateMean(values)
	if mean != 3 {
		t.Errorf("expected mean 3, got %f", mean)
	}
}

// TestCalculateMean_Empty runs the test calculate mean_empty operation.
func TestCalculateMean_Empty(t *testing.T) {
	values := []float64{}
	mean := calculateMean(values)
	if mean != 0 {
		t.Errorf("expected mean 0 for empty, got %f", mean)
	}
}

// TestCalculateStdDev runs the test calculate std dev operation.
func TestCalculateStdDev(t *testing.T) {
	values := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	mean := calculateMean(values)
	stdDev := calculateStdDev(values, mean)
	if stdDev < 2 || stdDev > 2.5 {
		t.Errorf("expected stdDev around 2.29, got %f", stdDev)
	}
}

// TestCalculateStdDev_Empty runs the test calculate std dev_empty operation.
func TestCalculateStdDev_Empty(t *testing.T) {
	values := []float64{}
	mean := calculateMean(values)
	stdDev := calculateStdDev(values, mean)
	if stdDev != 0 {
		t.Errorf("expected stdDev 0 for empty, got %f", stdDev)
	}
}
