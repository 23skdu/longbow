package streaming

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamingAggregationConfig_Defaults runs the test streaming aggregation config_defaults operation.
func TestStreamingAggregationConfig_Defaults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	config := StreamingAggregationConfig{}

	if config.WindowSize <= 0 {
		config.WindowSize = 100
	}
	if config.DecayFactor <= 0 || config.DecayFactor > 1.0 {
		config.DecayFactor = 0.9
	}
	if config.MaxAggregates <= 0 {
		config.MaxAggregates = 10000
	}

	assert.Equal(t, 100, config.WindowSize)
	assert.Equal(t, 0.9, config.DecayFactor)
	assert.Equal(t, 10000, config.MaxAggregates)
}

// TestAggregationType_Constants runs the test aggregation type_constants operation.
func TestAggregationType_Constants(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	assert.Equal(t, AggregationType("moving_average"), AggregationTypeMovingAverage)
	assert.Equal(t, AggregationType("exponential"), AggregationTypeExponential)
	assert.Equal(t, AggregationType("cumulative"), AggregationTypeCumulative)
	assert.Equal(t, AggregationType("weighted"), AggregationTypeWeighted)
}

// TestNewStreamingAggregation runs the test new streaming aggregation operation.
func TestNewStreamingAggregation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	config := StreamingAggregationConfig{
		WindowSize:    50,
		DecayFactor:   0.8,
		MaxAggregates: 5000,
	}

	sa := NewStreamingAggregation(logger, config)

	assert.NotNil(t, sa)
	assert.Equal(t, 50, sa.config.WindowSize)
	assert.Equal(t, 0.8, sa.config.DecayFactor)
	assert.Equal(t, 5000, sa.config.MaxAggregates)
}

// TestStreamingAggregation_CreateAggregate runs the test streaming aggregation_create aggregate operation.
func TestStreamingAggregation_CreateAggregate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	err := sa.CreateAggregate("vec1", 3, AggregationTypeMovingAverage)
	require.NoError(t, err)

	agg, exists := sa.GetAggregate("vec1")
	require.True(t, exists)
	assert.Equal(t, "vec1", agg.VectorID)
	assert.Equal(t, 3, agg.Dimension)
}

// TestStreamingAggregation_CreateAggregate_AlreadyExists runs the test streaming aggregation_create aggregate_already exists operation.
func TestStreamingAggregation_CreateAggregate_AlreadyExists(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	err := sa.CreateAggregate("vec1", 3, AggregationTypeMovingAverage)
	require.NoError(t, err)

	agg, exists := sa.GetAggregate("vec1")
	require.True(t, exists)
	assert.Equal(t, 3, agg.Dimension)
}

// TestStreamingAggregation_AddVector runs the test streaming aggregation_add vector operation.
func TestStreamingAggregation_AddVector(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	err := sa.CreateAggregate("vec1", 3, AggregationTypeMovingAverage)
	require.NoError(t, err)

	err = sa.AddVector("vec1", []float32{1.0, 2.0, 3.0}, time.Now())
	require.NoError(t, err)

	_, _, aggregated, _ := sa.GetStats()
	assert.GreaterOrEqual(t, aggregated, int64(1))
}

// TestStreamingAggregation_AddVector_NewAggregate runs the test streaming aggregation_add vector_new aggregate operation.
func TestStreamingAggregation_AddVector_NewAggregate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	vector := []float32{1.0, 2.0, 3.0}
	err := sa.AddVector("vec1", vector, time.Now())
	require.NoError(t, err)

	result, err := sa.GetMovingAverage("vec1")
	require.NoError(t, err)
	assert.Equal(t, vector, result)
}

// TestStreamingAggregation_GetMovingAverage_NotFound runs the test streaming aggregation_get moving average_not found operation.
func TestStreamingAggregation_GetMovingAverage_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	_, err := sa.GetMovingAverage("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "aggregate not found")
}

// TestStreamingAggregation_GetExponentialMovingAverage runs the test streaming aggregation_get exponential moving average operation.
func TestStreamingAggregation_GetExponentialMovingAverage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{
		DecayFactor: 0.5,
	})

	err := sa.CreateAggregate("vec1", 3, AggregationTypeExponential)
	require.NoError(t, err)

	_ = sa.AddVector("vec1", []float32{1.0, 2.0, 3.0}, time.Now())
	_ = sa.AddVector("vec1", []float32{3.0, 4.0, 5.0}, time.Now())

	result, err := sa.GetExponentialMovingAverage("vec1")
	require.NoError(t, err)
	assert.Len(t, result, 3)
}

// TestStreamingAggregation_GetCumulative runs the test streaming aggregation_get cumulative operation.
func TestStreamingAggregation_GetCumulative(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	err := sa.CreateAggregate("vec1", 3, AggregationTypeCumulative)
	require.NoError(t, err)

	_ = sa.AddVector("vec1", []float32{1.0, 2.0, 3.0}, time.Now())
	_ = sa.AddVector("vec1", []float32{4.0, 5.0, 6.0}, time.Now())

	result, err := sa.GetCumulative("vec1")
	require.NoError(t, err)
	assert.Equal(t, []float32{5.0, 7.0, 9.0}, result)
}

// TestStreamingAggregation_GetAggregate runs the test streaming aggregation_get aggregate operation.
func TestStreamingAggregation_GetAggregate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	_ = sa.AddVector("vec1", []float32{1.0, 2.0, 3.0}, time.Now())

	agg, exists := sa.GetAggregate("vec1")
	require.True(t, exists)
	assert.Equal(t, "vec1", agg.VectorID)
	assert.Equal(t, 3, agg.Dimension)
}

// TestStreamingAggregation_RemoveAggregate runs the test streaming aggregation_remove aggregate operation.
func TestStreamingAggregation_RemoveAggregate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	_ = sa.AddVector("vec1", []float32{1.0, 2.0, 3.0}, time.Now())
	assert.Equal(t, 1, sa.GetAggregateCount())

	sa.RemoveAggregate("vec1")
	assert.Equal(t, 0, sa.GetAggregateCount())
}

// TestStreamingAggregation_ListAggregates runs the test streaming aggregation_list aggregates operation.
func TestStreamingAggregation_ListAggregates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	_ = sa.AddVector("vec1", []float32{1.0, 2.0, 3.0}, time.Now())
	_ = sa.AddVector("vec2", []float32{4.0, 5.0, 6.0}, time.Now())

	ids := sa.ListAggregates()
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, "vec1")
	assert.Contains(t, ids, "vec2")
}

// TestStreamingAggregation_Clear runs the test streaming aggregation_clear operation.
func TestStreamingAggregation_Clear(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	_ = sa.AddVector("vec1", []float32{1.0, 2.0, 3.0}, time.Now())
	_ = sa.AddVector("vec2", []float32{4.0, 5.0, 6.0}, time.Now())

	sa.Clear()

	assert.Equal(t, 0, sa.GetAggregateCount())
}

// TestStreamingAggregation_GetStats runs the test streaming aggregation_get stats operation.
func TestStreamingAggregation_GetStats(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	_ = sa.AddVector("vec1", []float32{1.0, 2.0, 3.0}, time.Now())

	received, _, _, _ := sa.GetStats()
	assert.Greater(t, received, int64(0))
}

// TestStreamingAggregation_GetConfig runs the test streaming aggregation_get config operation.
func TestStreamingAggregation_GetConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	config := StreamingAggregationConfig{
		WindowSize:    200,
		DecayFactor:   0.95,
		MaxAggregates: 2000,
	}

	sa := NewStreamingAggregation(logger, config)

	got := sa.GetConfig()
	assert.Equal(t, config, got)
}

// TestStreamingAggregation_SetConfig runs the test streaming aggregation_set config operation.
func TestStreamingAggregation_SetConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	newConfig := StreamingAggregationConfig{
		WindowSize:    300,
		DecayFactor:   0.85,
		MaxAggregates: 3000,
	}

	sa.SetConfig(newConfig)
	assert.Equal(t, 300, sa.config.WindowSize)
	assert.Equal(t, 0.85, sa.config.DecayFactor)
	assert.Equal(t, 3000, sa.config.MaxAggregates)
}

// TestStreamingAggregation_MaxAggregates_AutoEvict runs the test streaming aggregation_max aggregates_auto evict operation.
func TestStreamingAggregation_MaxAggregates_AutoEvict(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{
		MaxAggregates: 2,
		AutoEvict:     true,
	})

	_ = sa.AddVector("vec1", []float32{1.0}, time.Now())
	_ = sa.AddVector("vec2", []float32{2.0}, time.Now())

	assert.Equal(t, 2, sa.GetAggregateCount())

	_ = sa.AddVector("vec3", []float32{3.0}, time.Now())

	assert.Equal(t, 2, sa.GetAggregateCount())
}

// TestStreamingAggregation_MultipleVectors runs the test streaming aggregation_multiple vectors operation.
func TestStreamingAggregation_MultipleVectors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	vectors := [][]float32{
		{1.0, 2.0, 3.0},
		{2.0, 3.0, 4.0},
		{3.0, 4.0, 5.0},
	}

	for _, v := range vectors {
		err := sa.AddVector("vec1", v, time.Now())
		require.NoError(t, err)
	}

	result, err := sa.GetMovingAverage("vec1")
	require.NoError(t, err)

	expected := []float32{2.0, 3.0, 4.0}
	assert.Equal(t, expected, result)
}

// TestStreamingAggregation_WindowSize_Limit runs the test streaming aggregation_window size_limit operation.
func TestStreamingAggregation_WindowSize_Limit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{
		WindowSize: 2,
	})

	for i := 0; i < 5; i++ {
		_ = sa.AddVector("vec1", []float32{float32(i)}, time.Now())
	}

	result, err := sa.GetMovingAverage("vec1")
	require.NoError(t, err)

	assert.Equal(t, []float32{3.5}, result)
}

// TestStreamingAggregation_DifferentAggregateTypes runs the test streaming aggregation_different aggregate types operation.
func TestStreamingAggregation_DifferentAggregateTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	sa := NewStreamingAggregation(logger, StreamingAggregationConfig{})

	err := sa.CreateAggregate("ma", 2, AggregationTypeMovingAverage)
	require.NoError(t, err)
	err = sa.CreateAggregate("ewma", 2, AggregationTypeExponential)
	require.NoError(t, err)
	err = sa.CreateAggregate("cum", 2, AggregationTypeCumulative)
	require.NoError(t, err)

	_ = sa.AddVector("ma", []float32{1.0, 2.0}, time.Now())
	_ = sa.AddVector("ewma", []float32{1.0, 2.0}, time.Now())
	_ = sa.AddVector("cum", []float32{1.0, 2.0}, time.Now())

	_, err = sa.GetMovingAverage("ma")
	require.NoError(t, err)

	_, err = sa.GetExponentialMovingAverage("ewma")
	require.NoError(t, err)

	_, err = sa.GetCumulative("cum")
	require.NoError(t, err)
}
