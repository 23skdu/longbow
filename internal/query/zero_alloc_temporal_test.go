package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestZeroAllocTemporalParser(t *testing.T) {
	parser := NewZeroAllocTemporalParser()

	t.Run("ParseSearch", func(t *testing.T) {
		json := `{"search_type": "range", "k": 10, "start_time": 1000, "end_time": 2000, "duration": 3600000000000}`
		req, err := parser.ParseSearch([]byte(json))
		assert.NoError(t, err)
		assert.Equal(t, "range", req.SearchType)
		assert.Equal(t, 10, req.K)
		assert.Equal(t, int64(1000), req.StartTime)
		assert.Equal(t, int64(2000), req.EndTime)
		assert.Equal(t, time.Hour, req.Duration)
	})

	t.Run("ParseAggregation", func(t *testing.T) {
		json := `{"aggregation_type": "mean", "start_time": 1000, "end_time": 5000, "interval": 1000000000, "metric_field": "price"}`
		req, err := parser.ParseAggregation([]byte(json))
		assert.NoError(t, err)
		assert.Equal(t, "mean", req.AggregationType)
		assert.Equal(t, int64(1000), req.StartTime)
		assert.Equal(t, int64(5000), req.EndTime)
		assert.Equal(t, int64(1*time.Second), req.Interval)
		assert.Equal(t, "price", req.MetricField)
	})

	t.Run("Unknown Fields", func(t *testing.T) {
		json := `{"aggregation_type": "count", "unknown": "field", "interval": 1000}`
		req, err := parser.ParseAggregation([]byte(json))
		assert.NoError(t, err)
		assert.Equal(t, "count", req.AggregationType)
		assert.Equal(t, int64(1000), req.Interval)
	})
}
