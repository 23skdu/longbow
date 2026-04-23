package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTemporalAggregator_Aggregate(t *testing.T) {
	aggregator := NewTemporalAggregator(100)

	now := time.Now().Truncate(time.Hour)
	vectors := []VectorTimestamp{
		{
			Timestamp: now,
			Vector:    []float32{1.0, 2.0},
			Metadata:  map[string]interface{}{"price": 10.0},
		},
		{
			Timestamp: now.Add(15 * time.Minute),
			Vector:    []float32{3.0, 4.0},
			Metadata:  map[string]interface{}{"price": 20.0},
		},
		{
			Timestamp: now.Add(2 * time.Hour),
			Vector:    []float32{5.0, 6.0},
			Metadata:  map[string]interface{}{"price": 30.0},
		},
	}

	t.Run("Count Aggregation", func(t *testing.T) {
		req := TemporalAggRequest{
			AggType:  TemporalAggCount,
			Interval: int64(time.Hour),
		}
		buckets := aggregator.Aggregate(req, vectors)
		assert.Equal(t, 2, len(buckets))
		assert.Equal(t, 2, buckets[0].Count)
		assert.Equal(t, 1, buckets[1].Count)
	})

	t.Run("Mean Aggregation on Vector", func(t *testing.T) {
		req := TemporalAggRequest{
			AggType:  TemporalAggMean,
			Interval: int64(time.Hour),
		}
		buckets := aggregator.Aggregate(req, vectors)
		assert.Equal(t, 2, len(buckets))
		// Bucket 1: [1.0, 2.0, 3.0, 4.0] -> Mean = 2.5
		assert.Equal(t, float32(2.5), *buckets[0].Mean)
		// Bucket 2: [5.0, 6.0] -> Mean = 5.5
		assert.Equal(t, float32(5.5), *buckets[1].Mean)
	})

	t.Run("Sum Aggregation on MetricField", func(t *testing.T) {
		req := TemporalAggRequest{
			AggType:     TemporalAggSum,
			Interval:    int64(time.Hour),
			MetricField: "price",
		}
		buckets := aggregator.Aggregate(req, vectors)
		assert.Equal(t, 2, len(buckets))
		// Bucket 1: 10.0 + 20.0 = 30.0
		assert.Equal(t, float32(30.0), *buckets[0].Sum)
		// Bucket 2: 30.0
		assert.Equal(t, float32(30.0), *buckets[1].Sum)
	})
}
