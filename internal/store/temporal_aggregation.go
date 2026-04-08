package store

import (
	"sort"
	"sync"
)

type TemporalAggregator struct {
	mu            sync.RWMutex
	maxBucketSize int
}

type TemporalAggType string

const (
	TemporalAggCount TemporalAggType = "count"
	TemporalAggMin   TemporalAggType = "min"
	TemporalAggMax   TemporalAggType = "max"
	TemporalAggMean  TemporalAggType = "mean"
	TemporalAggSum   TemporalAggType = "sum"
)

type AggregationBucket struct {
	Timestamp int64    `json:"timestamp"`
	Count     int      `json:"count"`
	Min       *float32 `json:"min,omitempty"`
	Max       *float32 `json:"max,omitempty"`
	Mean      *float32 `json:"mean,omitempty"`
	Sum       *float32 `json:"sum,omitempty"`
}

type TemporalAggRequest struct {
	AggType     TemporalAggType `json:"aggregation_type"`
	StartTime   int64           `json:"start_time"`
	EndTime     int64           `json:"end_time"`
	Interval    int64           `json:"interval"`
	MetricField string          `json:"metric_field,omitempty"`
}

func NewTemporalAggregator(maxBuckets int) *TemporalAggregator {
	if maxBuckets <= 0 {
		maxBuckets = 1000
	}
	return &TemporalAggregator{
		maxBucketSize: maxBuckets,
	}
}

func (ta *TemporalAggregator) Aggregate(req TemporalAggRequest, vectors []VectorTimestamp) []AggregationBucket {
	if len(vectors) == 0 {
		return nil
	}

	if req.Interval <= 0 {
		req.Interval = 3600000000000
	}

	switch req.AggType {
	case TemporalAggCount:
		return ta.countBuckets(req, vectors)
	case TemporalAggMin:
		return ta.minBuckets(req, vectors)
	case TemporalAggMax:
		return ta.maxBuckets(req, vectors)
	case TemporalAggMean:
		return ta.meanBuckets(req, vectors)
	case TemporalAggSum:
		return ta.sumBuckets(req, vectors)
	default:
		return ta.countBuckets(req, vectors)
	}
}

func (ta *TemporalAggregator) countBuckets(req TemporalAggRequest, vectors []VectorTimestamp) []AggregationBucket {
	bucketMap := make(map[int64]int)

	for _, v := range vectors {
		bucketTs := (v.Timestamp.UnixNano() / req.Interval) * req.Interval
		bucketMap[bucketTs]++
	}

	buckets := make([]AggregationBucket, 0, len(bucketMap))
	for ts, count := range bucketMap {
		buckets = append(buckets, AggregationBucket{
			Timestamp: ts,
			Count:     count,
		})
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Timestamp < buckets[j].Timestamp
	})

	return ta.truncateBuckets(buckets)
}

func (ta *TemporalAggregator) minBuckets(req TemporalAggRequest, vectors []VectorTimestamp) []AggregationBucket {
	bucketMap := make(map[int64][]float32)

	for _, v := range vectors {
		if len(v.Vector) == 0 {
			continue
		}
		bucketTs := (v.Timestamp.UnixNano() / req.Interval) * req.Interval
		bucketMap[bucketTs] = append(bucketMap[bucketTs], v.Vector...)
	}

	buckets := make([]AggregationBucket, 0, len(bucketMap))
	for ts, values := range bucketMap {
		if len(values) == 0 {
			continue
		}
		minVal := values[0]
		for _, v := range values[1:] {
			if v < minVal {
				minVal = v
			}
		}
		buckets = append(buckets, AggregationBucket{
			Timestamp: ts,
			Count:     len(values),
			Min:       &minVal,
		})
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Timestamp < buckets[j].Timestamp
	})

	return ta.truncateBuckets(buckets)
}

func (ta *TemporalAggregator) maxBuckets(req TemporalAggRequest, vectors []VectorTimestamp) []AggregationBucket {
	bucketMap := make(map[int64][]float32)

	for _, v := range vectors {
		if len(v.Vector) == 0 {
			continue
		}
		bucketTs := (v.Timestamp.UnixNano() / req.Interval) * req.Interval
		bucketMap[bucketTs] = append(bucketMap[bucketTs], v.Vector...)
	}

	buckets := make([]AggregationBucket, 0, len(bucketMap))
	for ts, values := range bucketMap {
		if len(values) == 0 {
			continue
		}
		maxVal := values[0]
		for _, v := range values[1:] {
			if v > maxVal {
				maxVal = v
			}
		}
		buckets = append(buckets, AggregationBucket{
			Timestamp: ts,
			Count:     len(values),
			Max:       &maxVal,
		})
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Timestamp < buckets[j].Timestamp
	})

	return ta.truncateBuckets(buckets)
}

func (ta *TemporalAggregator) meanBuckets(req TemporalAggRequest, vectors []VectorTimestamp) []AggregationBucket {
	bucketMap := make(map[int64][]float32)

	for _, v := range vectors {
		if len(v.Vector) == 0 {
			continue
		}
		bucketTs := (v.Timestamp.UnixNano() / req.Interval) * req.Interval
		bucketMap[bucketTs] = append(bucketMap[bucketTs], v.Vector...)
	}

	buckets := make([]AggregationBucket, 0, len(bucketMap))
	for ts, values := range bucketMap {
		if len(values) == 0 {
			continue
		}
		var sum float32
		for _, v := range values {
			sum += v
		}
		meanVal := sum / float32(len(values))
		buckets = append(buckets, AggregationBucket{
			Timestamp: ts,
			Count:     len(values),
			Mean:      &meanVal,
		})
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Timestamp < buckets[j].Timestamp
	})

	return ta.truncateBuckets(buckets)
}

func (ta *TemporalAggregator) sumBuckets(req TemporalAggRequest, vectors []VectorTimestamp) []AggregationBucket {
	bucketMap := make(map[int64][]float32)

	for _, v := range vectors {
		if len(v.Vector) == 0 {
			continue
		}
		bucketTs := (v.Timestamp.UnixNano() / req.Interval) * req.Interval
		bucketMap[bucketTs] = append(bucketMap[bucketTs], v.Vector...)
	}

	buckets := make([]AggregationBucket, 0, len(bucketMap))
	for ts, values := range bucketMap {
		if len(values) == 0 {
			continue
		}
		var sum float32
		for _, v := range values {
			sum += v
		}
		buckets = append(buckets, AggregationBucket{
			Timestamp: ts,
			Count:     len(values),
			Sum:       &sum,
		})
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Timestamp < buckets[j].Timestamp
	})

	return ta.truncateBuckets(buckets)
}

func (ta *TemporalAggregator) truncateBuckets(buckets []AggregationBucket) []AggregationBucket {
	if len(buckets) <= ta.maxBucketSize {
		return buckets
	}
	return buckets[:ta.maxBucketSize]
}
