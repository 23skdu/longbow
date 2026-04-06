package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type AggregationType string

const (
	AggregationTypeMovingAverage AggregationType = "moving_average"
	AggregationTypeExponential   AggregationType = "exponential"
	AggregationTypeCumulative    AggregationType = "cumulative"
	AggregationTypeWeighted      AggregationType = "weighted"
)

type StreamAggregate struct {
	VectorID  string
	Vector    []float32
	Timestamp time.Time
	Weight    float64
	Sequence  uint64
}

type StreamingAggregation struct {
	logger      zerolog.Logger
	config      StreamingAggregationConfig
	aggregates  map[string]*VectorAggregate
	aggregateMu sync.RWMutex
	stats       StreamingAggregationStats
	wg          sync.WaitGroup
	stopChan    chan struct{}
}

type VectorAggregate struct {
	VectorID      string
	Dimension     int
	AggregateType AggregationType
	WindowSize    int
	DecayFactor   float64

	values  []StreamAggregate
	valueMu sync.Mutex

	cumulative   []float32
	cumulativeMu sync.Mutex

	ewma   []float32
	ewmaMu sync.Mutex

	lastUpdate time.Time
	sequence   uint64
}

type StreamingAggregationConfig struct {
	WindowSize      int     `json:"window_size"`
	DecayFactor     float64 `json:"decay_factor"`
	AggregationType string  `json:"aggregation_type"`
	MaxAggregates   int     `json:"max_aggregates"`
	AutoEvict       bool    `json:"auto_evict"`
	EnableWeighted  bool    `json:"enable_weighted"`
}

type StreamingAggregationStats struct {
	EventsReceived    atomic.Int64
	EventsAggregated  atomic.Int64
	AggregatesCreated atomic.Int64
	AggregatesEvicted atomic.Int64
}

func NewStreamingAggregation(logger zerolog.Logger, config StreamingAggregationConfig) *StreamingAggregation {
	if config.WindowSize <= 0 {
		config.WindowSize = 100
	}
	if config.DecayFactor <= 0 || config.DecayFactor > 1.0 {
		config.DecayFactor = 0.9
	}
	if config.MaxAggregates <= 0 {
		config.MaxAggregates = 10000
	}

	return &StreamingAggregation{
		logger:     logger,
		config:     config,
		aggregates: make(map[string]*VectorAggregate, config.MaxAggregates),
		stopChan:   make(chan struct{}),
	}
}

func (s *StreamingAggregation) CreateAggregate(vectorID string, dimension int, aggType AggregationType) error {
	s.aggregateMu.Lock()
	defer s.aggregateMu.Unlock()

	if len(s.aggregates) >= s.config.MaxAggregates {
		if s.config.AutoEvict {
			s.evictOldestLocked()
		} else {
			return fmt.Errorf("max aggregates reached: %d", s.config.MaxAggregates)
		}
	}

	agg := &VectorAggregate{
		VectorID:      vectorID,
		Dimension:     dimension,
		AggregateType: aggType,
		WindowSize:    s.config.WindowSize,
		DecayFactor:   s.config.DecayFactor,
		values:        make([]StreamAggregate, 0, s.config.WindowSize),
		cumulative:    make([]float32, dimension),
		ewma:          make([]float32, dimension),
		lastUpdate:    time.Now(),
	}

	s.aggregates[vectorID] = agg
	s.stats.AggregatesCreated.Add(1)

	s.logger.Debug().Str("vector_id", vectorID).Int("dimension", dimension).Msg("Aggregate created")
	return nil
}

func (s *StreamingAggregation) AddVector(vectorID string, vector []float32, timestamp time.Time) error {
	s.stats.EventsReceived.Add(1)

	s.aggregateMu.RLock()
	agg, ok := s.aggregates[vectorID]
	s.aggregateMu.RUnlock()

	if !ok {
		return s.CreateAggregateAndAdd(vectorID, vector, timestamp)
	}

	agg.valueMu.Lock()
	defer agg.valueMu.Unlock()

	agg.sequence++
	seq := agg.sequence

	weight := 1.0
	if s.config.EnableWeighted {
		weight = 1.0
	}

	agg.values = append(agg.values, StreamAggregate{
		VectorID:  vectorID,
		Vector:    vector,
		Timestamp: timestamp,
		Weight:    weight,
		Sequence:  seq,
	})

	if len(agg.values) > agg.WindowSize {
		agg.values = agg.values[len(agg.values)-agg.WindowSize:]
	}

	agg.lastUpdate = time.Now()

	s.updateAggregates(agg)

	s.stats.EventsAggregated.Add(1)

	return nil
}

func (s *StreamingAggregation) CreateAggregateAndAdd(vectorID string, vector []float32, timestamp time.Time) error {
	err := s.CreateAggregate(vectorID, len(vector), AggregationTypeMovingAverage)
	if err != nil {
		return err
	}

	return s.AddVector(vectorID, vector, timestamp)
}

func (s *StreamingAggregation) updateAggregates(agg *VectorAggregate) {
	switch agg.AggregateType {
	case AggregationTypeMovingAverage:
		s.updateMovingAverage(agg)
	case AggregationTypeExponential:
		s.updateExponential(agg)
	case AggregationTypeCumulative:
		s.updateCumulative(agg)
	}
}

func (s *StreamingAggregation) updateMovingAverage(agg *VectorAggregate) {
	if len(agg.values) == 0 {
		return
	}

	sum := make([]float32, agg.Dimension)
	totalWeight := 0.0

	for _, v := range agg.values {
		weight := v.Weight
		for i := 0; i < agg.Dimension; i++ {
			sum[i] += v.Vector[i] * float32(weight)
		}
		totalWeight += weight
	}

	if totalWeight > 0 {
		for i := 0; i < agg.Dimension; i++ {
			agg.cumulative[i] = sum[i] / float32(totalWeight)
		}
	}
}

func (s *StreamingAggregation) updateExponential(agg *VectorAggregate) {
	if len(agg.values) == 0 {
		return
	}

	latest := agg.values[len(agg.values)-1].Vector
	decay := float32(agg.DecayFactor)

	if len(agg.ewma) == 0 {
		copy(agg.ewma, latest)
		return
	}

	for i := 0; i < agg.Dimension; i++ {
		agg.ewma[i] = decay*agg.ewma[i] + (1-decay)*latest[i]
	}
}

func (s *StreamingAggregation) updateCumulative(agg *VectorAggregate) {
	if len(agg.values) == 0 {
		return
	}

	latest := agg.values[len(agg.values)-1].Vector

	sum := make([]float32, agg.Dimension)
	copy(sum, agg.cumulative)

	for i := 0; i < agg.Dimension; i++ {
		agg.cumulative[i] = sum[i] + latest[i]
	}
}

func (s *StreamingAggregation) GetMovingAverage(vectorID string) ([]float32, error) {
	s.aggregateMu.RLock()
	agg, ok := s.aggregates[vectorID]
	s.aggregateMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("aggregate not found: %s", vectorID)
	}

	agg.cumulativeMu.Lock()
	defer agg.cumulativeMu.Unlock()

	result := make([]float32, len(agg.cumulative))
	copy(result, agg.cumulative)
	return result, nil
}

func (s *StreamingAggregation) GetExponentialMovingAverage(vectorID string) ([]float32, error) {
	s.aggregateMu.RLock()
	agg, ok := s.aggregates[vectorID]
	s.aggregateMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("aggregate not found: %s", vectorID)
	}

	agg.ewmaMu.Lock()
	defer agg.ewmaMu.Unlock()

	result := make([]float32, len(agg.ewma))
	copy(result, agg.ewma)
	return result, nil
}

func (s *StreamingAggregation) GetCumulative(vectorID string) ([]float32, error) {
	s.aggregateMu.RLock()
	agg, ok := s.aggregates[vectorID]
	s.aggregateMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("aggregate not found: %s", vectorID)
	}

	agg.cumulativeMu.Lock()
	defer agg.cumulativeMu.Unlock()

	result := make([]float32, len(agg.cumulative))
	copy(result, agg.cumulative)
	return result, nil
}

func (s *StreamingAggregation) GetAggregate(vectorID string) (*VectorAggregate, bool) {
	s.aggregateMu.RLock()
	defer s.aggregateMu.RUnlock()

	agg, ok := s.aggregates[vectorID]
	if !ok {
		return nil, false
	}

	return &VectorAggregate{
		VectorID:      agg.VectorID,
		Dimension:     agg.Dimension,
		AggregateType: agg.AggregateType,
		WindowSize:    agg.WindowSize,
		DecayFactor:   agg.DecayFactor,
		lastUpdate:    agg.lastUpdate,
		sequence:      agg.sequence,
	}, true
}

func (s *StreamingAggregation) RemoveAggregate(vectorID string) {
	s.aggregateMu.Lock()
	defer s.aggregateMu.Unlock()

	delete(s.aggregates, vectorID)
	s.stats.AggregatesEvicted.Add(1)
}

func (s *StreamingAggregation) evictOldestLocked() {
	type aggInfo struct {
		id   string
		time time.Time
	}

	var candidates []aggInfo
	for id, agg := range s.aggregates {
		candidates = append(candidates, aggInfo{id: id, time: agg.lastUpdate})
	}

	if len(candidates) == 0 {
		return
	}

	oldest := candidates[0]
	for _, c := range candidates[1:] {
		if c.time.Before(oldest.time) {
			oldest = c
		}
	}

	delete(s.aggregates, oldest.id)
	s.stats.AggregatesEvicted.Add(1)
}

func (s *StreamingAggregation) evictOldest() {
	s.aggregateMu.Lock()
	defer s.aggregateMu.Unlock()
	s.evictOldestLocked()
}

func (s *StreamingAggregation) Clear() {
	s.aggregateMu.Lock()
	defer s.aggregateMu.Unlock()

	s.aggregates = make(map[string]*VectorAggregate, s.config.MaxAggregates)
}

func (s *StreamingAggregation) GetStats() (received, aggregated, created, evicted int64) {
	return s.stats.EventsReceived.Load(),
		s.stats.EventsAggregated.Load(),
		s.stats.AggregatesCreated.Load(),
		s.stats.AggregatesEvicted.Load()
}

func (s *StreamingAggregation) GetConfig() StreamingAggregationConfig {
	return s.config
}

func (s *StreamingAggregation) SetConfig(config StreamingAggregationConfig) {
	s.config = config
}

func (s *StreamingAggregation) ListAggregates() []string {
	s.aggregateMu.RLock()
	defer s.aggregateMu.RUnlock()

	ids := make([]string, 0, len(s.aggregates))
	for id := range s.aggregates {
		ids = append(ids, id)
	}
	return ids
}

func (s *StreamingAggregation) GetAggregateCount() int {
	s.aggregateMu.RLock()
	defer s.aggregateMu.RUnlock()
	return len(s.aggregates)
}
