package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/rs/zerolog"
)

// CDCEventType represents the type of Change Data Capture event.
type CDCEventType int

const (
	// CDCEventInsert indicates a new record has been inserted.
	CDCEventInsert CDCEventType = iota
	// CDCEventUpdate indicates an existing record has been updated.
	CDCEventUpdate
	// CDCEventDelete indicates a record has been deleted.
	CDCEventDelete
)

// String returns the string representation of the CDCEventType.
func (t CDCEventType) String() string {
	switch t {
	case CDCEventInsert:
		return "INSERT"
	case CDCEventUpdate:
		return "UPDATE"
	case CDCEventDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// CDCEvent represents a single Change Data Capture event.
type CDCEvent struct {
	EventType  CDCEventType
	Dataset    string
	Batch      arrow.RecordBatch
	Sequence   uint64
	Timestamp  time.Time
	PrimaryKey []string
}

// CDCFilter defines criteria for filtering CDC events.
type CDCFilter struct {
	EventTypes []CDCEventType
	Columns    []string
	Since      time.Time
}

// CDCSubscription represents a subscription to CDC events for a specific dataset.
type CDCSubscription struct {
	ID         string
	Dataset    string
	Filter     CDCFilter
	Ch         chan arrow.RecordBatch
	Cancel     context.CancelFunc
	mu         sync.Mutex
	paused     bool
	bufferSize int
	closed     bool
}

// Pause temporarily stops event delivery for the subscription.
func (s *CDCSubscription) Pause() {
	s.mu.Lock()
	s.paused = true
	s.mu.Unlock()
}

// Resume restarts event delivery for a paused subscription.
func (s *CDCSubscription) Resume() {
	s.mu.Lock()
	s.paused = false
	s.mu.Unlock()
}

// IsPaused returns true if the subscription is currently paused.
func (s *CDCSubscription) IsPaused() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.paused
}

// IsClosed returns true if the subscription has been closed.
func (s *CDCSubscription) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close terminates the subscription and closes the event channel.
func (s *CDCSubscription) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.Ch)
	s.mu.Unlock()
}

// CDCConfig holds configuration settings for the Change Data Capture system.
type CDCConfig struct {
	Enabled                bool `json:"enabled"`
	BufferSize             int  `json:"buffer_size"`
	EnableJSON             bool `json:"enable_json"`
	EnableArrow            bool `json:"enable_arrow"`
	FilterDuplicates       bool `json:"filter_duplicates"`
	AsyncDispatch          bool `json:"async_dispatch"`
	DropOnFull             bool `json:"drop_on_full"`
	WorkerPoolSize         int  `json:"worker_pool_size"`
	BatchAggregationMs     int  `json:"batch_aggregation_ms"`
	ColumnFilterEnabled    bool `json:"column_filter_enabled"`
	EventTypeFilterEnabled bool `json:"event_type_filter_enabled"`
}

// CDCMetrics tracks performance and operational metrics for CDC.
type CDCMetrics struct {
	EventsReceived atomic.Int64
	EventsSent     atomic.Int64
	EventsDropped  atomic.Int64
	EventsFiltered atomic.Int64
	Subscriptions  atomic.Int64
	ChannelFull    atomic.Int64
}

// Reset clears all recorded CDC metrics.
func (m *CDCMetrics) Reset() {
	m.EventsReceived.Store(0)
	m.EventsSent.Store(0)
	m.EventsDropped.Store(0)
	m.EventsFiltered.Store(0)
	m.Subscriptions.Store(0)
	m.ChannelFull.Store(0)
}

// ChangeDataCapture manages real-time data change notifications.
type ChangeDataCapture struct {
	store         CDCStore
	logger        zerolog.Logger
	subscriptions map[string]*CDCSubscription
	mu            sync.RWMutex
	config        CDCConfig
	metrics       CDCMetrics
	stopChan      chan struct{}
	wg            sync.WaitGroup

	batchAggregator *CDCBatchAggregator
}

// NewChangeDataCapture creates a new CDC manager for the given store.
func NewChangeDataCapture(store CDCStore, logger zerolog.Logger) *ChangeDataCapture {
	cdc := &ChangeDataCapture{
		store:         store,
		logger:        logger,
		subscriptions: make(map[string]*CDCSubscription),
		stopChan:      make(chan struct{}),
		config: CDCConfig{
			Enabled:                true,
			BufferSize:             1024,
			EnableJSON:             true,
			EnableArrow:            true,
			FilterDuplicates:       false,
			AsyncDispatch:          true,
			DropOnFull:             true,
			WorkerPoolSize:         4,
			BatchAggregationMs:     100,
			ColumnFilterEnabled:    true,
			EventTypeFilterEnabled: true,
		},
	}
	cdc.metrics.Reset()

	return cdc
}

// Subscribe creates a new CDC subscription for the specified dataset.
func (c *ChangeDataCapture) Subscribe(dataset string, filter CDCFilter, bufferSize int) (*CDCSubscription, error) {
	if dataset == "" {
		return nil, fmt.Errorf("dataset name required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.config.Enabled {
		return nil, fmt.Errorf("CDC is disabled")
	}

	ctx, cancel := context.WithCancel(context.Background())
	sub := &CDCSubscription{
		ID:         fmt.Sprintf("cdc-%d", time.Now().UnixNano()),
		Dataset:    dataset,
		Filter:     filter,
		Ch:         make(chan arrow.RecordBatch, bufferSize),
		Cancel:     cancel,
		bufferSize: bufferSize,
	}

	c.subscriptions[sub.ID] = sub
	c.metrics.Subscriptions.Add(1)

	c.logger.Info().Str("id", sub.ID).Str("dataset", dataset).Int("buffer_size", bufferSize).Msg("CDC subscription created")

	c.store.RegisterCDCSubscriber(dataset, sub.Ch)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		<-ctx.Done()
		sub.Close()
	}()

	return sub, nil
}

// Unsubscribe cancels and removes an existing CDC subscription.
func (c *ChangeDataCapture) Unsubscribe(subID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	sub, ok := c.subscriptions[subID]
	if !ok {
		return fmt.Errorf("subscription %s not found", subID)
	}

	sub.Cancel()
	delete(c.subscriptions, subID)
	c.metrics.Subscriptions.Add(-1)

	c.store.UnregisterCDCSubscriber(sub.Dataset, sub.Ch)

	c.logger.Info().Str("id", subID).Msg("CDC subscription removed")
	return nil
}

// GetSubscription retrieves a subscription by its ID.
func (c *ChangeDataCapture) GetSubscription(subID string) (*CDCSubscription, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sub, ok := c.subscriptions[subID]
	if !ok {
		return nil, fmt.Errorf("subscription %s not found", subID)
	}
	return sub, nil
}

// ListSubscriptions returns a list of all active subscription IDs.
func (c *ChangeDataCapture) ListSubscriptions() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ids := make([]string, 0, len(c.subscriptions))
	for id := range c.subscriptions {
		ids = append(ids, id)
	}
	return ids
}

// GetSubscriptionByDataset returns all active subscriptions for a given dataset.
func (c *ChangeDataCapture) GetSubscriptionByDataset(dataset string) []*CDCSubscription {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var subs []*CDCSubscription
	for _, sub := range c.subscriptions {
		if sub.Dataset == dataset && !sub.IsClosed() {
			subs = append(subs, sub)
		}
	}
	return subs
}

// HandleCDCBatch processes a batch of record batches and dispatches to subscribers.
func (c *ChangeDataCapture) HandleCDCBatch(dataset string, batches []arrow.RecordBatch) {
	if !c.config.Enabled {
		return
	}

	c.mu.RLock()
	var subs []*CDCSubscription
	for _, sub := range c.subscriptions {
		if sub.Dataset == dataset && !sub.IsClosed() {
			subs = append(subs, sub)
		}
	}
	c.mu.RUnlock()

	if len(subs) == 0 {
		return
	}

	totalEvents := 0
	for _, batch := range batches {
		totalEvents += int(batch.NumRows())
	}
	c.metrics.EventsReceived.Add(int64(totalEvents))

	for _, batch := range batches {
		schema := batch.Schema()

		for _, sub := range subs {
			if sub.IsPaused() || sub.IsClosed() {
				continue
			}

			if c.config.ColumnFilterEnabled && len(sub.Filter.Columns) > 0 {
				if !c.matchesColumnsFilter(sub.Filter.Columns, schema) {
					c.metrics.EventsFiltered.Add(1)
					continue
				}
			}

			if c.config.EventTypeFilterEnabled && len(sub.Filter.EventTypes) > 0 {
				if !c.matchesEventTypeFilter(sub.Filter.EventTypes) {
					c.metrics.EventsFiltered.Add(1)
					continue
				}
			}

			batch.Retain()

			if c.config.AsyncDispatch {
				select {
				case sub.Ch <- batch:
					c.metrics.EventsSent.Add(int64(batch.NumRows()))
				default:
					if c.config.DropOnFull {
						c.metrics.EventsDropped.Add(int64(batch.NumRows()))
						batch.Release()
					} else {
						select {
						case sub.Ch <- batch:
							c.metrics.EventsSent.Add(int64(batch.NumRows()))
						default:
							c.metrics.ChannelFull.Add(1)
							c.metrics.EventsDropped.Add(int64(batch.NumRows()))
							batch.Release()
						}
					}
				}
			} else {
				select {
				case sub.Ch <- batch:
					c.metrics.EventsSent.Add(int64(batch.NumRows()))
				default:
					c.metrics.ChannelFull.Add(1)
					c.metrics.EventsDropped.Add(int64(batch.NumRows()))
					batch.Release()
				}
			}
		}
	}
}

func (c *ChangeDataCapture) matchesColumnsFilter(filterCols []string, schema *arrow.Schema) bool {
	for _, col := range filterCols {
		found := false
		for i := 0; i < int(schema.NumFields()); i++ {
			if schema.Field(i).Name == col {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (c *ChangeDataCapture) matchesEventTypeFilter(filterTypes []CDCEventType) bool {
	for _, et := range filterTypes {
		switch et {
		case CDCEventInsert, CDCEventUpdate, CDCEventDelete:
			return true
		}
	}
	return false
}

func (c *ChangeDataCapture) matchesFilter(filter CDCFilter, batch arrow.RecordBatch) bool {
	if c.config.EventTypeFilterEnabled && len(filter.EventTypes) > 0 {
		found := false
		for _, et := range filter.EventTypes {
			if et == CDCEventInsert {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if c.config.ColumnFilterEnabled && len(filter.Columns) > 0 {
		schema := batch.Schema()
		for _, col := range filter.Columns {
			found := false
			for i := 0; i < int(schema.NumFields()); i++ {
				if schema.Field(i).Name == col {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}

	return true
}

func (c *ChangeDataCapture) extractPrimaryKeys(batch arrow.RecordBatch) []string {
	schema := batch.Schema()
	pkCols := make([]string, 0)

	for i := 0; i < int(schema.NumFields()); i++ {
		field := schema.Field(i)
		if field.Name == "_id" || field.Name == "id" || field.Name == "pk" {
			pkCols = append(pkCols, field.Name)
		}
	}

	return pkCols
}

// EventToJSON serializes a CDC event to JSON format.
func (c *ChangeDataCapture) EventToJSON(event CDCEvent) ([]byte, error) {
	if !c.config.EnableJSON {
		return nil, fmt.Errorf("JSON serialization disabled")
	}

	schema := event.Batch.Schema()
	record := make([]map[string]interface{}, event.Batch.NumRows())

	for rowIdx := 0; rowIdx < int(event.Batch.NumRows()); rowIdx++ {
		record[rowIdx] = make(map[string]interface{})
		for colIdx := 0; colIdx < int(schema.NumFields()); colIdx++ {
			col := event.Batch.Column(colIdx)
			val, err := GetValueAt(col, rowIdx)
			if err != nil {
				continue
			}
			record[rowIdx][schema.Field(colIdx).Name] = val
		}
	}

	type EventJSON struct {
		EventType  string                   `json:"event_type"`
		Dataset    string                   `json:"dataset"`
		Sequence   uint64                   `json:"sequence"`
		Timestamp  time.Time                `json:"timestamp"`
		PrimaryKey []string                 `json:"primary_key,omitempty"`
		Data       []map[string]interface{} `json:"data"`
	}

	jsonEvent := EventJSON{
		EventType:  event.EventType.String(),
		Dataset:    event.Dataset,
		Sequence:   event.Sequence,
		Timestamp:  event.Timestamp,
		PrimaryKey: event.PrimaryKey,
		Data:       record,
	}

	return json.Marshal(jsonEvent)
}

// GetValueAt retrieves the value at the specified row index in an arrow array.
func GetValueAt(col arrow.Array, row int) (interface{}, error) {
	switch col := col.(type) {
	case *array.Int8:
		return col.Value(row), nil
	case *array.Int16:
		return col.Value(row), nil
	case *array.Int32:
		return col.Value(row), nil
	case *array.Int64:
		return col.Value(row), nil
	case *array.Uint8:
		return col.Value(row), nil
	case *array.Uint16:
		return col.Value(row), nil
	case *array.Uint32:
		return col.Value(row), nil
	case *array.Uint64:
		return col.Value(row), nil
	case *array.Float32:
		return col.Value(row), nil
	case *array.Float64:
		return col.Value(row), nil
	case *array.String:
		return col.Value(row), nil
	case *array.LargeString:
		return col.Value(row), nil
	case *array.Binary:
		return col.Value(row), nil
	case *array.LargeBinary:
		return col.Value(row), nil
	case *array.Boolean:
		return col.Value(row), nil
	case *array.Timestamp:
		return col.Value(row), nil
	case *array.Date32:
		return col.Value(row), nil
	case *array.Date64:
		return col.Value(row), nil
	case *array.Duration:
		return col.Value(row), nil
	case *array.Null:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported column type: %T", col)
	}
}

// SetConfig updates the CDC configuration.
func (c *ChangeDataCapture) SetConfig(config CDCConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = config
}

// GetConfig returns the current CDC configuration.
func (c *ChangeDataCapture) GetConfig() CDCConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config
}

// GetMetrics returns the current operational metrics for CDC.
func (c *ChangeDataCapture) GetMetrics() (received, sent, dropped, filtered, subs, full int64) {
	return c.metrics.EventsReceived.Load(),
		c.metrics.EventsSent.Load(),
		c.metrics.EventsDropped.Load(),
		c.metrics.EventsFiltered.Load(),
		c.metrics.Subscriptions.Load(),
		c.metrics.ChannelFull.Load()
}

// IsEnabled returns true if CDC is currently enabled.
func (c *ChangeDataCapture) IsEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config.Enabled
}

// Enable activates the CDC system.
func (c *ChangeDataCapture) Enable() {
	c.mu.Lock()
	c.config.Enabled = true
	c.mu.Unlock()
	c.logger.Info().Msg("CDC enabled")
}

// Disable deactivates the CDC system.
func (c *ChangeDataCapture) Disable() {
	c.mu.Lock()
	c.config.Enabled = false
	c.mu.Unlock()
	c.logger.Info().Msg("CDC disabled")
}

// Stop stops the CDC manager and waits for background workers to finish.
func (c *ChangeDataCapture) Stop() {
	close(c.stopChan)
	c.wg.Wait()
}

// CDCBatchAggregator aggregates small CDC events into larger batches.
type CDCBatchAggregator struct {
	mu          sync.Mutex
	batches     [][]arrow.RecordBatch
	pendingSize int
	maxSize     int
	interval    time.Duration
	timer       *time.Timer
	onFlush     func(dataset string, batches [][]arrow.RecordBatch)
	dataset     string
	closed      bool
}

// NewCDCBatchAggregator creates a new CDC batch aggregator.
func NewCDCBatchAggregator(dataset string, maxSize int, interval time.Duration, onFlush func(string, [][]arrow.RecordBatch)) *CDCBatchAggregator {
	return &CDCBatchAggregator{
		dataset:  dataset,
		maxSize:  maxSize,
		interval: interval,
		onFlush:  onFlush,
		batches:  make([][]arrow.RecordBatch, 0),
	}
}

// AddBatch adds a record batch to the aggregator.
func (ba *CDCBatchAggregator) AddBatch(batch arrow.RecordBatch) {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	if ba.closed {
		return
	}

	ba.batches = append(ba.batches, []arrow.RecordBatch{batch})
	ba.pendingSize += int(batch.NumRows())

	if ba.pendingSize >= ba.maxSize {
		ba.flushLocked()
	} else if ba.timer == nil {
		ba.timer = time.AfterFunc(ba.interval, func() {
			ba.mu.Lock()
			ba.flushLocked()
			ba.mu.Unlock()
		})
	}
}

func (ba *CDCBatchAggregator) flushLocked() {
	if len(ba.batches) == 0 {
		return
	}

	if ba.timer != nil {
		ba.timer.Stop()
		ba.timer = nil
	}

	ba.onFlush(ba.dataset, ba.batches)
	ba.batches = make([][]arrow.RecordBatch, 0)
	ba.pendingSize = 0
}

// Flush manually triggers a flush of aggregated batches.
func (ba *CDCBatchAggregator) Flush() {
	ba.mu.Lock()
	defer ba.mu.Unlock()
	ba.flushLocked()
}

// Close stops the aggregator and flushes any pending batches.
func (ba *CDCBatchAggregator) Close() {
	ba.mu.Lock()
	defer ba.mu.Unlock()
	ba.closed = true
	if ba.timer != nil {
		ba.timer.Stop()
	}
	ba.flushLocked()
}
