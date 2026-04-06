package store

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

type CDCEventType int

const (
	CDCEventInsert CDCEventType = iota
	CDCEventUpdate
	CDCEventDelete
)

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

type CDCEvent struct {
	EventType  CDCEventType
	Dataset    string
	Batch      arrow.RecordBatch
	Sequence   uint64
	Timestamp  time.Time
	PrimaryKey []string
}

type CDCFilter struct {
	EventTypes []CDCEventType
	Columns    []string
	Since      time.Time
}

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

func (s *CDCSubscription) Pause() {
	s.mu.Lock()
	s.paused = true
	s.mu.Unlock()
}

func (s *CDCSubscription) Resume() {
	s.mu.Lock()
	s.paused = false
	s.mu.Unlock()
}

func (s *CDCSubscription) IsPaused() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.paused
}

func (s *CDCSubscription) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

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

type CDCMetrics struct {
	EventsReceived atomic.Int64
	EventsSent     atomic.Int64
	EventsDropped  atomic.Int64
	EventsFiltered atomic.Int64
	Subscriptions  atomic.Int64
	ChannelFull    atomic.Int64
}

func (m *CDCMetrics) Reset() {
	m.EventsReceived.Store(0)
	m.EventsSent.Store(0)
	m.EventsDropped.Store(0)
	m.EventsFiltered.Store(0)
	m.Subscriptions.Store(0)
	m.ChannelFull.Store(0)
}

type ChangeDataCapture struct {
	store         *VectorStore
	logger        zerolog.Logger
	subscriptions map[string]*CDCSubscription
	mu            sync.RWMutex
	config        CDCConfig
	metrics       CDCMetrics
	stopChan      chan struct{}
	wg            sync.WaitGroup
}

func NewChangeDataCapture(store *VectorStore, logger zerolog.Logger) *ChangeDataCapture {
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

	c.store.cdcMu.Lock()
	if c.store.cdcSubscribers == nil {
		c.store.cdcSubscribers = make(map[string][]chan arrow.RecordBatch)
	}
	c.store.cdcSubscribers[dataset] = append(c.store.cdcSubscribers[dataset], sub.Ch)
	c.store.cdcMu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		<-ctx.Done()
		sub.Close()
	}()

	return sub, nil
}

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

	c.store.cdcMu.Lock()
	if subs, ok := c.store.cdcSubscribers[sub.Dataset]; ok {
		for i, ch := range subs {
			if ch == sub.Ch {
				c.store.cdcSubscribers[sub.Dataset] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
	}
	c.store.cdcMu.Unlock()

	c.logger.Info().Str("id", subID).Msg("CDC subscription removed")
	return nil
}

func (c *ChangeDataCapture) GetSubscription(subID string) (*CDCSubscription, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sub, ok := c.subscriptions[subID]
	if !ok {
		return nil, fmt.Errorf("subscription %s not found", subID)
	}
	return sub, nil
}

func (c *ChangeDataCapture) ListSubscriptions() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ids := make([]string, 0, len(c.subscriptions))
	for id := range c.subscriptions {
		ids = append(ids, id)
	}
	return ids
}

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
	default:
		return nil, fmt.Errorf("unsupported column type: %T", col)
	}
}

func (c *ChangeDataCapture) SetConfig(config CDCConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = config
}

func (c *ChangeDataCapture) GetConfig() CDCConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config
}

func (c *ChangeDataCapture) GetMetrics() (received, sent, dropped, filtered, subs, full int64) {
	return c.metrics.EventsReceived.Load(),
		c.metrics.EventsSent.Load(),
		c.metrics.EventsDropped.Load(),
		c.metrics.EventsFiltered.Load(),
		c.metrics.Subscriptions.Load(),
		c.metrics.ChannelFull.Load()
}

func (c *ChangeDataCapture) IsEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config.Enabled
}

func (c *ChangeDataCapture) Enable() {
	c.mu.Lock()
	c.config.Enabled = true
	c.mu.Unlock()
	c.logger.Info().Msg("CDC enabled")
}

func (c *ChangeDataCapture) Disable() {
	c.mu.Lock()
	c.config.Enabled = false
	c.mu.Unlock()
	c.logger.Info().Msg("CDC disabled")
}

func (c *ChangeDataCapture) Stop() {
	close(c.stopChan)
	c.wg.Wait()
}
