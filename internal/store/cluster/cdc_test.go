package cluster

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow"

	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCDCEventType_String runs the test cdc event type_string operation.
func TestCDCEventType_String(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	tests := []struct {
		eventType CDCEventType
		expected  string
	}{
		{CDCEventInsert, "INSERT"},
		{CDCEventUpdate, "UPDATE"},
		{CDCEventDelete, "DELETE"},
		{CDCEventType(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.eventType.String())
	}
}

// TestCDCFilter runs the test cdc filter operation.
func TestCDCFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	filter := CDCFilter{
		EventTypes: []CDCEventType{CDCEventInsert, CDCEventUpdate},
		Columns:    []string{"vector", "value"},
	}

	assert.Len(t, filter.EventTypes, 2)
	assert.Contains(t, filter.Columns, "vector")
}

// TestCDCSubscription_PauseResume runs the test cdc subscription_pause resume operation.
func TestCDCSubscription_PauseResume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	sub := &CDCSubscription{
		ID:     "test-sub",
		Ch:     make(chan arrow.RecordBatch, 10),
		paused: false,
	}

	assert.False(t, sub.IsPaused())
	sub.Pause()
	assert.True(t, sub.IsPaused())
	sub.Resume()
	assert.False(t, sub.IsPaused())
}

// TestCDCSubscription_Close runs the test cdc subscription_close operation.
func TestCDCSubscription_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	sub := &CDCSubscription{
		ID:     "test-sub",
		Ch:     make(chan arrow.RecordBatch, 10),
		closed: false,
	}

	assert.False(t, sub.IsClosed())
	sub.Close()
	assert.True(t, sub.IsClosed())

	select {
	case <-sub.Ch:
	default:
		t.Error("channel should be closed")
	}
}

// TestCDCSubscription_Close_Idempotent runs the test cdc subscription_close_idempotent operation.
func TestCDCSubscription_Close_Idempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	sub := &CDCSubscription{
		ID:     "test-sub",
		Ch:     make(chan arrow.RecordBatch, 10),
		closed: false,
	}

	sub.Close()
	assert.True(t, sub.IsClosed())

	sub.Close()
	assert.True(t, sub.IsClosed())
}

// TestCDCConfig_Defaults runs the test cdc config_defaults operation.
func TestCDCConfig_Defaults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()

	cdc := NewChangeDataCapture(store, logger)

	assert.True(t, cdc.config.Enabled)
	assert.Equal(t, 1024, cdc.config.BufferSize)
	assert.True(t, cdc.config.EnableJSON)
	assert.True(t, cdc.config.EnableArrow)
	assert.False(t, cdc.config.FilterDuplicates)
	assert.True(t, cdc.config.AsyncDispatch)
	assert.True(t, cdc.config.DropOnFull)
	assert.Equal(t, 4, cdc.config.WorkerPoolSize)
	assert.Equal(t, 100, cdc.config.BatchAggregationMs)
	assert.True(t, cdc.config.ColumnFilterEnabled)
	assert.True(t, cdc.config.EventTypeFilterEnabled)
}

// TestNewChangeDataCapture runs the test new change data capture operation.
func TestNewChangeDataCapture(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()

	cdc := NewChangeDataCapture(store, logger)

	assert.NotNil(t, cdc)
	assert.NotNil(t, cdc.subscriptions)
	assert.NotNil(t, cdc.stopChan)
	assert.Equal(t, 1024, cdc.config.BufferSize)
	assert.True(t, cdc.config.EnableJSON)
	assert.True(t, cdc.config.EnableArrow)
}

// TestChangeDataCapture_Subscribe runs the test change data capture_subscribe operation.
func TestChangeDataCapture_Subscribe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	filter := CDCFilter{
		EventTypes: []CDCEventType{CDCEventInsert},
		Columns:    []string{"vector"},
	}

	sub, err := cdc.Subscribe("test-dataset", filter, 100)
	require.NoError(t, err)
	assert.NotNil(t, sub)
	assert.Equal(t, "test-dataset", sub.Dataset)

	store.mu.RLock()
	subs, ok := store.cdcSubscribers["test-dataset"]
	store.mu.RUnlock()
	assert.True(t, ok)
	assert.Len(t, subs, 1)
	assert.Equal(t, sub.Ch, subs[0])
}

// TestChangeDataCapture_Subscribe_EmptyDataset runs the test change data capture_subscribe_empty dataset operation.
func TestChangeDataCapture_Subscribe_EmptyDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	_, err := cdc.Subscribe("", CDCFilter{}, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dataset name required")
}

// TestChangeDataCapture_Subscribe_Disabled runs the test change data capture_subscribe_disabled operation.
func TestChangeDataCapture_Subscribe_Disabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)
	cdc.config.Enabled = false

	_, err := cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CDC is disabled")
}

// TestChangeDataCapture_Unsubscribe runs the test change data capture_unsubscribe operation.
func TestChangeDataCapture_Unsubscribe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	sub, err := cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	require.NoError(t, err)

	err = cdc.Unsubscribe(sub.ID)
	require.NoError(t, err)

	subs := cdc.ListSubscriptions()
	assert.Len(t, subs, 0)
}

// TestChangeDataCapture_Unsubscribe_NotFound runs the test change data capture_unsubscribe_not found operation.
func TestChangeDataCapture_Unsubscribe_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	err := cdc.Unsubscribe("non-existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestChangeDataCapture_GetSubscription runs the test change data capture_get subscription operation.
func TestChangeDataCapture_GetSubscription(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	sub, err := cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	require.NoError(t, err)

	got, err := cdc.GetSubscription(sub.ID)
	require.NoError(t, err)
	assert.Equal(t, sub.ID, got.ID)

	_, err = cdc.GetSubscription("non-existent")
	assert.Error(t, err)
}

// TestChangeDataCapture_GetSubscriptionByDataset runs the test change data capture_get subscription by dataset operation.
func TestChangeDataCapture_GetSubscriptionByDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	_, err := cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	require.NoError(t, err)
	_, err = cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	require.NoError(t, err)
	_, err = cdc.Subscribe("other-dataset", CDCFilter{}, 100)
	require.NoError(t, err)

	subs := cdc.GetSubscriptionByDataset("test-dataset")
	assert.Len(t, subs, 2)
}

// TestChangeDataCapture_ListSubscriptions runs the test change data capture_list subscriptions operation.
func TestChangeDataCapture_ListSubscriptions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	_, err := cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	require.NoError(t, err)
	_, err = cdc.Subscribe("other-dataset", CDCFilter{}, 100)
	require.NoError(t, err)

	subs := cdc.ListSubscriptions()
	assert.Len(t, subs, 2)
}

// TestChangeDataCapture_HandleCDCBatch_Disabled runs the test change data capture_handle cdc batch_disabled operation.
func TestChangeDataCapture_HandleCDCBatch_Disabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)
	cdc.config.Enabled = false

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	cdc.HandleCDCBatch("test-dataset", []arrow.RecordBatch{record})
}

// TestChangeDataCapture_HandleCDCBatch_NoSubscribers runs the test change data capture_handle cdc batch_no subscribers operation.
func TestChangeDataCapture_HandleCDCBatch_NoSubscribers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	cdc.HandleCDCBatch("test-dataset", []arrow.RecordBatch{record})
}

// TestChangeDataCapture_HandleCDCBatch runs the test change data capture_handle cdc batch operation.
func TestChangeDataCapture_HandleCDCBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	sub, err := cdc.Subscribe("test-dataset", CDCFilter{}, 10)
	require.NoError(t, err)

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.Float64Builder).AppendValues([]float64{1.0, 2.0, 3.0}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	cdc.HandleCDCBatch("test-dataset", []arrow.RecordBatch{record})

	select {
	case received := <-sub.Ch:
		assert.Equal(t, record.NumRows(), received.NumRows())
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for CDC event")
	}

	received, _, _, _, _, _ := cdc.GetMetrics()
	assert.Equal(t, int64(3), received)
}

// TestChangeDataCapture_HandleCDCBatch_PausedSubscription runs the test change data capture_handle cdc batch_paused subscription operation.
func TestChangeDataCapture_HandleCDCBatch_PausedSubscription(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	sub, err := cdc.Subscribe("test-dataset", CDCFilter{}, 10)
	require.NoError(t, err)

	sub.Pause()

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	cdc.HandleCDCBatch("test-dataset", []arrow.RecordBatch{record})

	select {
	case <-sub.Ch:
		t.Error("should not receive event when paused")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestChangeDataCapture_HandleCDCBatch_ColumnFilter runs the test change data capture_handle cdc batch_column filter operation.
func TestChangeDataCapture_HandleCDCBatch_ColumnFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	filter := CDCFilter{Columns: []string{"value"}}
	sub, err := cdc.Subscribe("test-dataset", filter, 10)
	require.NoError(t, err)

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	builder.Field(1).(*array.Float64Builder).AppendValues([]float64{1.0}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	cdc.HandleCDCBatch("test-dataset", []arrow.RecordBatch{record})

	select {
	case <-sub.Ch:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for CDC event")
	}
}

// TestChangeDataCapture_HandleCDCBatch_ColumnFilter_Disabled runs the test change data capture_handle cdc batch_column filter_disabled operation.
func TestChangeDataCapture_HandleCDCBatch_ColumnFilter_Disabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)
	cdc.config.ColumnFilterEnabled = false

	filter := CDCFilter{Columns: []string{"nonexistent"}}
	sub, err := cdc.Subscribe("test-dataset", filter, 10)
	require.NoError(t, err)

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	builder.Field(1).(*array.Float64Builder).AppendValues([]float64{1.0}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	cdc.HandleCDCBatch("test-dataset", []arrow.RecordBatch{record})

	select {
	case <-sub.Ch:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for CDC event")
	}
}

// TestChangeDataCapture_matchesFilter runs the test change data capture_matches filter operation.
func TestChangeDataCapture_matchesFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	builder.Field(1).(*array.Float64Builder).AppendValues([]float64{1.0}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	t.Run("no filter", func(t *testing.T) {
		filter := CDCFilter{}
		assert.True(t, cdc.matchesFilter(filter, record))
	})

	t.Run("event type filter - match", func(t *testing.T) {
		filter := CDCFilter{EventTypes: []CDCEventType{CDCEventInsert}}
		assert.True(t, cdc.matchesFilter(filter, record))
	})

	t.Run("event type filter - no match", func(t *testing.T) {
		filter := CDCFilter{EventTypes: []CDCEventType{CDCEventDelete}}
		assert.False(t, cdc.matchesFilter(filter, record))
	})

	t.Run("column filter - match", func(t *testing.T) {
		filter := CDCFilter{Columns: []string{"value"}}
		assert.True(t, cdc.matchesFilter(filter, record))
	})

	t.Run("column filter - no match", func(t *testing.T) {
		filter := CDCFilter{Columns: []string{"nonexistent"}}
		assert.False(t, cdc.matchesFilter(filter, record))
	})

	t.Run("combined filter", func(t *testing.T) {
		filter := CDCFilter{
			EventTypes: []CDCEventType{CDCEventInsert},
			Columns:    []string{"value"},
		}
		assert.True(t, cdc.matchesFilter(filter, record))
	})
}

// TestChangeDataCapture_matchesColumnsFilter runs the test change data capture_matches columns filter operation.
func TestChangeDataCapture_matchesColumnsFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	assert.True(t, cdc.matchesColumnsFilter([]string{"id", "value"}, schema))
	assert.True(t, cdc.matchesColumnsFilter([]string{"id"}, schema))
	assert.False(t, cdc.matchesColumnsFilter([]string{"nonexistent"}, schema))
	assert.False(t, cdc.matchesColumnsFilter([]string{"id", "nonexistent"}, schema))
}

// TestChangeDataCapture_matchesEventTypeFilter runs the test change data capture_matches event type filter operation.
func TestChangeDataCapture_matchesEventTypeFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	assert.True(t, cdc.matchesEventTypeFilter([]CDCEventType{CDCEventInsert}))
	assert.True(t, cdc.matchesEventTypeFilter([]CDCEventType{CDCEventUpdate}))
	assert.True(t, cdc.matchesEventTypeFilter([]CDCEventType{CDCEventDelete}))
	assert.False(t, cdc.matchesEventTypeFilter([]CDCEventType{}))
}

// TestChangeDataCapture_extractPrimaryKeys runs the test change data capture_extract primary keys operation.
func TestChangeDataCapture_extractPrimaryKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	builder.Field(1).(*array.Float64Builder).AppendValues([]float64{1.0}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	pkCols := cdc.extractPrimaryKeys(record)
	assert.Contains(t, pkCols, "_id")
}

// TestChangeDataCapture_EventToJSON runs the test change data capture_event to json operation.
func TestChangeDataCapture_EventToJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	builder.Field(1).(*array.Float64Builder).AppendValues([]float64{1.0, 2.0}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	event := CDCEvent{
		EventType:  CDCEventInsert,
		Dataset:    "test-dataset",
		Batch:      record,
		Sequence:   1,
		Timestamp:  time.Now(),
		PrimaryKey: []string{"id"},
	}

	jsonBytes, err := cdc.EventToJSON(event)
	require.NoError(t, err)
	assert.NotEmpty(t, jsonBytes)

	var result map[string]interface{}
	err = json.Unmarshal(jsonBytes, &result)
	require.NoError(t, err)
	assert.Equal(t, "INSERT", result["event_type"])
	assert.Equal(t, "test-dataset", result["dataset"])
}

// TestChangeDataCapture_EventToJSON_Disabled runs the test change data capture_event to json_disabled operation.
func TestChangeDataCapture_EventToJSON_Disabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)
	cdc.config.EnableJSON = false

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	event := CDCEvent{
		EventType: CDCEventInsert,
		Dataset:   "test-dataset",
		Batch:     record,
	}

	_, err := cdc.EventToJSON(event)
	assert.Error(t, err)
}

// TestChangeDataCapture_SetConfig runs the test change data capture_set config operation.
func TestChangeDataCapture_SetConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	newConfig := CDCConfig{
		Enabled:                false,
		BufferSize:             2048,
		EnableJSON:             false,
		EnableArrow:            true,
		FilterDuplicates:       true,
		AsyncDispatch:          false,
		DropOnFull:             false,
		WorkerPoolSize:         8,
		BatchAggregationMs:     50,
		ColumnFilterEnabled:    false,
		EventTypeFilterEnabled: false,
	}

	cdc.SetConfig(newConfig)
	assert.Equal(t, newConfig, cdc.GetConfig())
}

// TestChangeDataCapture_GetMetrics runs the test change data capture_get metrics operation.
func TestChangeDataCapture_GetMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	received, sent, dropped, filtered, subs, full := cdc.GetMetrics()
	assert.Equal(t, int64(0), received)
	assert.Equal(t, int64(0), sent)
	assert.Equal(t, int64(0), dropped)
	assert.Equal(t, int64(0), filtered)
	assert.Equal(t, int64(0), subs)
	assert.Equal(t, int64(0), full)
}

// TestChangeDataCapture_IsEnabled runs the test change data capture_is enabled operation.
func TestChangeDataCapture_IsEnabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	assert.True(t, cdc.IsEnabled())

	cdc.Disable()
	assert.False(t, cdc.IsEnabled())

	cdc.Enable()
	assert.True(t, cdc.IsEnabled())
}

// TestChangeDataCapture_EnableDisable runs the test change data capture_enable disable operation.
func TestChangeDataCapture_EnableDisable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	assert.True(t, cdc.IsEnabled())
	cdc.Disable()
	assert.False(t, cdc.IsEnabled())
	cdc.Enable()
	assert.True(t, cdc.IsEnabled())
}

// TestGetValueAt runs the test get value at operation.
func TestGetValueAt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	mem := memory.NewGoAllocator()

	t.Run("Int64", func(t *testing.T) {
		builder := array.NewInt64Builder(mem)
		builder.AppendValues([]int64{42}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		val, err := GetValueAt(arr, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(42), val)
	})

	t.Run("Float32", func(t *testing.T) {
		builder := array.NewFloat32Builder(mem)
		builder.AppendValues([]float32{3.14}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		val, err := GetValueAt(arr, 0)
		require.NoError(t, err)
		assert.Equal(t, float32(3.14), val)
	})

	t.Run("Float64", func(t *testing.T) {
		builder := array.NewFloat64Builder(mem)
		builder.AppendValues([]float64{2.718}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		val, err := GetValueAt(arr, 0)
		require.NoError(t, err)
		assert.Equal(t, float64(2.718), val)
	})

	t.Run("unsupported type", func(t *testing.T) {
		builder := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 4})
		builder.Append([]byte("test"))
		arr := builder.NewArray()
		defer arr.Release()

		_, err := GetValueAt(arr, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported")
	})
}

// TestChangeDataCapture_HandleCDCBatch_DropOnFull runs the test change data capture_handle cdc batch_drop on full operation.
func TestChangeDataCapture_HandleCDCBatch_DropOnFull(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)
	cdc.config.DropOnFull = true
	cdc.config.AsyncDispatch = false

	sub, err := cdc.Subscribe("test-dataset", CDCFilter{}, 1)
	require.NoError(t, err)

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	sub.Ch <- record

	cdc.HandleCDCBatch("test-dataset", []arrow.RecordBatch{record})

	received, sent, dropped, filtered, subs, full := cdc.GetMetrics()
	assert.Equal(t, int64(1), received)
	assert.Equal(t, int64(0), sent)
	assert.Equal(t, int64(1), dropped)
	assert.Equal(t, int64(0), filtered)
	assert.Equal(t, int64(1), subs)
	assert.Equal(t, int64(1), full)
}

// TestChangeDataCapture_HandleCDCBatch_AsyncDispatch runs the test change data capture_handle cdc batch_async dispatch operation.
func TestChangeDataCapture_HandleCDCBatch_AsyncDispatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)
	cdc.config.AsyncDispatch = true
	cdc.config.DropOnFull = true

	sub, err := cdc.Subscribe("test-dataset", CDCFilter{}, 1)
	require.NoError(t, err)

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	sub.Ch <- record

	cdc.HandleCDCBatch("test-dataset", []arrow.RecordBatch{record})

	received, _, _, _, _, _ := cdc.GetMetrics()
	assert.Equal(t, int64(1), received)
}

// TestChangeDataCapture_MultipleBatches runs the test change data capture_multiple batches operation.
func TestChangeDataCapture_MultipleBatches(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	_, err := cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	require.NoError(t, err)

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	batches := make([]arrow.RecordBatch, 3)
	for i := 0; i < 3; i++ {
		builder := array.NewRecordBuilder(mem, schema)
		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{int64(i)}, nil)
		batches[i] = builder.NewRecordBatch()
	}
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	cdc.HandleCDCBatch("test-dataset", batches)

	received, _, _, _, _, _ := cdc.GetMetrics()
	assert.Equal(t, int64(3), received)
}

// TestChangeDataCapture_CloseSubscription_UpdatesMetrics runs the test change data capture_close subscription_updates metrics operation.
func TestChangeDataCapture_CloseSubscription_UpdatesMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	sub, err := cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	require.NoError(t, err)

	_, _, _, _, initialSubs, _ := cdc.GetMetrics()
	assert.Equal(t, int64(1), initialSubs)

	_ = cdc.Unsubscribe(sub.ID)

	_, _, _, _, afterSubs, _ := cdc.GetMetrics()
	assert.Equal(t, int64(0), afterSubs)
}

// TestCDCMetrics_Reset runs the test cdc metrics_reset operation.
func TestCDCMetrics_Reset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	store := &MockCDCStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	cdc.metrics.EventsReceived.Add(10)
	cdc.metrics.EventsSent.Add(5)

	cdc.metrics.Reset()

	received, sent, _, _, _, _ := cdc.GetMetrics()
	assert.Equal(t, int64(0), received)
	assert.Equal(t, int64(0), sent)
}

// MockCDCStore represents a mock cdc store value.
type MockCDCStore struct {
	mu sync.RWMutex

	cdcSubscribers map[string][]chan arrow.RecordBatch
}

// RegisterCDCSubscriber runs the register cdc subscriber operation.
func (m *MockCDCStore) RegisterCDCSubscriber(dataset string, ch chan arrow.RecordBatch) {
	if m.cdcSubscribers == nil {
		m.cdcSubscribers = make(map[string][]chan arrow.RecordBatch)
	}
	m.cdcSubscribers[dataset] = append(m.cdcSubscribers[dataset], ch)
}

// UnregisterCDCSubscriber runs the unregister cdc subscriber operation.
func (m *MockCDCStore) UnregisterCDCSubscriber(dataset string, ch chan arrow.RecordBatch) {
	if m.cdcSubscribers == nil {
		return
	}
	subs := m.cdcSubscribers[dataset]
	for i, sub := range subs {
		if sub == ch {
			m.cdcSubscribers[dataset] = append(subs[:i], subs[i+1:]...)
			return
		}
	}
}
