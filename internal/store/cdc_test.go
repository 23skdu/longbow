package store

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCDCEventType_String(t *testing.T) {
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

func TestCDCFilter(t *testing.T) {
	filter := CDCFilter{
		EventTypes: []CDCEventType{CDCEventInsert, CDCEventUpdate},
		Columns:    []string{"vector", "value"},
		Since:      time.Now().Add(-time.Hour),
	}

	assert.Len(t, filter.EventTypes, 2)
	assert.Contains(t, filter.Columns, "vector")
}

func TestCDCSubscription_PauseResume(t *testing.T) {
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

func TestCDCSubscription_Close(t *testing.T) {
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

func TestCDCSubscription_Close_Idempotent(t *testing.T) {
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

func TestCDCConfig_Defaults(t *testing.T) {
	store := &VectorStore{}
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

func TestNewChangeDataCapture(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()

	cdc := NewChangeDataCapture(store, logger)

	assert.NotNil(t, cdc)
	assert.NotNil(t, cdc.subscriptions)
	assert.NotNil(t, cdc.stopChan)
	assert.Equal(t, 1024, cdc.config.BufferSize)
	assert.True(t, cdc.config.EnableJSON)
	assert.True(t, cdc.config.EnableArrow)
}

func TestChangeDataCapture_Subscribe(t *testing.T) {
	store := &VectorStore{
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

	store.cdcMu.RLock()
	_, ok := store.cdcSubscribers["test-dataset"]
	store.cdcMu.RUnlock()
	assert.True(t, ok)
	assert.Len(t, _, 1)
	assert.Equal(t, sub.Ch[0])
}

func TestChangeDataCapture_Subscribe_EmptyDataset(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	_, err := cdc.Subscribe("", CDCFilter{}, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dataset name required")
}

func TestChangeDataCapture_Subscribe_Disabled(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)
	cdc.config.Enabled = false

	_, err := cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CDC is disabled")
}

func TestChangeDataCapture_Unsubscribe(t *testing.T) {
	store := &VectorStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	sub, err := cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	require.NoError(t, err)

	err = cdc.Unsubscribe(sub.ID)
	require.NoError(t, err)

	subs := cdc.ListSubscriptions()
	assert.Len(t, _, 0)
}

func TestChangeDataCapture_Unsubscribe_NotFound(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	err := cdc.Unsubscribe("non-existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestChangeDataCapture_GetSubscription(t *testing.T) {
	store := &VectorStore{}
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

func TestChangeDataCapture_GetSubscriptionByDataset(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	_, err := cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	require.NoError(t, err)
	_, err = cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	require.NoError(t, err)
	_, err = cdc.Subscribe("other-dataset", CDCFilter{}, 100)
	require.NoError(t, err)

	subs := cdc.GetSubscriptionByDataset("test-dataset")
	assert.Len(t, _, 2)
}

func TestChangeDataCapture_ListSubscriptions(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	_, err := cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	require.NoError(t, err)
	_, err = cdc.Subscribe("other-dataset", CDCFilter{}, 100)
	require.NoError(t, err)

	subs := cdc.ListSubscriptions()
	assert.Len(t, _, 2)
}

func TestChangeDataCapture_HandleCDCBatch_Disabled(t *testing.T) {
	store := &VectorStore{
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

func TestChangeDataCapture_HandleCDCBatch_NoSubscribers(t *testing.T) {
	store := &VectorStore{
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

func TestChangeDataCapture_HandleCDCBatch(t *testing.T) {
	store := &VectorStore{
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
		assert.Equal(t, record.NumRows().NumRows())
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for CDC event")
	}

	metrics, _, _, _, _, _ := cdc.GetMetrics()
	assert.Equal(t, int64(3), metrics)
}

func TestChangeDataCapture_HandleCDCBatch_PausedSubscription(t *testing.T) {
	store := &VectorStore{
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

func TestChangeDataCapture_HandleCDCBatch_ColumnFilter(t *testing.T) {
	store := &VectorStore{
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

func TestChangeDataCapture_HandleCDCBatch_ColumnFilter_Disabled(t *testing.T) {
	store := &VectorStore{
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

func TestChangeDataCapture_matchesFilter(t *testing.T) {
	store := &VectorStore{}
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

func TestChangeDataCapture_matchesColumnsFilter(t *testing.T) {
	store := &VectorStore{}
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

func TestChangeDataCapture_matchesEventTypeFilter(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	assert.True(t, cdc.matchesEventTypeFilter([]CDCEventType{CDCEventInsert}))
	assert.True(t, cdc.matchesEventTypeFilter([]CDCEventType{CDCEventUpdate}))
	assert.True(t, cdc.matchesEventTypeFilter([]CDCEventType{CDCEventDelete}))
	assert.False(t, cdc.matchesEventTypeFilter([]CDCEventType{}))
}

func TestChangeDataCapture_extractPrimaryKeys(t *testing.T) {
	store := &VectorStore{}
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

func TestChangeDataCapture_EventToJSON(t *testing.T) {
	store := &VectorStore{}
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

func TestChangeDataCapture_EventToJSON_Disabled(t *testing.T) {
	store := &VectorStore{}
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

func TestChangeDataCapture_SetConfig(t *testing.T) {
	store := &VectorStore{}
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

func TestChangeDataCapture_GetMetrics(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	_, _, _, _, _ := cdc.GetMetrics()
	assert.Equal(t, int64(0))
	assert.Equal(t, int64(0))
	assert.Equal(t, int64(0))
}

func TestChangeDataCapture_IsEnabled(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	assert.True(t, cdc.IsEnabled())

	cdc.Disable()
	assert.False(t, cdc.IsEnabled())

	cdc.Enable()
	assert.True(t, cdc.IsEnabled())
}

func TestChangeDataCapture_EnableDisable(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	assert.True(t, cdc.IsEnabled())
	cdc.Disable()
	assert.False(t, cdc.IsEnabled())
	cdc.Enable()
	assert.True(t, cdc.IsEnabled())
}

func TestGetValueAt(t *testing.T) {
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
		builder := array.NewInt8Builder(mem)
		builder.AppendValues([]int8{1}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		_, err := GetValueAt(arr, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported")
	})
}

func TestChangeDataCapture_HandleCDCBatch_DropOnFull(t *testing.T) {
	store := &VectorStore{
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

	_, _, _, _, _ := cdc.GetMetrics()
	assert.Equal(t, int64(1))
	assert.Equal(t, int64(1))
}

func TestChangeDataCapture_HandleCDCBatch_AsyncDispatch(t *testing.T) {
	store := &VectorStore{
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

	_, _, _, _, _ := cdc.GetMetrics()
	assert.Equal(t, int64(1))
}

func TestChangeDataCapture_MultipleBatches(t *testing.T) {
	store := &VectorStore{
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

	_, _, _, _, _ := cdc.GetMetrics()
	assert.Equal(t, int64(3))
}

func TestChangeDataCapture_CloseSubscription_UpdatesMetrics(t *testing.T) {
	store := &VectorStore{
		cdcSubscribers: make(map[string][]chan arrow.RecordBatch),
	}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	sub, err := cdc.Subscribe("test-dataset", CDCFilter{}, 100)
	require.NoError(t, err)

	_, _, _, _, _ := cdc.GetMetrics()
	initialSubs := subs
	assert.Equal(t, int64(1), initialSubs)

	sub.Close()

	metrics = cdc.GetMetrics()
	assert.Equal(t, int64(0))
}

func TestCDCMetrics_Reset(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	_, _, _, _, _ := cdc.GetMetrics()
	metrics.EventsReceived.Add(10)
	metrics.EventsSent.Add(5)

	metrics.Reset()

	assert.Equal(t, int64(0))
	assert.Equal(t, int64(0))
}
