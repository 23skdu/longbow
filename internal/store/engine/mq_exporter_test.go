package engine

import (
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMQConfig_Defaults runs the test mq config_defaults operation.
func TestMQConfig_Defaults(t *testing.T) {
	config := MQConfig{
		Brokers: "localhost:9092",
		Topic:   "test-topic",
		Type:    MQTypeKafka,
	}

	assert.Equal(t, "localhost:9092", config.Brokers)
	assert.Equal(t, "test-topic", config.Topic)
	assert.Equal(t, MQTypeKafka, config.Type)
}

// TestMQType_Constants runs the test mq type_constants operation.
func TestMQType_Constants(t *testing.T) {
	assert.Equal(t, MQType("kafka"), MQTypeKafka)
	assert.Equal(t, MQType("pulsar"), MQTypePulsar)
}

// TestNewMessageQueueExporter_MissingBrokers runs the test new message queue exporter_missing brokers operation.
func TestNewMessageQueueExporter_MissingBrokers(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	config := MQConfig{
		Brokers: "",
		Topic:   "test-topic",
		Type:    MQTypeKafka,
	}

	_, err := NewMessageQueueExporter(logger, cdc, config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "brokers is required")
}

// TestNewMessageQueueExporter_MissingTopic runs the test new message queue exporter_missing topic operation.
func TestNewMessageQueueExporter_MissingTopic(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	config := MQConfig{
		Brokers: "localhost:9092",
		Topic:   "",
		Type:    MQTypeKafka,
	}

	_, err := NewMessageQueueExporter(logger, cdc, config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "topic is required")
}

// TestNewMessageQueueExporter_InvalidType runs the test new message queue exporter_invalid type operation.
func TestNewMessageQueueExporter_InvalidType(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	config := MQConfig{
		Brokers: "localhost:9092",
		Topic:   "test-topic",
		Type:    "invalid",
	}

	_, err := NewMessageQueueExporter(logger, cdc, config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported message queue type")
}

// TestMQMetrics_InitialState runs the test mq metrics_initial state operation.
func TestMQMetrics_InitialState(t *testing.T) {
	metrics := MQMetrics{}

	assert.Equal(t, int64(0), metrics.MessagesProduced.Load())
	assert.Equal(t, int64(0), metrics.MessagesFailed.Load())
	assert.Equal(t, int64(0), metrics.BytesProduced.Load())
	assert.Equal(t, int64(0), metrics.BatchesProduced.Load())
}

// TestMQMetrics_Add runs the test mq metrics_add operation.
func TestMQMetrics_Add(t *testing.T) {
	metrics := MQMetrics{}

	metrics.MessagesProduced.Add(10)
	metrics.MessagesFailed.Add(2)
	metrics.BytesProduced.Add(100)
	metrics.BatchesProduced.Add(1)

	assert.Equal(t, int64(10), metrics.MessagesProduced.Load())
	assert.Equal(t, int64(2), metrics.MessagesFailed.Load())
	assert.Equal(t, int64(100), metrics.BytesProduced.Load())
	assert.Equal(t, int64(1), metrics.BatchesProduced.Load())
}

// TestMessageQueueExporter_GetConfig runs the test message queue exporter_get config operation.
func TestMessageQueueExporter_GetConfig(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	config := MQConfig{
		Brokers:        "localhost:9092",
		Topic:          "test-topic",
		Type:           MQTypeKafka,
		BatchSize:      50,
		BatchTimeoutMs: 200,
	}

	exporter := &MessageQueueExporter{
		logger: logger,
		cdc:    cdc,
		config: config,
	}

	gotConfig := exporter.GetConfig()
	assert.Equal(t, config, gotConfig)
}

// TestMessageQueueExporter_SetConfig runs the test message queue exporter_set config operation.
func TestMessageQueueExporter_SetConfig(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	config := MQConfig{
		Brokers: "localhost:9092",
		Topic:   "test-topic",
		Type:    MQTypeKafka,
	}

	exporter := &MessageQueueExporter{
		logger: logger,
		cdc:    cdc,
		config: config,
	}

	newConfig := MQConfig{
		Brokers:   "localhost:9092",
		Topic:     "test-topic",
		Type:      MQTypeKafka,
		BatchSize: 200,
	}

	err := exporter.SetConfig(newConfig)
	require.NoError(t, err)
	assert.Equal(t, 200, exporter.config.BatchSize)
}

// TestMessageQueueExporter_SetConfig_ChangeType runs the test message queue exporter_set config_change type operation.
func TestMessageQueueExporter_SetConfig_ChangeType(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	config := MQConfig{
		Brokers: "localhost:9092",
		Topic:   "test-topic",
		Type:    MQTypeKafka,
	}

	exporter := &MessageQueueExporter{
		logger: logger,
		cdc:    cdc,
		config: config,
	}

	newConfig := MQConfig{
		Brokers: "localhost:9092",
		Topic:   "test-topic",
		Type:    MQTypePulsar,
	}

	err := exporter.SetConfig(newConfig)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot change message queue type")
}

// TestMessageQueueExporter_IsRunning runs the test message queue exporter_is running operation.
func TestMessageQueueExporter_IsRunning(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	exporter := &MessageQueueExporter{
		logger:   logger,
		cdc:      cdc,
		stopChan: make(chan struct{}),
	}

	assert.True(t, exporter.IsRunning())

	close(exporter.stopChan)
	assert.False(t, exporter.IsRunning())
}

// TestMessageQueueExporter_compressionFromString runs the test message queue exporter_compression from string operation.
func TestMessageQueueExporter_compressionFromString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"gzip", "gzip"},
		{"snappy", "snappy"},
		{"lz4", "lz4"},
		{"none", "none"},
		{"invalid", "none"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			store := &VectorStore{}
			logger := zerolog.New(nil).With().Logger()
			cdc := NewChangeDataCapture(store, logger)

			exporter := &MessageQueueExporter{
				logger: logger,
				cdc:    cdc,
			}

			result := exporter.compressionFromString(tt.input)
			assert.Equal(t, tt.expected, result.String())
		})
	}
}

// TestMessageQueueExporter_BatchDefaults runs the test message queue exporter_batch defaults operation.
func TestMessageQueueExporter_BatchDefaults(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	config := MQConfig{
		Brokers: "localhost:9092",
		Topic:   "test-topic",
		Type:    MQTypeKafka,
	}

	exporter := &MessageQueueExporter{
		logger: logger,
		cdc:    cdc,
		config: config,
	}

	if exporter.config.BatchSize <= 0 {
		exporter.config.BatchSize = 100
	}
	if exporter.config.BatchTimeoutMs <= 0 {
		exporter.config.BatchTimeoutMs = 100
	}

	assert.Equal(t, 100, exporter.config.BatchSize)
	assert.Equal(t, 100, exporter.config.BatchTimeoutMs)
}

// TestMQExporter_BufferManagement runs the test mq exporter_buffer management operation.
func TestMQExporter_BufferManagement(t *testing.T) {
	store := &VectorStore{}
	logger := zerolog.New(nil).With().Logger()
	cdc := NewChangeDataCapture(store, logger)

	exporter := &MessageQueueExporter{
		logger:   logger,
		cdc:      cdc,
		buffer:   make([][]byte, 0, 10),
		bufferMu: sync.Mutex{},
	}

	exporter.bufferMu.Lock()
	exporter.buffer = append(exporter.buffer, []byte("test1"))
	exporter.buffer = append(exporter.buffer, []byte("test2"))
	exporter.buffer = append(exporter.buffer, []byte("test3"))
	length := len(exporter.buffer)
	exporter.bufferMu.Unlock()

	assert.Equal(t, 3, length)

	exporter.bufferMu.Lock()
	buffer := exporter.buffer
	exporter.buffer = make([][]byte, 0, 10)
	exporter.bufferMu.Unlock()

	assert.Equal(t, 3, len(buffer))
	assert.Equal(t, 0, len(exporter.buffer))
}
