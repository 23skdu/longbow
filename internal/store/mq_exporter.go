package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rs/zerolog"
)

type MQType string

const (
	MQTypeKafka  MQType = "kafka"
	MQTypePulsar MQType = "pulsar"
)

type MQConfig struct {
	Type           MQType `json:"type"`
	Brokers        string `json:"brokers"`
	Topic          string `json:"topic"`
	ProducerMode   string `json:"producer_mode"`
	BatchSize      int    `json:"batch_size"`
	BatchTimeoutMs int    `json:"batch_timeout_ms"`
	Acks           int    `json:"acks"`
	Compression    string `json:"compression"`
}

type MQMetrics struct {
	MessagesProduced atomic.Int64
	MessagesFailed   atomic.Int64
	BytesProduced    atomic.Int64
	BatchesProduced  atomic.Int64
}

type MessageQueueExporter struct {
	logger         zerolog.Logger
	cdc            *ChangeDataCapture
	config         MQConfig
	metrics        MQMetrics
	kafkaProducer  sarama.AsyncProducer
	pulsarProducer pulsar.Producer
	sub            *CDCSubscription
	stopChan       chan struct{}
	wg             sync.WaitGroup
	buffer         [][]byte
	bufferMu       sync.Mutex
	flushTicker    *time.Ticker
	mqType         MQType
	ctx            context.Context
	cancel         context.CancelFunc
}

func NewMessageQueueExporter(logger zerolog.Logger, cdc *ChangeDataCapture, config MQConfig) (*MessageQueueExporter, error) {
	if config.Brokers == "" {
		return nil, fmt.Errorf("brokers is required")
	}
	if config.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	exporter := &MessageQueueExporter{
		logger:   logger,
		cdc:      cdc,
		config:   config,
		stopChan: make(chan struct{}),
		mqType:   config.Type,
		ctx:      ctx,
		cancel:   cancel,
	}

	if config.BatchSize <= 0 {
		exporter.config.BatchSize = 100
	}
	if config.BatchTimeoutMs <= 0 {
		exporter.config.BatchTimeoutMs = 100
	}
	exporter.buffer = make([][]byte, 0, exporter.config.BatchSize)

	var err error
	switch config.Type {
	case MQTypeKafka:
		err = exporter.initKafka()
	case MQTypePulsar:
		err = exporter.initPulsar()
	default:
		err = fmt.Errorf("unsupported message queue type: %s", config.Type)
	}

	if err != nil {
		return nil, err
	}

	return exporter, nil
}

func (m *MessageQueueExporter) initKafka() error {
	saramaConfig := sarama.NewConfig()
	// Ensure Acks is within valid int16 range for Kafka
	acks := m.config.Acks
	if acks > 32767 {
		acks = 32767
	} else if acks < -1 {
		acks = -1
	}
	saramaConfig.Producer.RequiredAcks = sarama.RequiredAcks(acks)
	saramaConfig.Producer.Compression = m.compressionFromString(m.config.Compression)
	saramaConfig.Producer.Flush.Messages = m.config.BatchSize
	saramaConfig.Producer.Flush.Frequency = time.Duration(m.config.BatchTimeoutMs) * time.Millisecond

	producer, err := sarama.NewAsyncProducer([]string{m.config.Brokers}, saramaConfig)
	if err != nil {
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}

	m.kafkaProducer = producer

	m.logger.Info().Str("brokers", m.config.Brokers).Str("topic", m.config.Topic).Msg("Kafka producer initialized")
	return nil
}

func (m *MessageQueueExporter) initPulsar() error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               m.config.Brokers,
		ConnectionTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("failed to create pulsar client: %w", err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: m.config.Topic,
	})
	if err != nil {
		return fmt.Errorf("failed to create pulsar producer: %w", err)
	}

	m.pulsarProducer = producer

	m.logger.Info().Str("brokers", m.config.Brokers).Str("topic", m.config.Topic).Msg("Pulsar producer initialized")
	return nil
}

func (m *MessageQueueExporter) compressionFromString(comp string) sarama.CompressionCodec {
	switch comp {
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	default:
		return sarama.CompressionNone
	}
}

func (m *MessageQueueExporter) Start() error {
	filter := CDCFilter{
		EventTypes: []CDCEventType{CDCEventInsert, CDCEventUpdate, CDCEventDelete},
	}

	sub, err := m.cdc.Subscribe("__all__", filter, m.config.BatchSize*2)
	if err != nil {
		return fmt.Errorf("failed to subscribe to CDC: %w", err)
	}

	m.sub = sub
	m.flushTicker = time.NewTicker(time.Duration(m.config.BatchTimeoutMs) * time.Millisecond)

	m.wg.Add(2)
	go m.eventLoop()
	go m.flushLoop()

	m.logger.Info().Msg("Message queue exporter started")
	return nil
}

func (m *MessageQueueExporter) eventLoop() {
	defer m.wg.Done()

	for {
		select {
		case batch, ok := <-m.sub.Ch:
			if !ok {
				return
			}

			jsonData, err := m.cdc.EventToJSON(CDCEvent{
				EventType: CDCEventInsert,
				Dataset:   "unknown",
				Batch:     batch,
			})
			if err != nil {
				m.logger.Error().Err(err).Msg("Failed to serialize CDC event")
				batch.Release()
				continue
			}

			m.bufferMu.Lock()
			m.buffer = append(m.buffer, jsonData)
			shouldFlush := len(m.buffer) >= m.config.BatchSize
			m.bufferMu.Unlock()

			if shouldFlush {
				m.flush()
			}

			batch.Release()

		case <-m.ctx.Done():
			return

		case <-m.stopChan:
			return
		}
	}
}

func (m *MessageQueueExporter) flushLoop() {
	defer m.wg.Done()

	for {
		select {
		case <-m.flushTicker.C:
			m.flush()
		case <-m.ctx.Done():
			m.flush()
			return
		case <-m.stopChan:
			m.flush()
			return
		}
	}
}

func (m *MessageQueueExporter) flush() {
	m.bufferMu.Lock()
	if len(m.buffer) == 0 {
		m.bufferMu.Unlock()
		return
	}

	buffer := m.buffer
	m.buffer = make([][]byte, 0, m.config.BatchSize)
	m.bufferMu.Unlock()

	for _, data := range buffer {
		switch m.mqType {
		case MQTypeKafka:
			m.sendToKafka(data)
		case MQTypePulsar:
			m.sendToPulsar(data)
		}
	}

	m.metrics.BatchesProduced.Add(1)
}

func (m *MessageQueueExporter) sendToKafka(data []byte) {
	if m.kafkaProducer == nil {
		m.metrics.MessagesFailed.Add(1)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: m.config.Topic,
		Key:   nil,
		Value: sarama.ByteEncoder(data),
	}

	select {
	case m.kafkaProducer.Input() <- msg:
		m.metrics.MessagesProduced.Add(1)
		m.metrics.BytesProduced.Add(int64(len(data)))
	default:
		m.metrics.MessagesFailed.Add(1)
	}
}

func (m *MessageQueueExporter) sendToPulsar(data []byte) {
	if m.pulsarProducer == nil {
		m.metrics.MessagesFailed.Add(1)
		return
	}

	_, err := m.pulsarProducer.Send(m.ctx, &pulsar.ProducerMessage{
		Payload: data,
	})
	if err != nil {
		m.metrics.MessagesFailed.Add(1)
		return
	}

	m.metrics.MessagesProduced.Add(1)
	m.metrics.BytesProduced.Add(int64(len(data)))
}

func (m *MessageQueueExporter) Stop() error {
	close(m.stopChan)

	if m.flushTicker != nil {
		m.flushTicker.Stop()
	}

	if m.sub != nil {
		m.cdc.Unsubscribe(m.sub.ID)
	}

	m.wg.Wait()

	if m.kafkaProducer != nil {
		m.kafkaProducer.AsyncClose()
	}

	if m.pulsarProducer != nil {
		m.pulsarProducer.Close()
	}

	m.logger.Info().Msg("Message queue exporter stopped")
	return nil
}

func (m *MessageQueueExporter) GetMetrics() (produced, failed, bytes, batches int64) {
	return m.metrics.MessagesProduced.Load(),
		m.metrics.MessagesFailed.Load(),
		m.metrics.BytesProduced.Load(),
		m.metrics.BatchesProduced.Load()
}

func (m *MessageQueueExporter) GetConfig() MQConfig {
	return m.config
}

func (m *MessageQueueExporter) SetConfig(config MQConfig) error {
	if config.Type != m.config.Type {
		return fmt.Errorf("cannot change message queue type")
	}

	m.config = config
	return nil
}

func (m *MessageQueueExporter) IsRunning() bool {
	select {
	case <-m.stopChan:
		return false
	default:
		return true
	}
}
