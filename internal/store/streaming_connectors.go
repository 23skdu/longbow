package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/23skdu/longbow/internal/metrics"
)

// StreamConnectorConfig represents a stream connector config value.
type StreamConnectorConfig struct {
	Name                string           `json:"name"`
	Dataset             string           `json:"dataset"`
	SourceType          StreamSourceType `json:"source_type"`
	Brokers             string           `json:"brokers"`
	Topic               string           `json:"topic"`
	GroupID             string           `json:"group_id,omitempty"`
	SchemaRegistryURL   string           `json:"schema_registry_url,omitempty"`
	SchemaSubject       string           `json:"schema_subject,omitempty"`
	ExpectedDim         int              `json:"expected_dim,omitempty"`
	BatchSize           int              `json:"batch_size,omitempty"`
	FlushIntervalMs     int              `json:"flush_interval_ms,omitempty"`
	MaxInFlightBatches  int              `json:"max_in_flight_batches,omitempty"`
	BackpressurePauseMs int              `json:"backpressure_pause_ms,omitempty"`
	ExactlyOnce         bool             `json:"exactly_once"`
	ExportCDC           bool             `json:"export_cdc,omitempty"`
	ExportTopic         string           `json:"export_topic,omitempty"`
}

// StreamConnectorStats exposes live lag, deduplication, backpressure, and throughput metrics.
type StreamConnectorStats struct {
	Name               string  `json:"name"`
	Dataset            string  `json:"dataset"`
	SourceType         string  `json:"source_type"`
	Topic              string  `json:"topic"`
	Running            bool    `json:"running"`
	Consumed           int64   `json:"consumed"`
	Committed          int64   `json:"committed"`
	Deduplicated       int64   `json:"deduplicated"`
	Failed             int64   `json:"failed"`
	DLQCount           int64   `json:"dlq_count"`
	BackpressurePauses int64   `json:"backpressure_pauses"`
	InFlight           int64   `json:"in_flight"`
	CommittedOffset    int64   `json:"committed_offset"`
	LatestOffset       int64   `json:"latest_offset"`
	ConsumerLag        int64   `json:"consumer_lag"`
	ThroughputEPS      float64 `json:"throughput_eps"`
}

// StreamConnector integrates Confluent-style Schema Registry HTTP endpoints
// and local schema caching with wire-frame decoding and payload validation.
// StreamConnector manages a single streaming ingestion connector (and optional CDC exporter).
type StreamConnector struct {
	store      *VectorStore
	cfg        StreamConnectorConfig
	registry   *SchemaRegistryClient
	dedup      *SequenceDeduplicator
	exporter   *MessageQueueExporter
	inFlightCh chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	startedAt          time.Time
	running            atomic.Bool
	consumed           atomic.Int64
	committed          atomic.Int64
	deduplicated       atomic.Int64
	failed             atomic.Int64
	dlqCount           atomic.Int64
	backpressurePauses atomic.Int64
	inFlight           atomic.Int64
	committedOffset    atomic.Int64
	latestOffset       atomic.Int64

	dlqMu sync.Mutex
	dlq   []DLQEntry
}

// StreamIngestionManager orchestrates active streaming connectors and schema registry integration.
type StreamIngestionManager struct {
	store      *VectorStore
	registry   *SchemaRegistryClient
	mu         sync.RWMutex
	connectors map[string]*StreamConnector
}

// NewStreamIngestionManager initializes a streaming ingestion manager attached to VectorStore.
func NewStreamIngestionManager(s *VectorStore) *StreamIngestionManager {
	return &StreamIngestionManager{
		store:      s,
		registry:   NewSchemaRegistryClient(""),
		connectors: make(map[string]*StreamConnector),
	}
}

// Registry returns the shared SchemaRegistryClient.
func (m *StreamIngestionManager) Registry() *SchemaRegistryClient {
	return m.registry
}

// CreateConnector registers (and optionally starts for external Kafka/Pulsar brokers) a streaming connector.
func (m *StreamIngestionManager) CreateConnector(cfg StreamConnectorConfig, startNow bool) (*StreamConnector, error) {
	if cfg.Name == "" {
		return nil, errors.New("connector name is required")
	}
	if cfg.Dataset == "" {
		return nil, errors.New("connector dataset is required")
	}
	if cfg.SourceType == "" {
		cfg.SourceType = StreamSourceMemory
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 64
	}
	if cfg.FlushIntervalMs <= 0 {
		cfg.FlushIntervalMs = 50
	}
	if cfg.MaxInFlightBatches <= 0 {
		cfg.MaxInFlightBatches = 16
	}
	if cfg.BackpressurePauseMs <= 0 {
		cfg.BackpressurePauseMs = 25
	}
	reg := m.registry
	if cfg.SchemaRegistryURL != "" {
		reg = NewSchemaRegistryClient(cfg.SchemaRegistryURL)
	}
	if cfg.ExpectedDim > 0 && cfg.SchemaSubject != "" {
		reg.RegisterLocal(cfg.SchemaSubject, RegisteredStreamSchema{
			Subject:     cfg.SchemaSubject,
			ExpectedDim: cfg.ExpectedDim,
		})
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.connectors[cfg.Name]; exists {
		return nil, fmt.Errorf("connector %q already exists", cfg.Name)
	}

	ctx, cancel := context.WithCancel(context.Background()) // #nosec G118 -- lifecycle managed by Stop()
	conn := &StreamConnector{
		store:      m.store,
		cfg:        cfg,
		registry:   reg,
		dedup:      NewSequenceDeduplicator(65536),
		inFlightCh: make(chan struct{}, cfg.MaxInFlightBatches),
		ctx:        ctx,
		cancel:     cancel,
		startedAt:  time.Now(),
	}
	if cfg.ExportCDC && (cfg.SourceType == StreamSourceKafka || cfg.SourceType == StreamSourcePulsar) && m.store != nil && cfg.Brokers != "" {
		exportTopic := cfg.ExportTopic
		if exportTopic == "" {
			exportTopic = cfg.Topic + ".cdc"
		}
		mqType := MQTypeKafka
		if cfg.SourceType == StreamSourcePulsar {
			mqType = MQTypePulsar
		}
		cdcAdapter := NewChangeDataCapture(m.store, m.store.logger)
		exp, err := NewMessageQueueExporter(m.store.logger, cdcAdapter, MQConfig{
			Type:           mqType,
			Brokers:        cfg.Brokers,
			Topic:          exportTopic,
			BatchSize:      cfg.BatchSize,
			BatchTimeoutMs: cfg.FlushIntervalMs,
		})
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to initialize bidirectional CDC exporter: %w", err)
		}
		conn.exporter = exp
	}

	m.connectors[cfg.Name] = conn
	if startNow {
		if err := conn.Start(); err != nil {
			delete(m.connectors, cfg.Name)
			cancel()
			return nil, err
		}
	}
	return conn, nil
}

// GetConnector returns a named connector.
func (m *StreamIngestionManager) GetConnector(name string) (*StreamConnector, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.connectors[name]
	return c, ok
}

// StopConnector stops and removes a connector.
func (m *StreamIngestionManager) StopConnector(name string) error {
	m.mu.Lock()
	c, ok := m.connectors[name]
	if ok {
		delete(m.connectors, name)
	}
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("connector %q not found", name)
	}
	return c.Stop()
}

// ListStats lists stats for all connectors.
func (m *StreamIngestionManager) ListStats() []StreamConnectorStats {
	m.mu.RLock()
	conns := make([]*StreamConnector, 0, len(m.connectors))
	for _, c := range m.connectors {
		conns = append(conns, c)
	}
	m.mu.RUnlock()

	out := make([]StreamConnectorStats, 0, len(conns))
	for _, c := range conns {
		out = append(out, c.Stats())
	}
	return out
}

// Start begins external Kafka/Pulsar consumer loops (or marks memory connector running).
func (c *StreamConnector) Start() error {
	if c.running.Swap(true) {
		return nil
	}
	if c.exporter != nil {
		if err := c.exporter.Start(); err != nil {
			c.running.Store(false)
			return err
		}
	}
	switch c.cfg.SourceType {
	case StreamSourceKafka:
		if c.cfg.Brokers == "" || c.cfg.Topic == "" {
			c.running.Store(false)
			return errors.New("kafka source requires brokers and topic")
		}
		c.wg.Add(1)
		go c.runKafkaConsumer()
	case StreamSourcePulsar:
		if c.cfg.Brokers == "" || c.cfg.Topic == "" {
			c.running.Store(false)
			return errors.New("pulsar source requires brokers and topic")
		}
		c.wg.Add(1)
		go c.runPulsarConsumer()
	case StreamSourceMemory:
		// Driven directly via ProcessEnvelope / IngestBatch
	default:
		c.running.Store(false)
		return fmt.Errorf("unsupported source type: %s", c.cfg.SourceType)
	}
	return nil
}

// Stop shuts down consumer loops and bidirectional exporter.
func (c *StreamConnector) Stop() error {
	c.running.Store(false)
	c.cancel()
	c.wg.Wait()
	if c.exporter != nil {
		return c.exporter.Stop()
	}
	return nil
}

// Stats returns a point-in-time snapshot of connector metrics.
func (c *StreamConnector) Stats() StreamConnectorStats {
	committedOff := c.committedOffset.Load()
	latestOff := c.latestOffset.Load()
	lag := latestOff - committedOff
	if lag < 0 {
		lag = 0
	}
	metrics.StreamConsumerLag.WithLabelValues(c.cfg.Name, c.cfg.Topic).Set(float64(lag))
	elapsed := time.Since(c.startedAt).Seconds()
	eps := 0.0
	if elapsed > 0 {
		eps = float64(c.committed.Load()) / elapsed
	}
	return StreamConnectorStats{
		Name:               c.cfg.Name,
		Dataset:            c.cfg.Dataset,
		SourceType:         string(c.cfg.SourceType),
		Topic:              c.cfg.Topic,
		Running:            c.running.Load(),
		Consumed:           c.consumed.Load(),
		Committed:          c.committed.Load(),
		Deduplicated:       c.deduplicated.Load(),
		Failed:             c.failed.Load(),
		DLQCount:           c.dlqCount.Load(),
		BackpressurePauses: c.backpressurePauses.Load(),
		InFlight:           c.inFlight.Load(),
		CommittedOffset:    committedOff,
		LatestOffset:       latestOff,
		ConsumerLag:        lag,
		ThroughputEPS:      eps,
	}
}

// DLQEntries returns recorded dead-letter queue entries.
func (c *StreamConnector) DLQEntries() []DLQEntry {
	c.dlqMu.Lock()
	defer c.dlqMu.Unlock()
	out := make([]DLQEntry, len(c.dlq))
	copy(out, c.dlq)
	return out
}

func (c *StreamConnector) recordDLQ(env StreamEnvelope, reason string) {
	c.failed.Add(1)
	c.dlqCount.Add(1)
	metrics.StreamDLQMessagesTotal.WithLabelValues(c.cfg.Name, reason).Inc()
	metrics.StreamConsumedMessagesTotal.WithLabelValues(c.cfg.Name, string(c.cfg.SourceType), "dlq").Inc()

	c.dlqMu.Lock()
	if len(c.dlq) >= 1024 {
		c.dlq = c.dlq[1:]
	}
	c.dlq = append(c.dlq, DLQEntry{
		Connector: c.cfg.Name,
		Topic:     env.Topic,
		Partition: env.Partition,
		Offset:    env.Offset,
		Reason:    reason,
		Payload:   string(env.Payload),
		Timestamp: time.Now(),
	})
	c.dlqMu.Unlock()
}

// ProcessEnvelopes validates, deduplicates, applies backpressure, and ingests a batch of envelopes.
// Returns (committedCount, deduplicatedCount, error).
func (c *StreamConnector) ProcessEnvelopes(ctx context.Context, envs []StreamEnvelope) (int, int, error) {
	if len(envs) == 0 {
		return 0, 0, nil
	}

	// Acquire backpressure slot + respect store ingestion backpressure
	for c.store != nil && c.store.CheckIngestionBackpressure() {
		c.backpressurePauses.Add(1)
		metrics.StreamBackpressurePausesTotal.WithLabelValues(c.cfg.Name).Inc()
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-c.ctx.Done():
			return 0, 0, errors.New("connector stopped")
		case <-time.After(time.Duration(c.cfg.BackpressurePauseMs) * time.Millisecond):
		}
	}

	select {
	case c.inFlightCh <- struct{}{}:
		c.inFlight.Add(1)
		defer func() {
			c.inFlight.Add(-1)
			<-c.inFlightCh
		}()
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	case <-c.ctx.Done():
		return 0, 0, errors.New("connector stopped")
	}

	events := make([]StreamVectorEvent, 0, len(envs))
	deduped := 0
	var maxOffsetSeen int64

	for _, env := range envs {
		c.consumed.Add(1)
		if env.Offset > maxOffsetSeen {
			maxOffsetSeen = env.Offset
		}
		if maxOffsetSeen > c.latestOffset.Load() {
			c.latestOffset.Store(maxOffsetSeen)
		}

		schemaID, hasHeader, jsonPayload, err := DecodeWirePayload(env.Payload)
		if err != nil {
			c.recordDLQ(env, "wire_decode_error")
			continue
		}

		var ev StreamVectorEvent
		if err := json.Unmarshal(jsonPayload, &ev); err != nil {
			c.recordDLQ(env, "json_decode_error")
			continue
		}
		if ev.ID == "" && env.Key != "" {
			ev.ID = env.Key
		}

		// Schema validation when schema ID or configured subject/dimension exists
		schema := RegisteredStreamSchema{ExpectedDim: c.cfg.ExpectedDim}
		if hasHeader || c.cfg.SchemaSubject != "" {
			resolved, found, err := c.registry.ResolveSchema(ctx, schemaID, c.cfg.SchemaSubject)
			if err != nil {
				c.recordDLQ(env, "schema_lookup_error")
				continue
			}
			if found {
				schema = resolved
			}
		}
		if err := ValidateStreamEvent(schema, &ev); err != nil {
			c.recordDLQ(env, "schema_validation_error")
			continue
		}

		if c.cfg.ExactlyOnce {
			idemKey := ev.IdempotencyKey
			if idemKey == "" && env.Headers != nil {
				idemKey = env.Headers["idempotency-key"]
			}
			if !c.dedup.CheckAndMark(env.Partition, env.Offset, idemKey) {
				deduped++
				c.deduplicated.Add(1)
				metrics.StreamDeduplicatedMessagesTotal.WithLabelValues(c.cfg.Name).Inc()
				continue
			}
		}
		events = append(events, ev)
	}

	if len(events) == 0 {
		if maxOffsetSeen > c.committedOffset.Load() {
			c.committedOffset.Store(maxOffsetSeen)
		}
		return 0, deduped, nil
	}

	if err := c.ingestEvents(ctx, events); err != nil {
		return 0, deduped, err
	}

	c.committed.Add(int64(len(events)))
	if maxOffsetSeen > c.committedOffset.Load() {
		c.committedOffset.Store(maxOffsetSeen)
	}
	metrics.StreamConsumedMessagesTotal.WithLabelValues(c.cfg.Name, string(c.cfg.SourceType), "ok").Add(float64(len(events)))
	metrics.StreamConsumerLag.WithLabelValues(c.cfg.Name, c.cfg.Topic).Set(float64(max(0, c.latestOffset.Load()-c.committedOffset.Load())))
	return len(events), deduped, nil
}

func (c *StreamConnector) ingestEvents(ctx context.Context, events []StreamVectorEvent) error {
	if c.store == nil {
		return nil
	}
	upserts := make([]StreamVectorEvent, 0, len(events))
	for _, ev := range events {
		if strings.EqualFold(ev.Op, "delete") {
			c.deleteVectorByID(c.cfg.Dataset, ev.ID)
			continue
		}
		upserts = append(upserts, ev)
	}
	if len(upserts) == 0 {
		return nil
	}
	alloc := c.store.mem
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	rec, err := buildStreamArrowBatch(alloc, upserts)
	if err != nil {
		return err
	}
	defer rec.Release()
	return c.store.StoreRecordBatch(ctx, c.cfg.Dataset, rec)
}

func (c *StreamConnector) deleteVectorByID(datasetName, rawID string) {
	if c.store == nil {
		return
	}
	ds, ok := c.store.getDataset(datasetName)
	if !ok {
		return
	}
	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()
	if loc, exists := ds.PrimaryIndex[rawID]; exists {
		setTombstone(ds, loc.BatchIdx, loc.RowIdx)
	}
}
