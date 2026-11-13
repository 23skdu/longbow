package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/pulsar-client-go/pulsar"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// ConfluentWireMagic is the standard 1-byte Confluent Schema Registry framing magic byte.
const ConfluentWireMagic byte = 0x00

// StreamSourceType defines supported streaming ingress sources.
type StreamSourceType string

const (
	StreamSourceKafka  StreamSourceType = "kafka"
	StreamSourcePulsar StreamSourceType = "pulsar"
	StreamSourceMemory StreamSourceType = "memory"
)

// RegisteredStreamSchema describes expected structure and dimensions for streaming payloads.
type RegisteredStreamSchema struct {
	ID              uint32            `json:"id"`
	Subject         string            `json:"subject"`
	Version         int               `json:"version"`
	ExpectedDim     int               `json:"expected_dim"`
	RequiredFields  []string          `json:"required_fields"`
	FieldTypes      map[string]string `json:"field_types,omitempty"`
	AllowExtraField bool              `json:"allow_extra_fields"`
}

// StreamVectorEvent represents a decoded streaming vector upsert/delete event.
type StreamVectorEvent struct {
	Op             string         `json:"op,omitempty"` // "upsert" (default) or "delete"
	ID             string         `json:"id"`
	Vector         []float32      `json:"vector,omitempty"`
	Embedding      []float32      `json:"embedding,omitempty"`
	Text           string         `json:"text,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	TimestampUnix  int64          `json:"timestamp_unix,omitempty"`
}

// StreamEnvelope wraps raw bytes with broker partition/offset/key metadata.
type StreamEnvelope struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	Payload   []byte            `json:"payload"`
	Timestamp time.Time         `json:"timestamp,omitempty"`
}

// DLQEntry stores a rejected streaming message and reason for inspection or replay.
type DLQEntry struct {
	Connector string    `json:"connector"`
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Reason    string    `json:"reason"`
	Payload   string    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

// StreamConnectorConfig configures a bidirectional streaming ingestion + optional CDC export pipeline.
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

// SchemaRegistryClient integrates Confluent-style Schema Registry HTTP endpoints
// and local schema caching with wire-frame decoding and payload validation.
type SchemaRegistryClient struct {
	mu         sync.RWMutex
	baseURL    string
	httpClient *http.Client
	byID       map[uint32]RegisteredStreamSchema
	bySubject  map[string]RegisteredStreamSchema
	nextID     uint32
}

// NewSchemaRegistryClient creates a schema registry client. baseURL may be empty for offline/local-only mode.
func NewSchemaRegistryClient(baseURL string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		byID:      make(map[uint32]RegisteredStreamSchema),
		bySubject: make(map[string]RegisteredStreamSchema),
		nextID:    1,
	}
}

// RegisterLocal registers or updates a schema locally for deterministic validation and tests.
func (r *SchemaRegistryClient) RegisterLocal(subject string, schema RegisteredStreamSchema) uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if schema.ID == 0 {
		schema.ID = r.nextID
		r.nextID++
	} else if schema.ID >= r.nextID {
		r.nextID = schema.ID + 1
	}
	schema.Subject = subject
	if schema.Version <= 0 {
		schema.Version = 1
	}
	r.byID[schema.ID] = schema
	if subject != "" {
		r.bySubject[subject] = schema
	}
	return schema.ID
}

// EncodeWirePayload prepends the 5-byte Confluent Schema Registry wire header to payload.
func EncodeWirePayload(schemaID uint32, payload []byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = ConfluentWireMagic
	binary.BigEndian.PutUint32(out[1:5], schemaID)
	copy(out[5:], payload)
	return out
}

// DecodeWirePayload strips an optional Confluent 5-byte wire header if present.
// Returns (schemaID, hasHeader, jsonPayload, err).
func DecodeWirePayload(raw []byte) (uint32, bool, []byte, error) {
	if len(raw) == 0 {
		return 0, false, nil, errors.New("empty streaming payload")
	}
	if raw[0] == ConfluentWireMagic {
		if len(raw) < 6 {
			return 0, true, nil, fmt.Errorf("truncated wire-framed payload: %d bytes", len(raw))
		}
		schemaID := binary.BigEndian.Uint32(raw[1:5])
		return schemaID, true, raw[5:], nil
	}
	return 0, false, raw, nil
}

// ResolveSchema looks up a schema by ID or subject, querying the HTTP registry when configured.
func (r *SchemaRegistryClient) ResolveSchema(ctx context.Context, schemaID uint32, subject string) (RegisteredStreamSchema, bool, error) {
	r.mu.RLock()
	if schemaID > 0 {
		if s, ok := r.byID[schemaID]; ok {
			r.mu.RUnlock()
			return s, true, nil
		}
	} else if subject != "" {
		if s, ok := r.bySubject[subject]; ok {
			r.mu.RUnlock()
			return s, true, nil
		}
	}
	baseURL := r.baseURL
	r.mu.RUnlock()

	if baseURL == "" {
		if schemaID > 0 {
			r.mu.RLock()
			if s, ok := r.bySubject[subject]; ok {
				r.mu.RUnlock()
				return s, true, nil
			}
			r.mu.RUnlock()
			return RegisteredStreamSchema{}, false, fmt.Errorf("schema id %d not found in local registry", schemaID)
		}
		return RegisteredStreamSchema{}, false, nil
	}

	parsed, err := url.Parse(baseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return RegisteredStreamSchema{}, false, fmt.Errorf("invalid schema registry URL: %q", baseURL)
	}

	var endpoint string
	if schemaID > 0 {
		endpoint = fmt.Sprintf("%s/schemas/ids/%d", baseURL, schemaID)
	} else if subject != "" {
		endpoint = fmt.Sprintf("%s/subjects/%s/versions/latest", baseURL, url.PathEscape(subject))
	} else {
		return RegisteredStreamSchema{}, false, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return RegisteredStreamSchema{}, false, err
	}
	resp, err := r.httpClient.Do(req) // #nosec G107 -- endpoint validated above
	if err != nil {
		return RegisteredStreamSchema{}, false, fmt.Errorf("schema registry fetch failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return RegisteredStreamSchema{}, false, fmt.Errorf("schema registry status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return RegisteredStreamSchema{}, false, err
	}
	var wireResp struct {
		ID      uint32 `json:"id"`
		Version int    `json:"version"`
		Schema  string `json:"schema"`
	}
	if err := json.Unmarshal(body, &wireResp); err != nil {
		return RegisteredStreamSchema{}, false, err
	}
	resolved := RegisteredStreamSchema{
		ID:      wireResp.ID,
		Subject: subject,
		Version: wireResp.Version,
	}
	var inner map[string]any
	if json.Unmarshal([]byte(wireResp.Schema), &inner) == nil {
		if dimVal, ok := inner["expected_dim"].(float64); ok {
			resolved.ExpectedDim = int(dimVal)
		}
		if reqRaw, ok := inner["required"].([]any); ok {
			for _, item := range reqRaw {
				if name, ok := item.(string); ok {
					resolved.RequiredFields = append(resolved.RequiredFields, name)
				}
			}
		}
	}
	if resolved.ID == 0 {
		resolved.ID = schemaID
	}
	r.RegisterLocal(subject, resolved)
	return resolved, true, nil
}

// ValidateEvent checks an event against the resolved schema rules.
func ValidateStreamEvent(schema RegisteredStreamSchema, ev *StreamVectorEvent) error {
	if ev.ID == "" {
		return errors.New("streaming event missing required field: id")
	}
	vec := ev.Vector
	if len(vec) == 0 && len(ev.Embedding) > 0 {
		vec = ev.Embedding
	}
	if !strings.EqualFold(ev.Op, "delete") {
		if len(vec) == 0 {
			return errors.New("streaming upsert event missing vector/embedding")
		}
		if schema.ExpectedDim > 0 && len(vec) != schema.ExpectedDim {
			return fmt.Errorf("vector dimension mismatch: got %d, schema requires %d", len(vec), schema.ExpectedDim)
		}
	}
	for _, reqField := range schema.RequiredFields {
		switch reqField {
		case "id":
			if ev.ID == "" {
				return errors.New("required field id is empty")
			}
		case "vector":
			if len(vec) == 0 && !strings.EqualFold(ev.Op, "delete") {
				return errors.New("required field vector is missing")
			}
		case "text":
			if ev.Text == "" {
				return errors.New("required field text is missing")
			}
		default:
			if ev.Metadata == nil {
				return fmt.Errorf("required metadata field %q missing", reqField)
			}
			if _, ok := ev.Metadata[reqField]; !ok {
				return fmt.Errorf("required metadata field %q missing", reqField)
			}
		}
	}
	return nil
}

// SequenceDeduplicator provides bounded exactly-once / idempotent message deduplication
// using explicit idempotency keys and monotonic per-partition offset watermarks.
type SequenceDeduplicator struct {
	mu           sync.Mutex
	maxKeys      int
	seenKeys     map[string]struct{}
	keyOrder     []string
	maxPartition map[int32]int64
}

// NewSequenceDeduplicator creates a bounded deduplicator holding maxKeys recent keys.
func NewSequenceDeduplicator(maxKeys int) *SequenceDeduplicator {
	if maxKeys <= 0 {
		maxKeys = 65536
	}
	return &SequenceDeduplicator{
		maxKeys:      maxKeys,
		seenKeys:     make(map[string]struct{}),
		keyOrder:     make([]string, 0, 1024),
		maxPartition: make(map[int32]int64),
	}
}

// CheckAndMark returns true if the message is fresh (not a duplicate) and records it after commit.
func (d *SequenceDeduplicator) CheckAndMark(partition int32, offset int64, idempotencyKey string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if idempotencyKey != "" {
		if _, exists := d.seenKeys[idempotencyKey]; exists {
			return false
		}
	}
	if offset > 0 {
		if prev, exists := d.maxPartition[partition]; exists && offset <= prev {
			return false
		}
		d.maxPartition[partition] = offset
	}
	if idempotencyKey != "" {
		if len(d.keyOrder) >= d.maxKeys {
			evict := d.keyOrder[0]
			d.keyOrder = d.keyOrder[1:]
			delete(d.seenKeys, evict)
		}
		d.seenKeys[idempotencyKey] = struct{}{}
		d.keyOrder = append(d.keyOrder, idempotencyKey)
	}
	return true
}

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
		if ds.Tombstones[loc.BatchIdx] == nil {
			ds.Tombstones[loc.BatchIdx] = types.NewBitset()
		}
		ds.Tombstones[loc.BatchIdx].Set(loc.RowIdx)
	}
}

func buildStreamArrowBatch(alloc memory.Allocator, events []StreamVectorEvent) (arrow.RecordBatch, error) {
	if len(events) == 0 {
		return nil, errors.New("no stream events to encode")
	}
	dim := len(events[0].Vector)
	if dim == 0 {
		dim = len(events[0].Embedding)
	}
	if dim <= 0 {
		return nil, errors.New("vector dimension must be positive")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32), Nullable: false}, // #nosec G115
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "metadata", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	idB := array.NewStringBuilder(alloc)
	defer idB.Release()
	vecListB := array.NewFixedSizeListBuilder(alloc, int32(dim), arrow.PrimitiveTypes.Float32) // #nosec G115
	defer vecListB.Release()
	vecValB := vecListB.ValueBuilder().(*array.Float32Builder)
	textB := array.NewStringBuilder(alloc)
	defer textB.Release()
	metaB := array.NewStringBuilder(alloc)
	defer metaB.Release()

	for _, ev := range events {
		vec := ev.Vector
		if len(vec) == 0 {
			vec = ev.Embedding
		}
		if len(vec) != dim {
			return nil, fmt.Errorf("inconsistent batch vector dim: got %d expected %d", len(vec), dim)
		}
		idB.Append(ev.ID)
		vecListB.Append(true)
		for _, v := range vec {
			vecValB.Append(v)
		}
		if ev.Text != "" {
			textB.Append(ev.Text)
		} else {
			textB.AppendNull()
		}
		if len(ev.Metadata) > 0 {
			raw, _ := json.Marshal(ev.Metadata)
			metaB.Append(string(raw))
		} else {
			metaB.AppendNull()
		}
	}

	idArr := idB.NewArray()
	defer idArr.Release()
	vecArr := vecListB.NewArray()
	defer vecArr.Release()
	textArr := textB.NewArray()
	defer textArr.Release()
	metaArr := metaB.NewArray()
	defer metaArr.Release()

	return array.NewRecordBatch(schema, []arrow.Array{idArr, vecArr, textArr, metaArr}, int64(len(events))), nil
}

func (c *StreamConnector) runKafkaConsumer() {
	defer c.wg.Done()
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	brokers := strings.Split(c.cfg.Brokers, ",")
	group := c.cfg.GroupID
	if group == "" {
		group = "longbow-" + c.cfg.Name
	}
	cg, err := sarama.NewConsumerGroup(brokers, group, cfg)
	if err != nil {
		c.store.logger.Error().Err(err).Str("connector", c.cfg.Name).Msg("Kafka consumer group creation failed")
		c.running.Store(false)
		return
	}
	defer func() { _ = cg.Close() }()

	handler := &kafkaStreamHandler{conn: c}
	for c.ctx.Err() == nil {
		if err := cg.Consume(c.ctx, []string{c.cfg.Topic}, handler); err != nil {
			if c.ctx.Err() != nil {
				return
			}
			c.store.logger.Warn().Err(err).Str("connector", c.cfg.Name).Msg("Kafka consume loop error, retrying")
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
}

type kafkaStreamHandler struct {
	conn *StreamConnector
}

func (h *kafkaStreamHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *kafkaStreamHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *kafkaStreamHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	batch := make([]StreamEnvelope, 0, h.conn.cfg.BatchSize)
	flushTimer := time.NewTimer(time.Duration(h.conn.cfg.FlushIntervalMs) * time.Millisecond)
	defer flushTimer.Stop()

	flush := func(lastMsg *sarama.ConsumerMessage) {
		if len(batch) == 0 {
			return
		}
		_, _, err := h.conn.ProcessEnvelopes(session.Context(), batch)
		if err == nil && lastMsg != nil {
			session.MarkMessage(lastMsg, "")
		}
		batch = batch[:0]
	}

	var lastMsg *sarama.ConsumerMessage
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				flush(lastMsg)
				return nil
			}
			hdrs := make(map[string]string, len(msg.Headers))
			for _, hdr := range msg.Headers {
				hdrs[string(hdr.Key)] = string(hdr.Value)
			}
			batch = append(batch, StreamEnvelope{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       string(msg.Key),
				Headers:   hdrs,
				Payload:   msg.Value,
				Timestamp: msg.Timestamp,
			})
			lastMsg = msg
			if len(batch) >= h.conn.cfg.BatchSize {
				flush(lastMsg)
			}
		case <-flushTimer.C:
			flush(lastMsg)
			flushTimer.Reset(time.Duration(h.conn.cfg.FlushIntervalMs) * time.Millisecond)
		case <-session.Context().Done():
			flush(lastMsg)
			return nil
		}
	}
}

func (c *StreamConnector) runPulsarConsumer() {
	defer c.wg.Done()
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               c.cfg.Brokers,
		ConnectionTimeout: 5 * time.Second,
	})
	if err != nil {
		c.store.logger.Error().Err(err).Str("connector", c.cfg.Name).Msg("Pulsar client creation failed")
		c.running.Store(false)
		return
	}
	defer client.Close()

	subName := c.cfg.GroupID
	if subName == "" {
		subName = "longbow-" + c.cfg.Name
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            c.cfg.Topic,
		SubscriptionName: subName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		c.store.logger.Error().Err(err).Str("connector", c.cfg.Name).Msg("Pulsar subscribe failed")
		c.running.Store(false)
		return
	}
	defer consumer.Close()

	for c.ctx.Err() == nil {
		msg, err := consumer.Receive(c.ctx)
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			continue
		}
		_, _, procErr := c.ProcessEnvelopes(c.ctx, []StreamEnvelope{{
			Topic:     msg.Topic(),
			Offset:    int64(len(msg.ID().Serialize())),
			Key:       msg.Key(),
			Headers:   msg.Properties(),
			Payload:   msg.Payload(),
			Timestamp: msg.PublishTime(),
		}})
		if procErr == nil {
			_ = consumer.Ack(msg)
		} else {
			consumer.Nack(msg)
		}
	}
}

// handleStreamAction serves Flight DoAction calls for Item 5 streaming connectors & schema registry.
func (s *VectorStore) handleStreamAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	switch action.Type {
	case "CreateStreamConnector":
		var req struct {
			StreamConnectorConfig
			Start bool `json:"start"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid connector json: %v", err)
		}
		conn, err := s.GetStreamManager().CreateConnector(req.StreamConnectorConfig, req.Start)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(conn.Stats())
		return true, stream.Send(&flight.Result{Body: body})

	case "StopStreamConnector":
		var req struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		if err := s.GetStreamManager().StopConnector(req.Name); err != nil {
			return true, status.Errorf(codes.NotFound, "%v", err)
		}
		return true, stream.Send(&flight.Result{Body: []byte(`{"status":"stopped"}`)})

	case "ListStreamConnectors", "GetStreamStats":
		stats := s.GetStreamManager().ListStats()
		body, _ := json.Marshal(map[string]any{"connectors": stats})
		return true, stream.Send(&flight.Result{Body: body})

	case "RegisterStreamSchema":
		var req RegisteredStreamSchema
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid schema json: %v", err)
		}
		id := s.GetStreamManager().Registry().RegisterLocal(req.Subject, req)
		body, _ := json.Marshal(map[string]any{"schema_id": id, "subject": req.Subject})
		return true, stream.Send(&flight.Result{Body: body})

	case "IngestStreamEvent":
		var req struct {
			Connector string           `json:"connector"`
			Envelopes []StreamEnvelope `json:"envelopes"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid ingest json: %v", err)
		}
		conn, ok := s.GetStreamManager().GetConnector(req.Connector)
		if !ok {
			return true, status.Errorf(codes.NotFound, "connector %q not found", req.Connector)
		}
		committed, deduped, err := conn.ProcessEnvelopes(stream.Context(), req.Envelopes)
		if err != nil {
			return true, status.Errorf(codes.Internal, "stream ingestion failed: %v", err)
		}
		body, _ := json.Marshal(map[string]any{
			"committed":    committed,
			"deduplicated": deduped,
			"stats":        conn.Stats(),
		})
		return true, stream.Send(&flight.Result{Body: body})
	}
	return false, nil
}
