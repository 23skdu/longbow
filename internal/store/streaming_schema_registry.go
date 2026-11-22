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
	"time"
)

// ConfluentWireMagic is the standard 1-byte Confluent Schema Registry framing magic byte.
const ConfluentWireMagic byte = 0x00

// StreamSourceType defines supported streaming ingress sources.
type StreamSourceType string

const (
	// StreamSourceKafka is a stream source kafka constant.
	StreamSourceKafka StreamSourceType = "kafka"
	// StreamSourcePulsar is a stream source pulsar constant.
	StreamSourcePulsar StreamSourceType = "pulsar"
	// StreamSourceMemory is a stream source memory constant.
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

// SchemaRegistryClient configures a bidirectional streaming ingestion + optional CDC export pipeline.
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

// ValidateStreamEvent checks an event against the resolved schema rules.
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
