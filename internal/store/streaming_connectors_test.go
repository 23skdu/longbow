package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestStreamingConnectorSchemaDedupDLQAndIngest(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = t.TempDir()
	vs.StartIngestionWorkers(2)
	vs.StartIndexingWorkers(2)
	defer func() { _ = vs.Close() }()

	// Spin up mock Confluent-style HTTP schema registry
	registrySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":      7,
			"version": 1,
			"schema":  `{"expected_dim":4,"required":["id","vector","source"]}`,
		})
	}))
	defer registrySrv.Close()

	mgr := vs.GetStreamManager()
	conn, err := mgr.CreateConnector(StreamConnectorConfig{
		Name:              "orders-stream",
		Dataset:           "tenant_a/orders",
		SourceType:        StreamSourceMemory,
		Topic:             "orders.v1",
		SchemaRegistryURL: registrySrv.URL,
		SchemaSubject:     "orders-value",
		ExpectedDim:       4,
		ExactlyOnce:       true,
	}, true)
	require.NoError(t, err)

	ctx := context.Background()

	// 1. Valid wire-framed event matching schema ID 7
	validPayload, _ := json.Marshal(StreamVectorEvent{
		ID:             "doc-1",
		Vector:         []float32{1, 0, 0, 0},
		Text:           "hello world",
		Metadata:       map[string]any{"source": "kafka"},
		IdempotencyKey: "k-1",
	})
	wireValid := EncodeWirePayload(7, validPayload)

	// 2. Duplicate delivery (same offset and idempotency key) -> deduplicated
	// 3. Invalid dimension -> sent to DLQ
	badDimPayload, _ := json.Marshal(StreamVectorEvent{
		ID:       "doc-bad-dim",
		Vector:   []float32{1, 2}, // wrong dim
		Metadata: map[string]any{"source": "kafka"},
	})
	// 4. Missing required metadata field -> sent to DLQ
	missingMetaPayload, _ := json.Marshal(StreamVectorEvent{
		ID:     "doc-bad-meta",
		Vector: []float32{0, 1, 0, 0},
	})

	committed, deduped, err := conn.ProcessEnvelopes(ctx, []StreamEnvelope{
		{Topic: "orders.v1", Partition: 0, Offset: 100, Payload: wireValid},
		{Topic: "orders.v1", Partition: 0, Offset: 100, Payload: wireValid},
		{Topic: "orders.v1", Partition: 0, Offset: 101, Payload: EncodeWirePayload(7, badDimPayload)},
		{Topic: "orders.v1", Partition: 0, Offset: 102, Payload: EncodeWirePayload(7, missingMetaPayload)},
		{Topic: "orders.v1", Partition: 0, Offset: 103, Payload: []byte("{corrupt")},
	})
	require.NoError(t, err)
	require.Equal(t, 1, committed)
	require.Equal(t, 1, deduped)

	dlq := conn.DLQEntries()
	require.Len(t, dlq, 3)

	stats := conn.Stats()
	require.True(t, stats.Running)
	require.Equal(t, int64(5), stats.Consumed)
	require.Equal(t, int64(1), stats.Committed)
	require.Equal(t, int64(1), stats.Deduplicated)
	require.Equal(t, int64(3), stats.DLQCount)
	require.Equal(t, int64(103), stats.CommittedOffset)
	require.Equal(t, int64(0), stats.ConsumerLag)

	require.Eventually(t, func() bool {
		ds, ok := vs.getDataset("tenant_a/orders")
		if !ok {
			return false
		}
		return ds.IndexLen() >= 1
	}, 2*time.Second, 20*time.Millisecond)

	require.NoError(t, mgr.StopConnector("orders-stream"))
}

func FuzzStreamWireAndEventParse(f *testing.F) {
	f.Add(EncodeWirePayload(1, []byte(`{"id":"1","vector":[0.1,0.2]}`)))
	f.Add([]byte{0x00, 0x00, 0x01})
	f.Add([]byte(`{"op":"delete","id":"x"}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		schemaID, hasHeader, payload, err := DecodeWirePayload(data)
		if err != nil {
			return
		}
		var ev StreamVectorEvent
		if json.Unmarshal(payload, &ev) == nil {
			_ = ValidateStreamEvent(RegisteredStreamSchema{ID: schemaID}, &ev)
		}
		if hasHeader && schemaID > 0 {
			reencoded := EncodeWirePayload(schemaID, payload)
			gotID, gotHdr, _, err := DecodeWirePayload(reencoded)
			if err != nil || !gotHdr || gotID != schemaID {
				t.Fatalf("roundtrip mismatch")
			}
		}
	})
}
