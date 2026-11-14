package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
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

func TestKafkaBrokerAndTestcontainersConnectorIntegration(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = t.TempDir()
	vs.StartIngestionWorkers(2)
	vs.StartIndexingWorkers(2)
	defer func() { _ = vs.Close() }()

	topic := "vectors.cdc.v1"
	brokerAddr := os.Getenv("LONGBOW_TESTCONTAINERS_KAFKA")
	var mockBroker *sarama.MockBroker
	if brokerAddr == "" {
		mockBroker = sarama.NewMockBroker(t, 1)
		defer mockBroker.Close()
		brokerAddr = mockBroker.Addr()

		evBytes, _ := json.Marshal(StreamVectorEvent{
			ID:             "kafka-vec-1",
			Vector:         []float32{0.5, 0.5, 0.5, 0.5},
			Text:           "kafka broker integration",
			IdempotencyKey: "k-tcp-1",
		})
		metaResp := sarama.NewMockMetadataResponse(t).
			SetBroker(mockBroker.Addr(), mockBroker.BrokerID()).
			SetLeader(topic, 0, mockBroker.BrokerID())
		prodSuccess := sarama.NewMockProduceResponse(t).
			SetError(topic, 0, sarama.ErrNoError)
		offsetResp := sarama.NewMockOffsetResponse(t).
			SetOffset(topic, 0, sarama.OffsetOldest, 0).
			SetOffset(topic, 0, sarama.OffsetNewest, 1)
		fetchResp := sarama.NewMockFetchResponse(t, 1).
			SetMessage(topic, 0, 0, sarama.ByteEncoder(evBytes))

		mockBroker.SetHandlerByMap(map[string]sarama.MockResponse{
			"MetadataRequest": metaResp,
			"ProduceRequest":  prodSuccess,
			"OffsetRequest":   offsetResp,
			"FetchRequest":    fetchResp,
		})
	}

	// Verify real TCP Kafka Producer + Consumer over the broker endpoint
	prodCfg := sarama.NewConfig()
	prodCfg.Version = sarama.V0_10_0_0
	prodCfg.ApiVersionsRequest = false
	prodCfg.Metadata.Full = false
	prodCfg.Producer.Return.Successes = true
	prodCfg.Metadata.Retry.Max = 1
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, prodCfg)
	require.NoError(t, err)
	defer func() { _ = producer.Close() }()

	msgPayload, _ := json.Marshal(StreamVectorEvent{
		ID:             "kafka-vec-1",
		Vector:         []float32{0.5, 0.5, 0.5, 0.5},
		Text:           "kafka broker integration",
		IdempotencyKey: "k-tcp-1",
	})
	part, off, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder("kafka-vec-1"),
		Value: sarama.ByteEncoder(msgPayload),
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), part)
	require.GreaterOrEqual(t, off, int64(0))

	consCfg := sarama.NewConfig()
	consCfg.Version = sarama.V0_10_0_0
	consCfg.ApiVersionsRequest = false
	consCfg.Metadata.Full = false
	consCfg.Metadata.Retry.Max = 1
	consumer, err := sarama.NewConsumer([]string{brokerAddr}, consCfg)
	require.NoError(t, err)
	defer func() { _ = consumer.Close() }()

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	require.NoError(t, err)
	defer func() { _ = pc.Close() }()

	conn, err := vs.GetStreamManager().CreateConnector(StreamConnectorConfig{
		Name:        "kafka-tcp-conn",
		Dataset:     "kafka_vectors",
		SourceType:  StreamSourceKafka,
		Brokers:     brokerAddr,
		Topic:       topic,
		ExpectedDim: 4,
		ExactlyOnce: true,
	}, false)
	require.NoError(t, err)

	select {
	case msg := <-pc.Messages():
		committed, deduped, err := conn.ProcessEnvelopes(context.Background(), []StreamEnvelope{{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:       string(msg.Key),
			Payload:   msg.Value,
		}})
		require.NoError(t, err)
		require.Equal(t, 1, committed)
		require.Equal(t, 0, deduped)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message from Kafka TCP broker")
	}

	require.Eventually(t, func() bool {
		ds, ok := vs.getDataset("kafka_vectors")
		return ok && ds.IndexLen() >= 1
	}, 2*time.Second, 20*time.Millisecond)
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
