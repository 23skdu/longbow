package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/pulsar-client-go/pulsar"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

// Setup sets the up.
func (h *kafkaStreamHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup runs the cleanup operation.
func (h *kafkaStreamHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim runs the consume claim operation.
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
