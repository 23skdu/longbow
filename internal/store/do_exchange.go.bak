package store

import (
	"io"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"go.uber.org/zap"
)

// DoExchange implements bidirectional Arrow Flight streaming for mesh replication.
// This provides the foundation for peer-to-peer Arrow stream transfer.
//
// Protocol:
// - Client sends FlightData messages with FlightDescriptor indicating dataset
// - Server receives, processes, and can send data back
// - Enables future mesh replication between peers
func (s *DataServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	start := time.Now()

	// Track operation
	metrics.DoExchangeCallsTotal.Inc()

	s.logger.Info("DoExchange started")

	var receivedCount int64
	var sentCount int64
	var lastDescriptor *flight.FlightDescriptor

	// Process incoming stream
	for {
		// Check context cancellation
		select {
		case <-stream.Context().Done():
			s.logger.Info("DoExchange cancelled", zap.Int64("received", receivedCount))
			metrics.DoExchangeErrorsTotal.Inc()
			return stream.Context().Err()
		default:
		}

		// Receive next FlightData
		data, err := stream.Recv()
		if err == io.EOF {
			// Client finished sending
			break
		}
		if err != nil {
			s.logger.Error("DoExchange recv error", zap.Error(err))
			metrics.DoExchangeErrorsTotal.Inc()
			return err
		}

		receivedCount++
		metrics.DoExchangeBatchesReceivedTotal.Inc()

		// Track descriptor for routing
		if data.FlightDescriptor != nil {
			lastDescriptor = data.FlightDescriptor
		}

		datasetName := "default"
		if lastDescriptor != nil && len(lastDescriptor.Path) > 0 {
			datasetName = lastDescriptor.Path[0]
		}

		s.logger.Debug("DoExchange received data",
			zap.String("dataset", datasetName),
			zap.Int("body_len", len(data.DataBody)),
			zap.Int64("count", receivedCount))

		// Check for sync command - foundation for mesh replication
		if lastDescriptor != nil && len(lastDescriptor.Cmd) > 0 {
			cmd := string(lastDescriptor.Cmd)
			if cmd == "sync" || cmd == "fetch" {
				// Send existing data back to requester
				if ds, ok := s.vectors.Get(datasetName); ok {
					for _, rec := range ds.Records {
						_ = rec // Would serialize and send in full implementation
					}
				}
			}
		}

		// Send acknowledgment back (foundation for bidirectional transfer)
		ack := &flight.FlightData{
			FlightDescriptor: lastDescriptor,
			DataBody:         []byte("ack"),
		}
		if err := stream.Send(ack); err != nil {
			s.logger.Error("DoExchange send error", zap.Error(err))
			metrics.DoExchangeErrorsTotal.Inc()
			return err
		}
		sentCount++
		metrics.DoExchangeBatchesSentTotal.Inc()
	}

	// Record duration
	duration := time.Since(start).Seconds()
	metrics.DoExchangeDurationSeconds.Observe(duration)

	s.logger.Info("DoExchange completed",
		zap.Int64("received", receivedCount),
		zap.Int64("sent", sentCount),
		zap.Float64("duration_s", duration))

	return nil
}
