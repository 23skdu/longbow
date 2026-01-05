package store

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DoExchange implements bidirectional Arrow Flight streaming for mesh replication.
// This provides the foundation for peer-to-peer Arrow stream transfer.
//
// Protocol:
// - Client sends FlightData messages with FlightDescriptor indicating dataset
// - Server receives, processes, and can send data back
// - Enables future mesh replication between peers
func (s *VectorStore) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	start := time.Now()

	// Track operation
	metrics.DoExchangeCallsTotal.Inc()

	s.logger.Info().Msg("DoExchange started")

	var receivedCount int64
	var sentCount int64
	var lastDescriptor *flight.FlightDescriptor

	// Process incoming stream
	for {
		// Check context cancellation
		select {
		case <-stream.Context().Done():
			s.logger.Info().Int64("received", receivedCount).Msg("DoExchange cancelled")
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
			s.logger.Error().Err(err).Msg("DoExchange recv error")
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

		s.logger.Debug().
			Str("dataset", datasetName).
			Int("body_len", len(data.DataBody)).
			Int64("count", receivedCount).
			Msg("DoExchange received data")

		// Check for sync command - foundation for mesh replication
		if lastDescriptor != nil && len(lastDescriptor.Cmd) > 0 {
			cmd := string(lastDescriptor.Cmd)
			if cmd == "sync" {
				// Parse last sequence
				if len(data.DataBody) < 8 {
					return status.Errorf(codes.InvalidArgument, "sync command requires 8-byte sequence number")
				}
				var lastSeq uint64
				if len(data.DataBody) >= 8 {
					lastSeq = binary.LittleEndian.Uint64(data.DataBody[0:8])
				}

				s.logger.Info().Uint64("last_seq", lastSeq).Msg("Starting delta sync")

				// Create Iterator
				it, err := NewWALIterator(s.dataPath, s.mem)
				if err != nil {
					s.logger.Error().Err(err).Msg("Failed to create WAL iterator")
					return err
				}

				if err := it.Seek(lastSeq); err != nil {
					s.logger.Error().Err(err).Msg("Failed to seek WAL")
					return err
				}

				// Stream deltas
				for {
					seq, ts, name, rec, err := it.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						s.logger.Error().Err(err).Msg("WAL read error")
						return err
					}

					// Serialize record using existing helper from DoGet or manually
					// We need to send FlightData.
					// Use flight.Writer? No, directly constructing FlightData is complex for Records (header+body).
					// Better to use `flight.NewRecordWriter(stream)`.
					// But we are sharing the stream? `DoExchange` is bidirectional.
					// Can we create a Writer on the stream?

					// Yes: `w := flight.NewRecordWriter(stream, ipc.WithSchema(rec.Schema()))`.
					// BUT schema might change between records in WAL (different datasets!).
					// Flight stream usually expects CONSTANT schema.
					// If strictly streaming ONE dataset, fine.
					// But WAL has mixed datasets.
					// Mixed schema stream is tricky in Arrow Flight (requires dictionary batches etc or schema messages).
					// Workaround: Send purely raw bytes as `FlightData.Body` if client can parse?
					// Or simpler: Only sync ONE dataset? The plan implied generic sync.
					// "Delta transfer".

					// If we mix schemas, `flight.NewRecordWriter` will complain or we need to recreate it for each record if schema differs.
					// Recreating writer sends Schema message each time. That effectively works as concatenation of valid Arrow streams.
					// Let's try that.
					// Send Header: Metadata with Seq, Name.

					// To transmit metadata (Name, Seq) we can use FlightDescriptor on the write.
					// `w.SetFlightDescriptor(...)` before writing? `RecordWriter` abstraction might hide this.
					// Actually `DoExchange` stream allows raw `Send(*FlightData)`.
					// We can serialize record to bytes (what WAL has) and wrap in FlightData.
					// Client (Peer) needs to deserialize.

					// Let's serialize manually to ensure controlling metadata.
					var buf bytes.Buffer
					writer := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
					if err := writer.Write(rec); err != nil {
						rec.Release()
						return err
					}
					if err := writer.Close(); err != nil {
						rec.Release()
						return err
					}

					// Construct FlightData
					// We send the whole IPC Payload (Schema + Record) in one blob?
					// Or valid IPC stream.
					// Let's send raw bytes of IPC stream.

					metaBuf := make([]byte, 16)
					binary.LittleEndian.PutUint64(metaBuf[0:8], seq)
					binary.LittleEndian.PutUint64(metaBuf[8:16], uint64(ts))

					// Using explicit FlightData construction
					fd := &flight.FlightData{
						FlightDescriptor: &flight.FlightDescriptor{
							Type: flight.DescriptorPATH,
							Path: []string{name},
						},
						AppMetadata: metaBuf, // Pass consistency tokens (Seq | TS)
						DataBody:    buf.Bytes(),
					}

					if err := stream.Send(fd); err != nil {
						rec.Release()
						return err
					}
					rec.Release() // Release after send

					sentCount++
					metrics.DoExchangeBatchesSentTotal.Inc()
				}
				s.logger.Info().
					Int64("count", sentCount).
					Uint64("last_seq", lastSeq).
					Msg("Sent deltas to peer")

				// Make sure to close the iterator
				_ = it.Close()
				// Send "done" or just end? Client reads until EOF?
				// Client triggered it.
			} else if cmd == "merkle_node" {
				pathLen := len(data.DataBody) / 4
				path := make([]int, pathLen)
				for i := 0; i < pathLen; i++ {
					path[i] = int(binary.LittleEndian.Uint32(data.DataBody[i*4 : (i+1)*4]))
				}

				ds, err := s.getDataset(datasetName)
				if err != nil {
					return err
				}

				hash, children, ok := ds.Merkle.GetNode(path)
				if !ok {
					return status.Errorf(codes.NotFound, "Merkle node not found")
				}

				// Respond with hashes: ParentHash(32) | Child1Hash(32) | ... | Child16Hash(32)
				resBuf := make([]byte, 32*(1+len(children)))
				copy(resBuf[0:32], hash[:])
				for i, child := range children {
					copy(resBuf[32+i*32:32+(i+1)*32], child[:])
				}

				if err := stream.Send(&flight.FlightData{
					DataBody: resBuf,
				}); err != nil {
					return err
				}
				continue // Skip the final ack for this command
			}
		}

		// Send acknowledgment back (foundation for bidirectional transfer)
		ack := &flight.FlightData{
			FlightDescriptor: lastDescriptor,
			DataBody:         []byte("ack"),
		}
		if err := stream.Send(ack); err != nil {
			s.logger.Error().Err(err).Msg("DoExchange send error")
			metrics.DoExchangeErrorsTotal.Inc()
			return err
		}
		sentCount++
		metrics.DoExchangeBatchesSentTotal.Inc()
	}

	// Record duration
	duration := time.Since(start).Seconds()
	metrics.DoExchangeDurationSeconds.Observe(duration)

	s.logger.Info().
		Int64("received", receivedCount).
		Int64("sent", sentCount).
		Float64("duration_s", duration).
		Msg("DoExchange completed")

	return nil
}
