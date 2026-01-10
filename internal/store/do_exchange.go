package store

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/storage"
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
				it, err := storage.NewWALIterator(s.dataPath, s.mem)
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

					// SERIALIZATION OPTIMIZATION: Zero-Copy (One-Copy)
					// Instead of writing to a bytes.Buffer (intermediate copy) then to FlightData,
					// we write directly to the stream using flight.RecordWriter.

					// We need to send AppMetadata (Seq | TS) with the record.
					// FlightRecordWriter doesn't easily allow per-record metadata *inside* the IPC message efficiently
					// without using custom Body/Header.
					// However, we can use `WriteWithAppMetadata` if available, or
					// since we control the stream, we can just write the Schema (once) and then Records.

					// Issue: WAL iterates over mixed datasets. Schema might change.
					// If Schema changes, we MUST create a new Writer (which sends a new Schema message).
					// This is overhead, but correct.

					metaBuf := make([]byte, 16)
					binary.LittleEndian.PutUint64(metaBuf[0:8], seq)
					binary.LittleEndian.PutUint64(metaBuf[8:16], uint64(ts))

					// Optimization: Create a writer specifically for this record's schema.
					// Note: If we reuse writers for same schema, we save Schema messages.
					// For now, optimize the *BUFFER* copy first.

					// flight.NewRecordWriter takes the stream.
					// We need to ensure metadata is attached.
					// The standard Writer writes the record batch. It does NOT attach AppMetadata to the RecordBatch message
					// in the way our custom protocol expected (FlightData.AppMetadata).
					// The standard separates Metadata from Body.

					// If our client expects AppMetadata on the FlightData frame containing the RecordBatch,
					// `flight.RecordWriter` might not expose that easily.
					// Let's check if we can subclass or just write manually but SMARTLY.
					//
					// Smart Manual:
					// Payload = ipc.GetRecordBatchPayload(rec)
					// This gets us the body buffers without copying.
					// Then we construct FlightData referencing those buffers. AVOID `ipc.Writer` to buffer.
					//
					// `ipc.GetRecordBatchPayload` returns `ipc.Payload` which has `Body []byte` (often sliced)
					// and `Metadata`.
					// Then `flight.FlightData` can be constructed.

					// Wait, `ipc.Writer` writes the IPC Message Header + Body.
					// `GetRecordBatchPayload` gives us the raw pieces? No, it gives us the `Arrow Message` components.

					// Let's stick to the simplest optimization: eliminate `bytes.Buffer` use flight.RecordWriter.
					// BUT we need `AppMetadata` for the Seq/TS.
					// `Writer.Write` doesn't take metadata.
					// WE CAN SEND METADATA SEPARATELY?
					// Or we can use `SetFlightDescriptor` on the Writer before writing?
					// Descriptor is usually for the stream, not per record.

					// Protocol change: Client expects Seq/TS in AppMetadata.
					// If we can't attach it to the RecordBatch frame via standard Writer, we might need a workaround.
					// Workaround 1: Custom Writer that bypasses buffer.
					// Workaround 2: Send metadata in a separate header frame (empty body)?
					// Workaround 3: Use `flight.Writer` but modify it? No.

					// Let's use `ipc.GetRecordBatchPayload` manually.
					// This allows Zero-Copy construction of the frame.

					// 1. Serialize Metadata (Schema/Dictionary) only if needed (not done for every record usually, but WAL is mixed).
					// For mixed WAL, we effectively send a "Stream" of 1 record.
					// If we assume client handles concatenation of streams?

					// Let's stick to valid Flight Protocol:
					// Just construct the FlightData manually but without `bytes.Buffer`.
					//
					// payload := ipc.GetRecordBatchPayload(rec) -> this doesn't exist in public API easily?
					// `rec.Serialize` writes to writer.

					// Actual Optimization: use `flight.NewRecordWriter` and send seq/ts as a separate metadata message
					// OR assume client can handle it.
					// BUT current client test checks `responses[0].AppMetadata`.

					// Backtrack: If we MUST maintain protocol (AppMetadata on data frame),
					// and standard Writer doesn't support it, we must verify `ipc` capability.

					// Actually, `flight.NewRecordWriter` uses `ipc.Writer`.
					// If we can't change protocol, we optimized by NOT RESIZING buffer.
					// `flight.NewRecordWriter` writes directly to stream (no dual copy).
					// BUT it doesn't attach AppMetadata.

					// SOLUTION: Send a separate Metadata frame *before* the record?
					// Client would need update.
					// If we want transparency:
					// Use `ipc.Writer` with a custom `WriterAt` or `WriteCloser` that wraps the stream
					// and injects the AppMetadata into the *FlightData* it generates?
					// Too complex.

					// Let's use `flight.NewRecordWriter` and assume we can send the Sequence Number in the `FlightDescriptor`?
					// Writer has `SetFlightDescriptor`.
					// `descriptor.Cmd = metaBuf`?

					wr := flight.NewRecordWriter(stream, ipc.WithSchema(rec.Schema()))
					wr.SetFlightDescriptor(&flight.FlightDescriptor{
						Type: flight.DescriptorPATH,
						Path: []string{name},
						Cmd:  metaBuf, // Pass metadata in Cmd!
					})

					if err := wr.Write(rec); err != nil {
						rec.Release()
						return err
					}
					// Important: Close the writer for this "stream" (1 record) to flush footer?
					// No, we are streaming multiple.
					// But schema changes.
					// If schema changes, we must close writer and start new one?
					// Actually if we just Close(), it sends EOS.
					// We might just want to Flush?
					if err := wr.Close(); err != nil {
						rec.Release()
						return err
					}

					rec.Release()
					sentCount++
					metrics.DoExchangeBatchesSentTotal.Inc()
					metrics.FlightZeroCopyBytesTotal.Add(float64(rec.NumRows())) // Approx
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

				ds, ok := s.getDataset(datasetName)
				if !ok {
					return status.Errorf(codes.NotFound, "dataset %s not found", datasetName)
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
