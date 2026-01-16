package store

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	lmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// peekedStream wraps the DoExchange stream to replay the first message
// so that NewRecordReader can consume the schema from the first message.
type peekedStream struct {
	flight.FlightService_DoExchangeServer
	firstMsg *flight.FlightData
	used     bool
}

func (p *peekedStream) Recv() (*flight.FlightData, error) {
	if !p.used {
		p.used = true
		return p.firstMsg, nil
	}
	return p.FlightService_DoExchangeServer.Recv()
}

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

	// Parse first message to determine protocol
	firstMsg, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		s.logger.Error().Err(err).Msg("DoExchange recv error")
		return err
	}

	// Route based on descriptor
	if firstMsg.FlightDescriptor != nil {
		// 1. Vector Search
		if string(firstMsg.FlightDescriptor.Cmd) == "VectorSearch" {
			return s.handleVectorSearchExchange(stream, firstMsg)
		}

		// 2. Ingest (Zero-Copy)
		// Protocol: Path=["ingest", dataset_name]
		if len(firstMsg.FlightDescriptor.Path) > 0 && firstMsg.FlightDescriptor.Path[0] == "ingest" {
			if len(firstMsg.FlightDescriptor.Path) < 2 {
				return status.Error(codes.InvalidArgument, "DoExchange ingest requires dataset name in path")
			}
			datasetName := firstMsg.FlightDescriptor.Path[1]

			// Use peeked stream to allow RecordReader to see the schema (first msg)
			wrappedStream := &peekedStream{
				FlightService_DoExchangeServer: stream,
				firstMsg:                       firstMsg,
			}
			return s.handleDoExchangeIngest(datasetName, wrappedStream)
		}
	}

	// Otherwise, assume standard replication/ingest protocol (Legacy/Replication)
	// Reuse loop, injecting firstMsg
	receivedCount = 1
	metrics.DoExchangeBatchesReceivedTotal.Inc()

	// Initial processing of first message
	if firstMsg.FlightDescriptor != nil {
		lastDescriptor = firstMsg.FlightDescriptor
	}

	data := firstMsg
	isFirst := true

	// Process incoming stream
	for {
		if !isFirst {
			var err error
			data, err = stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				s.logger.Error().Err(err).Msg("DoExchange recv error")
				metrics.DoExchangeErrorsTotal.Inc()
				return err
			}
			receivedCount++
			metrics.DoExchangeBatchesReceivedTotal.Inc()
		}
		isFirst = false

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
			switch cmd := string(lastDescriptor.Cmd); cmd {
			case "sync":
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
					seq, ts, name, recBytes, err := it.NextRaw()
					if err == io.EOF {
						break
					}
					if err != nil {
						s.logger.Error().Err(err).Msg("WAL read error")
						return err
					}

					metaBuf := make([]byte, 16)
					binary.LittleEndian.PutUint64(metaBuf[0:8], seq)
					binary.LittleEndian.PutUint64(metaBuf[8:16], uint64(ts))

					// Send as a single FlightData packet to ensure metadata is attached
					// and to avoid IPC stream overhead for single records.
					if err := stream.Send(&flight.FlightData{
						FlightDescriptor: &flight.FlightDescriptor{
							Type: flight.DescriptorPATH,
							Path: []string{name},
						},
						AppMetadata: metaBuf,
						DataBody:    recBytes,
					}); err != nil {
						return err
					}

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
			case "merkle_node":
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
			case "VectorSearch":
				// Fallback if checked late
				return s.handleVectorSearchExchange(stream, data)
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

// handleDoExchangeIngest processes valid record batches and sends ACKs back.
func (s *VectorStore) handleDoExchangeIngest(
	name string,
	readerSource interface {
		Recv() (*flight.FlightData, error)
		Send(*flight.FlightData) error
	},
) error {
	trackAlloc := lmem.NewTrackingAllocator(arrowmem.DefaultAllocator)
	r, err := flight.NewRecordReader(readerSource, ipc.WithAllocator(trackAlloc))
	if err != nil {
		s.logger.Error().Err(err).Msg("DoExchange ingest failed to create reader")
		return err
	}
	defer r.Release()

	s.PrewarmDataset(name, r.Schema())
	s.logger.Info().Str("dataset", name).Msg("DoExchange Ingest Started")

	// Create/Get Dataset logic (mirroring DoPut)
	ds, created := s.getOrCreateDataset(name, func() *Dataset {
		ds := NewDataset(name, r.Schema())
		ds.Topo = s.numaTopology
		if s.datasetInitHook != nil {
			s.datasetInitHook(ds)
		}

		// Disk vector store support
		if strings.HasPrefix(name, "test_disk") || os.Getenv("LONGBOW_USE_DISK") == "1" {
			path := filepath.Join(s.dataPath, name+"_vectors.bin")
			dim := 0
			for _, f := range r.Schema().Fields() {
				if f.Name == "vector" {
					if fst, ok := f.Type.(*arrow.FixedSizeListType); ok {
						dim = int(fst.Len())
						break
					}
				}
			}
			if dim > 0 {
				dvs, err := NewDiskVectorStore(path, dim)
				if err != nil {
					s.logger.Error().Err(err).Msg("Failed to create DiskVectorStore")
				} else {
					ds.DiskStore = dvs
				}
			}
		}

		if ds.Index != nil {
			ds.IndexMemoryBytes.Store(ds.Index.EstimateMemory())
		}
		return ds
	})

	if ds == nil {
		return status.Errorf(codes.Internal, "failed to get/create dataset %s", name)
	}
	if created {
		s.currentMemory.Add(ds.IndexMemoryBytes.Load())
	}

	batchID := 0
	for r.Next() {
		rec := r.RecordBatch()

		// Ingest
		if err := s.flushPutBatch(ds, []arrow.RecordBatch{rec}); err != nil {
			return status.Errorf(codes.Internal, "flush failed: %v", err)
		}

		batchID++
		// Send ACK back to client (required by protocol and tests)
		ack := map[string]interface{}{
			"status":   "acked",
			"batch_id": batchID,
			"rows":     rec.NumRows(),
		}
		ackBuf, _ := json.Marshal(ack)
		if err := readerSource.Send(&flight.FlightData{
			AppMetadata: ackBuf,
		}); err != nil {
			s.logger.Error().Err(err).Msg("Failed to send DoExchange ingestion ACK")
			return err
		}
	}

	if r.Err() != nil {
		s.logger.Error().Err(r.Err()).Msg("DoExchange ingest stream error")
		return r.Err()
	}
	s.logger.Info().Str("dataset", name).Int("batches", batchID).Msg("DoExchange Ingest Completed")
	return nil
}
