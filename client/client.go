package client

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/pkg/loadbalancing"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SmartClient is a wrapper around flight.Client that handles strict sharding redirects
type SmartClient struct {
	mu          sync.RWMutex
	primaryAddr string
	clients     map[string]flight.Client // addr -> client
	dialOpts    []grpc.DialOption
	timeout     time.Duration
	lastLoad    atomic.Pointer[loadbalancing.LoadHints]
}

// NewSmartClient creates a new smart client connected to the initial address
func NewSmartClient(addr string) (*SmartClient, error) {
	// Strip non-UDS schemes from address
	if !strings.HasPrefix(addr, "unix://") {
		addr = strings.TrimPrefix(addr, "grpc://")
		addr = strings.TrimPrefix(addr, "http://")
	} else {
		// Normalize unix://path to unix:path for gRPC dialer
		// unix:///path stays unix:///path
		if strings.HasPrefix(addr, "unix:///") {
			// already absolute
		} else {
			addr = "unix:" + strings.TrimPrefix(addr, "unix://")
		}
	}

	sc := &SmartClient{
		primaryAddr: addr,
		clients:     make(map[string]flight.Client),
		dialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(21474836470), // 20GB max (for 500k×3072×4 = 6GB payloads)
				grpc.MaxCallSendMsgSize(21474836470),
			),
		},
		timeout: 30 * time.Second,
	}

	// Warmup initial connection
	if _, err := sc.getClient(context.Background(), addr); err != nil {
		return nil, err
	}

	return sc, nil
}

// Close closes all active connections
func (c *SmartClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for _, client := range c.clients {
		if err := client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// getClient returns an existing client or creates a new one for the given address
func (c *SmartClient) getClient(_ context.Context, addr string) (flight.Client, error) {
	c.mu.RLock()
	client, ok := c.clients[addr]
	c.mu.RUnlock()
	if ok {
		return client, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double check
	if client, ok := c.clients[addr]; ok {
		return client, nil
	}

	// Create new client with Zero Copy optimizations where possible (Flight by default handles Zero Copy for DoGet)
	newClient, err := flight.NewClientWithMiddleware(addr, nil, nil, c.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	c.clients[addr] = newClient
	return newClient, nil
}

// DoGet performs a Flight DoGet with automatic redirection handling
func (c *SmartClient) DoGet(ctx context.Context, ticket []byte) (flight.FlightService_DoGetClient, error) {
	// Initial attempt
	client, err := c.getClient(ctx, c.primaryAddr)
	if err != nil {
		return nil, err
	}

	stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: ticket})
	if err != nil {
		// Handshake error check
		if forwardErr := IsForwardRequired(err); forwardErr != nil {
			// Fast path for immediate failures (rare for streams)
			return c.retryDoGet(ctx, ticket, forwardErr.TargetAddr, 1)
		}
		return nil, err
	}

	return &smartDoGetStream{
		FlightService_DoGetClient: stream,
		sc:                        c,
		ticket:                    ticket,
		ctx:                       ctx,
		stream:                    stream,
		firstRecv:                 true,
		attempt:                   1,
	}, nil
}

// retryDoGet performs the retry logic
func (c *SmartClient) retryDoGet(ctx context.Context, ticket []byte, targetAddr string, attempt int) (flight.FlightService_DoGetClient, error) {
	maxAttempts := 3
	currentAddr := targetAddr

	for attempt < maxAttempts {
		attempt++

		client, err := c.getClient(ctx, currentAddr)
		if err != nil {
			return nil, err
		}

		stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: ticket})
		if err == nil {
			return &smartDoGetStream{
				FlightService_DoGetClient: stream,
				sc:                        c,
				ticket:                    ticket,
				ctx:                       ctx,
				stream:                    stream,
				firstRecv:                 true,
				attempt:                   attempt,
			}, nil
		}

		if forwardErr := IsForwardRequired(err); forwardErr != nil {
			currentAddr = forwardErr.TargetAddr
			if currentAddr == "" {
				return nil, fmt.Errorf("redirect with empty address")
			}
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("max redirects exceeded")
}

// smartDoGetStream wraps the stream to intercept the first Recv()
type smartDoGetStream struct {
	flight.FlightService_DoGetClient
	sc        *SmartClient
	ticket    []byte
	ctx       context.Context
	stream    flight.FlightService_DoGetClient
	firstRecv bool
	attempt   int
}

func (s *smartDoGetStream) Recv() (*flight.FlightData, error) {
	data, err := s.stream.Recv()
	if err != nil && s.firstRecv {
		// Check for redirect
		if forwardErr := IsForwardRequired(err); forwardErr != nil {
			// Retry!
			newStream, retryErr := s.sc.retryDoGet(s.ctx, s.ticket, forwardErr.TargetAddr, s.attempt)
			if retryErr != nil {
				return nil, retryErr
			}
			// Update internal stream (type assertion to get the inner wrapper or just use the interface)
			// But wait, retryDoGet returns a WRAPPED stream.
			// We need the UNDERLYING stream to continue reading?
			// No, retryDoGet returns a *new* wrapper.
			// We can just forward calls to the new wrapper?
			// Actually, implementing this recursively is tricky if we just return the new stream's Recv.

			// Unbox the new wrapper to get state?
			// Or just swap `s.stream` with the new wrapper's stream?
			// No, `retryDoGet` returns a wrapper. The wrapper has the new `stream`.
			if wrapped, ok := newStream.(*smartDoGetStream); ok {
				s.stream = wrapped.stream
				s.attempt = wrapped.attempt
				s.firstRecv = true // Reset for the new stream
				return s.Recv()    // Recursive call
			}
		}
	}
	s.firstRecv = false
	return data, err
}

// DoPut performs a Flight DoPut with automatic redirection handling (best-effort)
func (c *SmartClient) DoPut(ctx context.Context, desc *flight.FlightDescriptor) (flight.FlightService_DoPutClient, error) {
	currentAddr := c.primaryAddr
	attempts := 0
	maxAttempts := 3

	for attempts < maxAttempts {
		attempts++

		client, err := c.getClient(ctx, currentAddr)
		if err != nil {
			return nil, err
		}

		stream, err := client.DoPut(ctx)
		if err != nil {
			if forwardErr := IsForwardRequired(err); forwardErr != nil {
				currentAddr = forwardErr.TargetAddr
				continue
			}
			return nil, err
		}

		return &smartDoPutStream{
			FlightService_DoPutClient: stream,
			desc:                      desc,
			firstSend:                 true,
		}, nil
	}

	return nil, fmt.Errorf("max redirects exceeded")
}

// GetFlightInfo gets flight info with redirect handling
func (c *SmartClient) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	currentAddr := c.primaryAddr
	attempts := 0
	maxAttempts := 3

	for attempts < maxAttempts {
		attempts++

		client, err := c.getClient(ctx, currentAddr)
		if err != nil {
			return nil, err
		}

		info, err := client.GetFlightInfo(ctx, desc)
		if err == nil {
			// Parse load hints from AppMetadata
			if len(info.AppMetadata) >= loadbalancing.LoadHintsSize {
				if hints, ok := loadbalancing.DeserializeLoadHints(info.AppMetadata); ok {
					c.lastLoad.Store(&hints)
				}
			}
			return info, nil
		}

		if forwardErr := IsForwardRequired(err); forwardErr != nil {
			currentAddr = forwardErr.TargetAddr
			// Check for empty addr to prevent loops if server is buggy
			if currentAddr == "" {
				return nil, fmt.Errorf("redirect with empty address")
			}
			continue
		}

		return nil, err
	}

	return nil, fmt.Errorf("max redirects exceeded")
}

// DoAction performs a Flight DoAction with automatic redirection handling
func (c *SmartClient) DoAction(ctx context.Context, action *flight.Action) (flight.FlightService_DoActionClient, error) {
	currentAddr := c.primaryAddr
	attempts := 0
	maxAttempts := 3

	for attempts < maxAttempts {
		attempts++

		client, err := c.getClient(ctx, currentAddr)
		if err != nil {
			return nil, err
		}

		stream, err := client.DoAction(ctx, action)
		if err == nil {
			return stream, nil
		}

		if forwardErr := IsForwardRequired(err); forwardErr != nil {
			currentAddr = forwardErr.TargetAddr
			if currentAddr == "" {
				return nil, fmt.Errorf("redirect with empty address")
			}
			continue
		}
		return nil, err
	}

	return nil, fmt.Errorf("max redirects exceeded")
}

type smartDoPutStream struct {
	flight.FlightService_DoPutClient
	desc      *flight.FlightDescriptor
	firstSend bool
}

func (s *smartDoPutStream) Send(data *flight.FlightData) error {
	if s.firstSend {
		s.firstSend = false
		data.FlightDescriptor = s.desc
	}
	return s.FlightService_DoPutClient.Send(data)
}

// GetLastLoadHints returns the latest load balancing hints received from the server.
func (c *SmartClient) GetLastLoadHints() *loadbalancing.LoadHints {
	return c.lastLoad.Load()
}

// StreamUploader handles streaming chunk uploads over Flight DoPut.
// It sends chunks of Arrow RecordBatches sequentially across the stream,
// avoiding huge memory spikes on either client or server.
type StreamUploader struct {
	sc     *SmartClient
	stream flight.FlightService_DoPutClient
	writer *flight.Writer
	desc   *flight.FlightDescriptor
}

// NewStreamUploader creates a stream uploader for streaming Arrow record batches/chunks into Longbow.
func (c *SmartClient) NewStreamUploader(ctx context.Context, dataset string, schema *arrow.Schema) (*StreamUploader, error) {
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{dataset},
	}
	stream, err := c.DoPut(ctx, desc)
	if err != nil {
		return nil, err
	}

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	writer.SetFlightDescriptor(desc)

	return &StreamUploader{
		sc:     c,
		stream: stream,
		writer: writer,
		desc:   desc,
	}, nil
}

// Write writes a single Arrow record batch to the upload stream.
func (u *StreamUploader) Write(record arrow.Record) error {
	return u.writer.Write(record)
}

// WriteChunked writes a record batch, automatically splitting it into smaller chunks if it exceeds maxChunkSize.
func (u *StreamUploader) WriteChunked(record arrow.Record, maxChunkSize int64) error {
	if maxChunkSize <= 0 {
		maxChunkSize = 10000
	}
	numRows := record.NumRows()
	if numRows <= maxChunkSize {
		return u.writer.Write(record)
	}
	for offset := int64(0); offset < numRows; offset += maxChunkSize {
		end := offset + maxChunkSize
		if end > numRows {
			end = numRows
		}
		chunk := record.NewSlice(offset, end)
		err := u.writer.Write(chunk)
		chunk.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the stream writer and completes the Flight DoPut exchange.
func (u *StreamUploader) Close() error {
	if err := u.writer.Close(); err != nil {
		return err
	}
	if err := u.stream.CloseSend(); err != nil {
		return err
	}
	if _, err := u.stream.Recv(); err != nil && err != io.EOF {
		return err
	}
	return nil
}

// UploadTable streams an arrow.Table in chunks (default 10,000 rows per chunk) to Longbow.
func (c *SmartClient) UploadTable(ctx context.Context, dataset string, tbl arrow.Table, chunkSize int64) error {
	if chunkSize <= 0 {
		chunkSize = 10000
	}
	uploader, err := c.NewStreamUploader(ctx, dataset, tbl.Schema())
	if err != nil {
		return err
	}
	defer uploader.Close()

	tr := array.NewTableReader(tbl, chunkSize)
	defer tr.Release()

	for tr.Next() {
		rec := tr.Record()
		if err := uploader.Write(rec); err != nil {
			return err
		}
	}
	if tr.Err() != nil {
		return tr.Err()
	}
	return uploader.Close()
}
