package client

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MockFlightServer mocks a Flight service
type MockFlightServer struct {
	flight.BaseFlightServer
	addr        string
	shouldError bool
	targetAddr  string
	callCount   atomic.Int32
}

func (s *MockFlightServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	s.callCount.Add(1)
	if s.shouldError {
		return fmt.Errorf("FORWARD_REQUIRED: target=node2 addr=%s", s.targetAddr)
	}
	// Simulate zero-copy response (noop for test)
	return nil
}

func (s *MockFlightServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	s.callCount.Add(1)
	if s.shouldError {
		return nil, fmt.Errorf("FORWARD_REQUIRED: target=node2 addr=%s", s.targetAddr)
	}
	return &flight.FlightInfo{}, nil
}

func startMockServer(t *testing.T) (*MockFlightServer, *grpc.Server, net.Listener) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	mock := &MockFlightServer{addr: lis.Addr().String()}
	flight.RegisterFlightServiceServer(s, mock)

	go func() {
		_ = s.Serve(lis)
	}()

	// Wait a tiny bit for server to be ready? Usually Listen is enough.
	time.Sleep(10 * time.Millisecond)
	return mock, s, lis
}

func TestSmartClient_Redirection(t *testing.T) {
	// Setup Node 1 (Redirects)
	mock1, s1, _ := startMockServer(t)
	defer s1.Stop()

	// Setup Node 2 (Success)
	mock2, s2, _ := startMockServer(t)
	defer s2.Stop()

	// Configure mock1 to redirect to mock2
	mock1.shouldError = true
	mock1.targetAddr = mock2.addr

	// Create Smart Client pointing to Node 1
	// Client will connect to mock1
	client, err := NewSmartClient(mock1.addr)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	// 1. Test DoGet Redirection
	t.Run("DoGet Redirection", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stream, err := client.DoGet(ctx, []byte("ticket"))
		require.NoError(t, err)
		require.NotNil(t, stream)

		// Consume stream to trigger redirection logic (Recv() handles the error)
		_, err = stream.Recv()
		// Mock2 returns nil error (EOF-ish) or empty data.
		// If it returns nil error in handler, Recv returns io.EOF?
		// Mock handler returns nil. gRPC stream Recv returns EOF.
		// Flight Recv returns (*FlightData, error).
		// We expect no error or EOF.
		if err != nil {
			// If it's EOF, that's fine (success).
			// But wait, io.EOF is an error.
			// flight.Recv returns (data, err).
			// We check that it is NOT a ForwardRequired error.
			assert.NotContains(t, err.Error(), "FORWARD_REQUIRED")
		}

		// Verify call counts
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, int32(1), mock1.callCount.Load(), "Node 1 should be called once (redirect)")
		assert.Equal(t, int32(1), mock2.callCount.Load(), "Node 2 should be called once (success)")
	})

	// Reset counters
	mock1.callCount.Store(0)
	mock2.callCount.Store(0)

	// 2. Test GetFlightInfo Redirection
	t.Run("GetFlightInfo Redirection", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		info, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{})
		require.NoError(t, err)
		require.NotNil(t, info)

		// Verify call counts
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, int32(1), mock1.callCount.Load(), "Node 1 should be called once")
		assert.Equal(t, int32(1), mock2.callCount.Load(), "Node 2 should be called once")
	})
}

func TestSmartClient_MaxRedirects(t *testing.T) {
	// Setup Node 1 (Redirects loop)
	mock1, s1, _ := startMockServer(t)
	defer s1.Stop()

	mock1.shouldError = true
	mock1.targetAddr = mock1.addr // Points to self (loop)

	client, err := NewSmartClient(mock1.addr)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.DoGet(ctx, []byte("ticket"))
	require.NoError(t, err) // DoGet itself returns stream

	_, err = stream.Recv()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max redirects exceeded")
}
