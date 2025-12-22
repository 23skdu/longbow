package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
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
}

// NewSmartClient creates a new smart client connected to the initial address
func NewSmartClient(addr string) (*SmartClient, error) {
	sc := &SmartClient{
		primaryAddr: addr,
		clients:     make(map[string]flight.Client),
		dialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(1024*1024*100), // 100MB
				grpc.MaxCallSendMsgSize(1024*1024*100),
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
func (c *SmartClient) getClient(ctx context.Context, addr string) (flight.Client, error) {
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
	currentAddr := c.primaryAddr
	attempts := 0
	maxAttempts := 3

	for attempts < maxAttempts {
		attempts++

		client, err := c.getClient(ctx, currentAddr)
		if err != nil {
			return nil, err
		}

		stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: ticket})
		if err == nil {
			return stream, nil
		}

		// Check for forward error
		if forwardErr := IsForwardRequired(err); forwardErr != nil {
			// Update target and retry
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

		return stream, nil
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
