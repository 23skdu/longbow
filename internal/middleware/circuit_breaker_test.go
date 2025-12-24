package middleware

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCircuitBreakerInterceptor(t *testing.T) {
	interceptor := CircuitBreakerInterceptor()

	// Mock handler that always fails
	failHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, errors.New("remote error")
	}

	// Mock handler that succeeds
	successHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	info := &grpc.UnaryServerInfo{
		FullMethod: "/arrow.flight.protocol.FlightService/DoAction",
	}

	ctx := context.Background()

	// 1. Trigger failures
	for i := 0; i < 10; i++ {
		_, err := interceptor(ctx, nil, info, failHandler)
		assert.Error(t, err)
		assert.NotEqual(t, codes.Unavailable, status.Code(err))
	}

	// 2. Next call should be blocked by circuit breaker
	_, err := interceptor(ctx, nil, info, successHandler)
	assert.Error(t, err)
	s, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Unavailable, s.Code())
	assert.Equal(t, "service circuit breaker is open", s.Message())
}

func TestCircuitBreakerInterceptor_Unprotected(t *testing.T) {
	interceptor := CircuitBreakerInterceptor()

	// Method NOT in the protected list
	info := &grpc.UnaryServerInfo{
		FullMethod: "/some.other.Service/SomeMethod",
	}

	failHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, errors.New("remote error")
	}

	// Trigger lots of failures
	for i := 0; i < 20; i++ {
		_, err := interceptor(context.Background(), nil, info, failHandler)
		assert.Error(t, err)
	}

	// Circuit should NOT be open for this method (or generally doesn't protect it)
	// But wait, the breaker is GLOBAL in the interceptor currently.
	// Actually, the current implementation uses a single 'cb' instance for ALL calls.
	// So even if the method is unprotected, it won't trigger the breaker.
	// But if it IS protected, it WILL.

	successHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	// Unprotected method should still work even if we had failures (since failures weren't on protected methods)
	res, err := interceptor(context.Background(), nil, info, successHandler)
	assert.NoError(t, err)
	assert.Equal(t, "success", res)
}
