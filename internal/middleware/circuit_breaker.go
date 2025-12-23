package middleware

import (
	"context"
	"time"

	"github.com/23skdu/longbow/internal/breaker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CircuitBreakerInterceptor creates a unary server interceptor that wraps `DoGet` and `VectorSearch`
// calls with a circuit breaker.
func CircuitBreakerInterceptor() grpc.UnaryServerInterceptor {
	// Configure breaker - trips after 10 consecutive failures
	config := breaker.Settings{
		Name:    "GlobalSearch",
		Timeout: 30 * time.Second, // Cooldown period
		ReadyToTrip: func(counts breaker.Counts) bool {
			return counts.ConsecutiveFailures >= 10
		},
		OnStateChange: func(name string, from breaker.State, to breaker.State) {
			// Log state change if logging available, or expose via metric
		},
	}
	cb := breaker.NewCircuitBreaker(config)

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Only protect expensive search operations
		isProtected := false
		if info.FullMethod == "/arrow.flight.protocol.FlightService/DoGet" ||
			info.FullMethod == "/arrow.flight.protocol.FlightService/DoAction" { // Simple check, could refine for specific actions
			isProtected = true
		}

		if !isProtected {
			return handler(ctx, req)
		}

		res, err := cb.Execute(func() (interface{}, error) {
			return handler(ctx, req)
		})

		if err == breaker.ErrOpenState {
			return nil, status.Error(codes.Unavailable, "service circuit breaker is open")
		}

		return res, err
	}
}
