package limiter

import (
	"context"
	"errors"

	"github.com/23skdu/longbow/internal/metrics"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config holds rate limiter configuration
type Config struct {
	RPS   int `envconfig:"RATE_LIMIT_RPS" default:"0"`   // 0 means disabled
	Burst int `envconfig:"RATE_LIMIT_BURST" default:"0"` // 0 means use RPS
}

// RateLimiter wraps the token bucket limiter
type RateLimiter struct {
	limiter *rate.Limiter
	enabled bool
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(cfg Config) *RateLimiter {
	if cfg.RPS <= 0 {
		return &RateLimiter{enabled: false}
	}
	burst := cfg.Burst
	if burst <= 0 {
		burst = cfg.RPS
	}
	return &RateLimiter{
		limiter: rate.NewLimiter(rate.Limit(cfg.RPS), burst),
		enabled: true,
	}
}

// UnaryInterceptor returns a gRPC unary interceptor
func (l *RateLimiter) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !l.enabled {
			return handler(ctx, req)
		}

		if err := l.limiter.Wait(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, status.FromContextError(err).Err()
			}
			// This happens if the wait time exceeds the context deadline
			metrics.RateLimitRequestsTotal.WithLabelValues("throttled").Inc()
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}

		metrics.RateLimitRequestsTotal.WithLabelValues("allowed").Inc()
		return handler(ctx, req)
	}
}

// StreamInterceptor returns a gRPC stream interceptor
func (l *RateLimiter) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !l.enabled {
			return handler(srv, ss)
		}

		ctx := ss.Context()
		if err := l.limiter.Wait(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return status.FromContextError(err).Err()
			}
			metrics.RateLimitRequestsTotal.WithLabelValues("throttled").Inc()
			return status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}

		metrics.RateLimitRequestsTotal.WithLabelValues("allowed").Inc()
		return handler(srv, ss)
	}
}
