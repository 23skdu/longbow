package sharding

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/metrics"
)

// PartitionProxyInterceptor creates a unary server interceptor for request routing
func PartitionProxyInterceptor(rm *RingManager, forwarder *RequestForwarder) grpc.UnaryServerInterceptor {
	tracer := otel.Tracer("longbow/sharding")
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, span := tracer.Start(ctx, "PartitionProxyFunc")
		defer span.End()

		// Extract routing key from metadata or request
		key, err := extractRoutingKey(ctx, req)
		if err != nil {
			// If no key found, assume local or broadcast
			return handler(ctx, req)
		}

		if rm.IsLocalKey(key) {
			return handler(ctx, req)
		}

		owner := rm.GetNode(key)
		if owner == "" {
			return nil, status.Error(codes.Unavailable, "ring is empty or uninitialized")
		}

		// Forwarding logic
		start := time.Now()
		resp, err := forwarder.Forward(ctx, owner, req, info.FullMethod)
		duration := time.Since(start).Seconds()

		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.ProxyRequestsForwardedTotal.WithLabelValues(info.FullMethod, status).Inc()
		metrics.ProxyRequestLatencySeconds.WithLabelValues(info.FullMethod).Observe(duration)

		return resp, err
	}
}

// PartitionProxyStreamInterceptor creates a stream server interceptor for request routing
func PartitionProxyStreamInterceptor(rm *RingManager, forwarder *RequestForwarder) grpc.StreamServerInterceptor {
	tracer := otel.Tracer("longbow/sharding")
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, span := tracer.Start(ss.Context(), "PartitionProxyStreamFunc")
		defer span.End()

		// For streams, we primarily rely on metadata for routing
		key, _ := extractRoutingKeyFromMetadata(ctx)
		if key == "" {
			return handler(srv, ss)
		}

		if rm.IsLocalKey(key) {
			return handler(srv, ss)
		}

		owner := rm.GetNode(key)
		if owner == "" {
			return status.Error(codes.Unavailable, "ring is empty or uninitialized")
		}

		// Forwarding logic
		return forwarder.ForwardStream(ctx, owner, ss, info.FullMethod)
	}
}

// extractRoutingKey extracts the partitioning key (e.g., ticket, id)
func extractRoutingKey(ctx context.Context, req interface{}) (string, error) {
	// 1. Metadata
	if key, err := extractRoutingKeyFromMetadata(ctx); err == nil {
		return key, nil
	}

	// 2. Type-specific extraction
	switch r := req.(type) {
	case *flight.FlightDescriptor:
		if len(r.Path) > 0 {
			return r.Path[0], nil
		}
		if len(r.Cmd) > 0 {
			var cmd map[string]interface{}
			if err := json.Unmarshal(r.Cmd, &cmd); err == nil {
				if name, ok := cmd["name"].(string); ok {
					return name, nil
				}
				if ds, ok := cmd["dataset"].(string); ok {
					return ds, nil
				}
			}
		}
	case *flight.Ticket:
		// Many Tickets are just dataset names or JSON with dataset names
		tStr := string(r.Ticket)
		if !strings.HasPrefix(tStr, "{") {
			return tStr, nil
		}
		var ticketData map[string]interface{}
		if err := json.Unmarshal(r.Ticket, &ticketData); err == nil {
			if name, ok := ticketData["name"].(string); ok {
				return name, nil
			}
			if ds, ok := ticketData["dataset"].(string); ok {
				return ds, nil
			}
		}
	}

	return "", fmt.Errorf("no routing key found")
}

func extractRoutingKeyFromMetadata(ctx context.Context) (string, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-longbow-key"); len(vals) > 0 {
			return vals[0], nil
		}
	}
	return "", fmt.Errorf("not found in metadata")
}
