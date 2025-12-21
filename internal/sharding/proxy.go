package sharding

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// PartitionProxyInterceptor creates a unary server interceptor for request routing
func PartitionProxyInterceptor(rm *RingManager, forwarder *RequestForwarder) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Extract routing key from metadata or request
		// Note: This requires requests to have a common GetId() or metadata extraction
		key, err := extractRoutingKey(ctx, req)
		if err != nil {
			// If no key found, assume local (or broadcast/admin op) use fallback
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
		return forwarder.Forward(ctx, owner, req, info.FullMethod)
	}
}

// extractRoutingKey extracts the partitioning key (e.g., ticket, id)
func extractRoutingKey(ctx context.Context, req interface{}) (string, error) {
	// Attempt to pull from metadata first
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-longbow-key"); len(vals) > 0 {
			return vals[0], nil
		}
	}

	// Implementation dependent: Reflection or interface assertion could be used here.
	// For now, simpler to rely on metadata or known types.
	return "", fmt.Errorf("no routing key found")
}

// RequestForwarder (Now defined in forwarder.go)
