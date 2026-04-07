package middleware

import (
	"context"
	"strings"

	"github.com/23skdu/longbow/internal/store"
	"google.golang.org/grpc"
)

// AdmissionInterceptor returns a gRPC unary interceptor that enforces admission control.
func AdmissionInterceptor(admission *store.AdmissionController) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if admission == nil {
			return handler(ctx, req)
		}

		opType := "search"
		if strings.Contains(info.FullMethod, "DoPut") || strings.Contains(info.FullMethod, "Put") {
			opType = "ingest"
		} else if strings.Contains(info.FullMethod, "Admin") {
			opType = "maintenance"
		}

		if err := admission.Admit(ctx, opType); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// AdmissionStreamInterceptor returns a gRPC stream interceptor that enforces admission control.
func AdmissionStreamInterceptor(admission *store.AdmissionController) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if admission == nil {
			return handler(srv, ss)
		}

		opType := "search"
		if strings.Contains(info.FullMethod, "DoPut") || strings.Contains(info.FullMethod, "Put") {
			opType = "ingest"
		} else if strings.Contains(info.FullMethod, "Admin") {
			opType = "maintenance"
		}

		if err := admission.Admit(ss.Context(), opType); err != nil {
			return err
		}

		return handler(srv, ss)
	}
}
