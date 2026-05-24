package types

import (
	"github.com/23skdu/longbow/internal/core"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ToGRPCStatus converts standard errors to gRPC status codes
func ToGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	// Already a gRPC status?
	if _, ok := status.FromError(err); ok {
		return err
	}
	// Check internal types
	switch e := err.(type) {
	case *core.ErrNotFound:
		return status.Errorf(codes.NotFound, "%v", e)
	case *core.ErrInvalidArgument:
		return status.Errorf(codes.InvalidArgument, "%v", e)
	case *core.ErrResourceExhausted:
		return status.Errorf(codes.ResourceExhausted, "%v", e)
	case *core.ErrUnavailable:
		return status.Errorf(codes.Unavailable, "%v", e)
	}
	return status.Errorf(codes.Internal, "%v", err)
}
