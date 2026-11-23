package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestGetAuditDenialCountsRBAC verifies that the aggregate denial view is an
// admin-only Flight action and reflects a failed authorization attempt.
func TestGetAuditDenialCountsRBAC(t *testing.T) {
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<28, 0, 0)
	defer func() { _ = s.Close() }()

	mgr := NewRBACManager()
	s.EnableRBAC(mgr)

	_, adminRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "audit-admin",
		Role: RoleAdmin,
	})
	require.NoError(t, err)
	_, readerRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name:      "audit-reader",
		Role:      RoleReader,
		TenantID:  "team-a",
		Namespace: "team-a",
		Datasets:  []string{"team-a/docs"},
	})
	require.NoError(t, err)

	readerCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+readerRaw))
	readerStream := &captureActionStream{ctx: readerCtx}
	err = s.DoAction(&flight.Action{Type: "GetAuditDenialCounts"}, readerStream)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Empty(t, readerStream.results)

	adminCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+adminRaw))
	adminStream := &captureActionStream{ctx: adminCtx}
	require.NoError(t, s.DoAction(&flight.Action{Type: "GetAuditDenialCounts"}, adminStream))
	require.Len(t, adminStream.results, 1)

	var resp struct {
		Counts map[string]int `json:"counts"`
		Total  int            `json:"total"`
	}
	require.NoError(t, json.Unmarshal(adminStream.results[0].Body, &resp))
	require.Equal(t, 1, resp.Counts["permission_denied|authorize:admin|rpc"])
	require.Equal(t, 1, resp.Total)
	for key := range resp.Counts {
		require.NotContains(t, key, adminRaw)
		require.NotContains(t, key, readerRaw)
	}
}
