package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/security"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type captureActionStream struct {
	flight.FlightService_DoActionServer
	ctx     context.Context
	results []*flight.Result
}

func (c *captureActionStream) Context() context.Context { return c.ctx }
func (c *captureActionStream) Send(r *flight.Result) error {
	c.results = append(c.results, r)
	return nil
}

func TestRBACRolesRotationTenantIsolationAndRLS(t *testing.T) {
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<28, 0, 0)
	defer func() { _ = s.Close() }()

	mgr := NewRBACManager()
	s.EnableRBAC(mgr)

	adminKey, adminRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name: "root-admin",
		Role: RoleAdmin,
	})
	require.NoError(t, err)
	require.NotEmpty(t, adminKey.KeyID)

	_, tenantAAdminRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name:      "tA-admin",
		Role:      RoleTenantAdmin,
		TenantID:  "tenantA",
		Namespace: "tenantA",
	})
	require.NoError(t, err)

	_, tenantAWriterRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name:      "tA-writer",
		Role:      RoleWriter,
		TenantID:  "tenantA",
		Namespace: "tenantA",
		Datasets:  []string{"tenantA/docs"},
	})
	require.NoError(t, err)

	_, tenantBReaderRaw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
		Name:       "tB-reader",
		Role:       RoleReader,
		TenantID:   "tenantB",
		Namespace:  "tenantB",
		RowFilters: map[string]string{"env": "prod"},
	})
	require.NoError(t, err)

	// 1. Permission boundary checks
	ok, err := mgr.CheckPermission(tenantAWriterRaw, PermIngest, "tenantA", "tenantA/docs")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = mgr.CheckPermission(tenantAWriterRaw, PermManageKeys, "tenantA", "tenantA/docs")
	require.Error(t, err)
	require.False(t, ok)

	// Cross-tenant leakage prevention: tenantA key cannot read tenantB!
	_, err = mgr.AuthorizeToken(context.Background(), tenantAWriterRaw, string(PermRead), "tenantB", "docs")
	require.Error(t, err)

	_, err = mgr.AuthorizeToken(context.Background(), tenantAAdminRaw, string(PermManageTenant), "tenantB", "docs")
	require.Error(t, err)

	// 2. Key rotation with grace period
	rotatedMeta, newWriterRaw, err := mgr.RotateAPIKey(tenantAWriterRaw, 50*time.Millisecond)
	require.NoError(t, err)
	require.NotEmpty(t, newWriterRaw)
	require.NotEqual(t, tenantAWriterRaw, newWriterRaw)
	require.NotEmpty(t, rotatedMeta.RotatedFrom)

	// Old key still valid within grace period, new key immediately valid
	ok, err = mgr.ValidateAPIKey(tenantAWriterRaw)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = mgr.ValidateAPIKey(newWriterRaw)
	require.NoError(t, err)
	require.True(t, ok)

	// After grace period expires, old key is rejected while new key remains active
	time.Sleep(65 * time.Millisecond)
	ok, err = mgr.ValidateAPIKey(tenantAWriterRaw)
	require.Error(t, err)
	require.False(t, ok)

	// 3. VectorStore DoAction enforcement & management APIs
	adminCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+adminRaw))
	createStream := &captureActionStream{ctx: adminCtx}
	createBody, _ := json.Marshal(map[string]any{
		"name":      "rls-key",
		"role":      "reader",
		"tenant_id": "tenantB",
		"namespace": "tenantB",
	})
	err = s.DoAction(&flight.Action{Type: "CreateAPIKey", Body: createBody}, createStream)
	require.NoError(t, err)
	require.Len(t, createStream.results, 1)

	// Non-admin reader cannot call GetAuditLog
	readerCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+tenantBReaderRaw))
	auditStream := &captureActionStream{ctx: readerCtx}
	err = s.DoAction(&flight.Action{Type: "GetAuditLog"}, auditStream)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// Admin CAN query audit log and observe recorded events
	adminAuditStream := &captureActionStream{ctx: adminCtx}
	err = s.DoAction(&flight.Action{Type: "GetAuditLog"}, adminAuditStream)
	require.NoError(t, err)
	require.Contains(t, string(adminAuditStream.results[0].Body), "create_api_key")

	// 4. Row-Level Security filtering on SearchResults
	metaMatch, _ := core.EncodeMetadata(map[string]any{"tenant_id": "tenantB", "env": "prod"})
	metaWrongTenant, _ := core.EncodeMetadata(map[string]any{"tenant_id": "tenantA", "env": "prod"})
	metaWrongEnv, _ := core.EncodeMetadata(map[string]any{"tenant_id": "tenantB", "env": "staging"})

	results := []SearchResult{
		{ID: 1, Metadata: metaMatch},
		{ID: 2, Metadata: metaWrongTenant},
		{ID: 3, Metadata: metaWrongEnv},
	}
	filtered := s.FilterResultsByRLS(readerCtx, "tenantB/events", results)
	require.Len(t, filtered, 1)
	require.Equal(t, core.VectorID(1), filtered[0].ID)

	// Admin sees all rows
	adminFiltered := s.FilterResultsByRLS(adminCtx, "tenantB/events", results)
	require.Len(t, adminFiltered, 3)

	// 5. Revoke key and verify denial
	require.NoError(t, mgr.RevokeAPIKey(newWriterRaw))
	_, err = mgr.AuthorizeToken(context.Background(), newWriterRaw, string(PermRead), "tenantA", "tenantA/docs")
	require.ErrorIs(t, err, security.ErrUnauthorized)
}

func FuzzRBACAuthorizeToken(f *testing.F) {
	f.Add("valid-role", "reader", "ns1", "ds1", "read")
	f.Add("bad-token", "writer", "ns2", "ds2", "admin")
	f.Fuzz(func(t *testing.T, name, roleStr, ns, ds, perm string) {
		mgr := NewRBACManager()
		_, raw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
			Name:      name,
			Role:      Role(roleStr),
			TenantID:  ns,
			Namespace: ns,
			Datasets:  []string{ds},
		})
		if err == nil {
			_, _ = mgr.AuthorizeToken(context.Background(), raw, perm, ns, ds)
			_, _ = mgr.AuthorizeToken(context.Background(), raw, perm, ns+"_other", ds)
		}
		_, _ = mgr.AuthorizeToken(context.Background(), name, perm, ns, ds)
	})
}
