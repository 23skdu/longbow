package actions

import (
	"context"
	"errors"
	"testing"

	"github.com/23skdu/longbow/internal/security"
	"github.com/23skdu/longbow/internal/store/rbac"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/stretchr/testify/require"
)

type recordingActionHost struct {
	permission rbac.Permission
	target     string
	err        error
}

func (h *recordingActionHost) AuthorizeDatasetAccess(_ context.Context, perm rbac.Permission, target string) (*security.TenantContext, error) {
	h.permission = perm
	h.target = target
	if h.err != nil {
		return nil, h.err
	}
	return &security.TenantContext{}, nil
}

func TestPermissionAndDatasetForAction(t *testing.T) {
	tests := []struct {
		actionType string
		body       string
		permission rbac.Permission
		target     string
	}{
		{"VectorSearch", `{"dataset":"tenant/docs"}`, rbac.PermRead, "tenant/docs"},
		{"delete-vector", `{"name":"tenant/docs"}`, rbac.PermDelete, "tenant/docs"},
		{"CreateStreamConnector", `{"namespace":"tenant"}`, rbac.PermManageTenant, "tenant/*"},
		{"CreateAPIKey", `{"dataset":"keys"}`, rbac.PermManageKeys, "keys"},
		{"GetAuditLog", `{"dataset":"ignored"}`, rbac.PermAdmin, ""},
		{"unmapped-action", `{"dataset":"tenant/docs"}`, rbac.PermAdmin, "tenant/docs"},
	}

	for _, tt := range tests {
		t.Run(tt.actionType, func(t *testing.T) {
			perm, target := PermissionAndDatasetForAction(&flight.Action{
				Type: tt.actionType,
				Body: []byte(tt.body),
			})
			require.Equal(t, tt.permission, perm)
			require.Equal(t, tt.target, target)
		})
	}
}

func TestAuthorizeActionPropagatesHostResult(t *testing.T) {
	host := &recordingActionHost{}
	action := &flight.Action{Type: "VectorSearch", Body: []byte(`{"dataset":"tenant/docs"}`)}

	require.NoError(t, AuthorizeAction(context.Background(), host, action))
	require.Equal(t, rbac.PermRead, host.permission)
	require.Equal(t, "tenant/docs", host.target)

	denied := errors.New("denied")
	host.err = denied
	require.ErrorIs(t, AuthorizeAction(context.Background(), host, action), denied)
}

func FuzzPermissionAndDatasetForAction(f *testing.F) {
	f.Add("VectorSearch", `{"dataset":"tenant/docs"}`)
	f.Add("delete-vector", `{"name":"tenant/docs"}`)
	f.Add("GetAuditLog", `{"dataset":"ignored"}`)
	f.Add("unmapped-action", `{}`)
	f.Add("", "")

	f.Fuzz(func(t *testing.T, actionType, body string) {
		perm, target := PermissionAndDatasetForAction(&flight.Action{
			Type: actionType,
			Body: []byte(body),
		})

		switch perm {
		case rbac.PermRead, rbac.PermWrite, rbac.PermDelete, rbac.PermBackup,
			rbac.PermManageKeys, rbac.PermManageTenant, rbac.PermAdmin:
		default:
			t.Fatalf("unexpected permission %q for action %q", perm, actionType)
		}
		if actionType == "GetAuditLog" && target != "" {
			t.Fatalf("admin action must not carry a dataset target, got %q", target)
		}
	})
}
