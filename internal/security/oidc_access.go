package security

import (
	"context"
	"fmt"
)

// OIDCAccessValidator resolves federated OIDC tokens to TenantContext and
// enforces permission, namespace, and dataset boundaries.
type OIDCAccessValidator struct {
	manager *OIDCManager
	audit   *AuditLogger
}

// NewOIDCAccessValidator creates a validator backed by an OIDCManager.
func NewOIDCAccessValidator(manager *OIDCManager) *OIDCAccessValidator {
	return &OIDCAccessValidator{manager: manager}
}

// SetAuditLogger attaches an audit logger.
func (v *OIDCAccessValidator) SetAuditLogger(a *AuditLogger) {
	v.audit = a
}

// AuthorizeToken implements AccessValidator.
func (v *OIDCAccessValidator) AuthorizeToken(ctx context.Context, rawToken, permission, namespace, dataset string) (*TenantContext, error) {
	if v.manager == nil {
		return nil, fmt.Errorf("%w: oidc manager not configured", ErrUnauthorized)
	}
	claims, err := v.manager.ValidateToken(ctx, rawToken)
	if err != nil {
		if v.audit != nil {
			v.audit.RecordEvent(ctx, "", namespace, "authorize:oidc", dataset, "rpc", false, err.Error())
		}
		return nil, fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	tc := claims.ToTenantContext()
	if permission != "" && !tc.HasPermission(permission) {
		if v.audit != nil {
			v.audit.RecordEvent(ctx, tc.KeyID, tc.TenantID, "authorize:"+permission, dataset, "rpc", false, "permission denied")
		}
		return nil, fmt.Errorf("%w: permission denied", ErrForbidden)
	}
	if !tc.CanAccessDataset(namespace, dataset) {
		if v.audit != nil {
			v.audit.RecordEvent(ctx, tc.KeyID, tc.TenantID, "authorize:"+permission, namespace+"/"+dataset, "rpc", false, "dataset not authorized")
		}
		return nil, fmt.Errorf("%w: dataset not authorized", ErrForbidden)
	}
	if v.audit != nil {
		v.audit.RecordEvent(ctx, tc.KeyID, tc.TenantID, "authorize:"+permission, dataset, "rpc", true, "")
	}
	return tc, nil
}
