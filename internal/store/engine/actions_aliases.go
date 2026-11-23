package engine

import (
	"context"

	"github.com/23skdu/longbow/internal/store/actions"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

// permissionAndDatasetForAction keeps existing engine call sites and white-box
// tests working after the RBAC action logic moved to its own domain package.
func permissionAndDatasetForAction(action *flight.Action) (Permission, string) {
	return actions.PermissionAndDatasetForAction(action)
}

// authorizeAction is the engine-side adapter to the actions domain host boundary.
func authorizeAction(ctx context.Context, host actions.ActionHost, action *flight.Action) error {
	return actions.AuthorizeAction(ctx, host, action)
}
