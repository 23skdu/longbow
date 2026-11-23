// alias-only: not re-exported
package apihandlers

import (
	"strings"

	"github.com/23skdu/longbow/internal/store/cluster"
	"github.com/23skdu/longbow/internal/store/rbac"
	"github.com/23skdu/longbow/internal/store/streaming"
)

// Domain-local aliases keep API handlers independent of the store engine while
// preserving the exported surface they historically used.
type (
	// ChangeDataCapture is the cluster CDC implementation.
	ChangeDataCapture = cluster.ChangeDataCapture
	// CDCSubscription is a cluster CDC subscription.
	CDCSubscription = cluster.CDCSubscription
	// CDCFilter filters CDC events.
	CDCFilter = cluster.CDCFilter
	// CDCEventType identifies a CDC event kind.
	CDCEventType = cluster.CDCEventType
	// CDCEvent is a change data capture event.
	CDCEvent = cluster.CDCEvent
	// Permission is the RBAC permission type.
	Permission = rbac.Permission
	// StreamVectorEvent is a streaming vector event.
	StreamVectorEvent = streaming.StreamVectorEvent
	// StreamConnectorConfig configures a streaming connector.
	StreamConnectorConfig = streaming.StreamConnectorConfig
	// StreamSourceType identifies a streaming source kind.
	StreamSourceType = streaming.StreamSourceType
)

const (
	// PermRead allows reads.
	PermRead = rbac.PermRead
	// PermWrite allows writes.
	PermWrite = rbac.PermWrite

	// CDCEventInsert identifies an insert event.
	CDCEventInsert = cluster.CDCEventInsert
	// CDCEventUpdate identifies an update event.
	CDCEventUpdate = cluster.CDCEventUpdate
	// CDCEventDelete identifies a delete event.
	CDCEventDelete = cluster.CDCEventDelete
	// StreamSourceMemory is the in-memory streaming source.
	StreamSourceMemory = streaming.StreamSourceMemory
)

// parseNamespacedPath mirrors the engine helper so API handlers do not need to
// import the engine package to split "namespace/dataset" paths.
func parseNamespacedPath(path string) (namespace, dataset string) {
	if strings.HasPrefix(path, "/") {
		return "default", strings.TrimPrefix(path, "/")
	}
	if path == "" {
		return "default", ""
	}
	if idx := strings.Index(path, "/"); idx >= 0 {
		return path[:idx], path[idx+1:]
	}
	return "default", path
}
