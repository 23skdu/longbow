// alias-only: not re-exported
package streaming

import "github.com/23skdu/longbow/internal/store/cluster"

// CDC type aliases keep the streaming domain decoupled from the store engine
// while preserving the public API re-exported through the store facade.
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
)

const (
	// CDCEventInsert identifies an insert event.
	CDCEventInsert = cluster.CDCEventInsert
	// CDCEventUpdate identifies an update event.
	CDCEventUpdate = cluster.CDCEventUpdate
	// CDCEventDelete identifies a delete event.
	CDCEventDelete = cluster.CDCEventDelete
)
