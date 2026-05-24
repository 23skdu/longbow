package cluster

import (
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/rs/zerolog"
)

// FlightBackend defines the store methods required by the flight servers.
type FlightBackend interface {
	flight.FlightServer
	
	// Accessors for logging and mesh
	GetLogger() zerolog.Logger
	GetMesh() *mesh.Gossip
	GetWALQueueDepth() (int, int)
	HandleVectorSearchAction(req *flight.Action, stream flight.FlightService_DoActionServer) error
	DoMetaAction(action *flight.Action, stream flight.FlightService_DoActionServer) error
}
