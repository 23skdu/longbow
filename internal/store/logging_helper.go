package store

import (
	"context"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/peer"
)

// LogClientAction logs an action performed by a client, resolving their identity if possible.
//
//nolint:gocritic // Logger passed by value for simplicity
func LogClientAction(ctx context.Context, logger zerolog.Logger, m *mesh.Gossip, action string, meta map[string]interface{}) {
	// 1. Get Client IP from Context
	var clientIP string
	if p, ok := peer.FromContext(ctx); ok {
		clientIP = p.Addr.String()
	} else {
		clientIP = "unknown"
	}

	// 2. Resolve to Member ID if Mesh is enabled
	var clientID string
	if m != nil && clientIP != "unknown" {
		if member, found := m.GetMemberByAddr(clientIP); found {
			clientID = member.ID
		}
	}

	// 3. Construct Event Log
	evt := logger.Debug().
		Str("action", action).
		Str("client_ip", clientIP)

	if clientID != "" {
		evt.Str("client_id", clientID)
	}

	// Add metadata
	for k, v := range meta {
		evt.Interface(k, v)
	}

	evt.Msg("Client Action")
}
