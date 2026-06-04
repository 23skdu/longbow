package sharding

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/rs/zerolog"
)

// TimeBounds represents a min and max timestamp.
type TimeBounds struct {
	Min int64
	Max int64
}

// RingManager coordinates the consistent hash ring with the cluster membership state
type RingManager struct {
	mu             sync.RWMutex
	ring           *ConsistentHash
	localNodeID    string
	logger         zerolog.Logger
	nodeAddrs      map[string]string  // ID -> Data Addr
	metaAddrs      map[string]string  // ID -> Meta Addr
	nodeLoads      map[string]float64 // ID -> load ratio (0.0-1.0)
	geoRouters     map[string]*mesh.Router
	temporalBounds map[string]map[string]*TimeBounds // dataset -> nodeID -> bounds
	baseVNodes     int
}

// NewRingManager creates a new RingManager
//
//nolint:gocritic // Logger passed by value for constructor simplicity
func NewRingManager(localNodeID string, logger zerolog.Logger) *RingManager {
	return &RingManager{
		ring:           NewConsistentHash(20), // 20 vnodes default
		localNodeID:    localNodeID,
		logger:         logger,
		nodeAddrs:      make(map[string]string),
		metaAddrs:      make(map[string]string),
		nodeLoads:      make(map[string]float64),
		geoRouters:     make(map[string]*mesh.Router),
		temporalBounds: make(map[string]map[string]*TimeBounds),
		baseVNodes:     20,
	}
}

// NotifyJoin is invoked when a node joins the cluster
func (rm *RingManager) NotifyJoin(member *mesh.Member) {
	rm.mu.Lock()
	rm.logger.Info().
		Str("node", member.ID).
		Str("grpc_addr", member.GRPCAddr).
		Str("meta_addr", member.MetaAddr).
		Msg("Node joined ring")
	rm.ring.AddNode(member.ID)
	rm.nodeAddrs[member.ID] = member.GRPCAddr
	rm.metaAddrs[member.ID] = member.MetaAddr
	rm.mu.Unlock()

	rm.updateIndexBoundaries(member)
}

// NotifyLeave is invoked when a node leaves the cluster
func (rm *RingManager) NotifyLeave(member *mesh.Member) {
	rm.mu.Lock()
	rm.logger.Info().Str("node", member.ID).Msg("Node left ring")
	rm.ring.RemoveNode(member.ID)
	delete(rm.nodeAddrs, member.ID)
	delete(rm.metaAddrs, member.ID)
	rm.mu.Unlock()

	rm.removeIndexBoundaries(member)
}

// NotifyUpdate is invoked when a node is updated
func (rm *RingManager) NotifyUpdate(member *mesh.Member) {
	// Update addresses if they changed
	rm.mu.Lock()
	rm.nodeAddrs[member.ID] = member.GRPCAddr
	rm.metaAddrs[member.ID] = member.MetaAddr
	rm.mu.Unlock()

	rm.updateIndexBoundaries(member)
}

// UpdateNodeLoad updates the reported load for a node and triggers rebalancing if necessary
func (rm *RingManager) UpdateNodeLoad(nodeID string, load float64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.nodeLoads[nodeID] = load
	rm.logger.Debug().Str("node", nodeID).Float64("load", load).Msg("Node load updated")

	// Trigger rebalancing logic (could be debounced or on a ticker, but we do it here for now)
	rm.rebalance()
}

// rebalance adjusts vnode counts based on node loads to distribute traffic more fairly
func (rm *RingManager) rebalance() {
	// Rebalance policy:
	// - If load > 0.8, decrease vnodes (reduce incoming traffic)
	// - If load < 0.4, increase vnodes (accept more traffic)
	// - Otherwise, keep at baseVNodes

	for nodeID, load := range rm.nodeLoads {
		targetVNodes := rm.baseVNodes
		if load > 0.8 {
			targetVNodes = rm.baseVNodes / 2
			if targetVNodes < 1 {
				targetVNodes = 1
			}
		} else if load < 0.4 {
			targetVNodes = rm.baseVNodes * 2
		}

		rm.ring.UpdateVNodeCount(nodeID, targetVNodes)
	}
}

// GetNode returns the owner of a key
func (rm *RingManager) GetNode(key string) string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.ring.GetNode(key)
}

// IsLocalKey returns true if the key belongs to the local node
func (rm *RingManager) IsLocalKey(key string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	owner := rm.ring.GetNode(key)
	return owner == rm.localNodeID
}

// GetPreferenceList returns the replication nodes for a key
func (rm *RingManager) GetPreferenceList(key string, n int) []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.ring.GetPreferenceList(key, n)
}

// GetNodeAddr returns the data service network address for a given node ID
func (rm *RingManager) GetNodeAddr(nodeID string) string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.nodeAddrs[nodeID]
}

// GetMetaAddr returns the metadata/search service network address for a given node ID
func (rm *RingManager) GetMetaAddr(nodeID string) string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.metaAddrs[nodeID]
}

// GetMembers returns a list of all known node IDs in the ring
func (rm *RingManager) GetMembers() []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	members := make([]string, 0, len(rm.nodeAddrs))
	for id := range rm.nodeAddrs {
		members = append(members, id)
	}
	return members
}

func (rm *RingManager) updateIndexBoundaries(member *mesh.Member) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Parse tags to find geo and temporal boundaries for datasets
	for k, v := range member.Tags {
		// Geospatial: geo:<dataset>:centroid and geo:<dataset>:radius
		if strings.HasPrefix(k, "geo:") && strings.HasSuffix(k, ":centroid") {
			dataset := k[4 : len(k)-9]
			radiusKey := fmt.Sprintf("geo:%s:radius", dataset)
			radiusStr, hasRadius := member.Tags[radiusKey]
			if hasRadius {
				var lat, lon float64
				n, err := fmt.Sscanf(v, "%f,%f", &lat, &lon)
				if err == nil && n == 2 {
					var radius float64
					_, err = fmt.Sscanf(radiusStr, "%f", &radius)
					if err == nil {
						if rm.geoRouters == nil {
							rm.geoRouters = make(map[string]*mesh.Router)
						}
						router, ok := rm.geoRouters[dataset]
						if !ok {
							router = mesh.NewRouter()
							rm.geoRouters[dataset] = router
						}
						
						// Create unique region ID based on member.ID hash
						h := fnv.New64a()
						_, _ = h.Write([]byte(member.ID))
						regionID := h.Sum64()

						router.UpdateRegion(mesh.Region{
							ID:       regionID,
							Centroid: []float32{float32(lat), float32(lon)},
							Radius:   float32(radius),
							OwnerID:  member.ID,
						})
					}
				}
			}
		}

		// Temporal: temporal:<dataset>:min and temporal:<dataset>:max
		if strings.HasPrefix(k, "temporal:") && strings.HasSuffix(k, ":min") {
			dataset := k[9 : len(k)-4]
			maxKey := fmt.Sprintf("temporal:%s:max", dataset)
			maxStr, hasMax := member.Tags[maxKey]
			if hasMax {
				var minTs, maxTs int64
				_, err1 := fmt.Sscanf(v, "%d", &minTs)
				_, err2 := fmt.Sscanf(maxStr, "%d", &maxTs)
				if err1 == nil && err2 == nil {
					if rm.temporalBounds == nil {
						rm.temporalBounds = make(map[string]map[string]*TimeBounds)
					}
					boundsMap, ok := rm.temporalBounds[dataset]
					if !ok {
						boundsMap = make(map[string]*TimeBounds)
						rm.temporalBounds[dataset] = boundsMap
					}
					boundsMap[member.ID] = &TimeBounds{Min: minTs, Max: maxTs}
				}
			}
		}
	}
}

func (rm *RingManager) removeIndexBoundaries(member *mesh.Member) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Clean up temporal bounds for this node
	for dataset, boundsMap := range rm.temporalBounds {
		delete(boundsMap, member.ID)
		if len(boundsMap) == 0 {
			delete(rm.temporalBounds, dataset)
		}
	}

	// Clean up geo routers
	for _, router := range rm.geoRouters {
		router.RemoveRegion(member.ID)
	}
}

func (rm *RingManager) RouteGeo(dataset string, lat, lon float64, radiusKm float64) []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.geoRouters == nil {
		return nil
	}
	router, ok := rm.geoRouters[dataset]
	if !ok {
		return nil
	}

	return router.Route([]float32{float32(lat), float32(lon)}, 1000)
}

func (rm *RingManager) RouteTemporal(dataset string, startTime, endTime int64) []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.temporalBounds == nil {
		return nil
	}
	boundsMap, ok := rm.temporalBounds[dataset]
	if !ok {
		return nil
	}

	var matchedNodes []string
	for nodeID, bounds := range boundsMap {
		if !(bounds.Max < startTime || bounds.Min > endTime) {
			matchedNodes = append(matchedNodes, nodeID)
		}
	}
	return matchedNodes
}
