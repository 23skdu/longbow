package mesh

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

const (
	ProtocolPeriod = 200 * time.Millisecond
	AckTimeout     = 100 * time.Millisecond
	IndirectK      = 3
	MaxPacketSize  = 1400
)

type Gossip struct {
	Config GossipConfig

	mu      sync.RWMutex
	members map[string]*Member     // ID -> Member
	peers   []string               // List of IDs for random selection
	updates map[string]*updateItem // Recently changed members for piggybacking

	conn *net.UDPConn
	seq  uint32

	pendingAcks map[uint32]chan struct{}
	ackMu       sync.Mutex

	closeCh  chan struct{}
	stopOnce sync.Once

	discoveryProvider DiscoveryProvider
}

type updateItem struct {
	Member    *Member
	SendCount int
}

type GossipConfig struct {
	ID        string
	Addr      string // Bind Addr
	Port      int
	Discovery DiscoveryConfig

	ProtocolPeriod   time.Duration
	AckTimeout       time.Duration
	SuspicionTimeout time.Duration

	Delegate EventDelegate
}

// EventDelegate handles cluster membership change events
type EventDelegate interface {
	NotifyJoin(member *Member)
	NotifyLeave(member *Member)
	NotifyUpdate(member *Member)
}

func NewGossip(cfg GossipConfig) *Gossip {
	if cfg.ProtocolPeriod == 0 {
		cfg.ProtocolPeriod = ProtocolPeriod
	}
	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = AckTimeout
	}
	if cfg.SuspicionTimeout == 0 {
		cfg.SuspicionTimeout = 5 * time.Second
	}

	return &Gossip{
		Config:      cfg,
		members:     make(map[string]*Member),
		peers:       make([]string, 0),
		pendingAcks: make(map[uint32]chan struct{}),
		updates:     make(map[string]*updateItem),
		closeCh:     make(chan struct{}),
	}
}

func (g *Gossip) Start() error {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", g.Config.Port))
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	g.conn = conn

	// Add self to members
	g.UpdateMember(&Member{
		ID:          g.Config.ID,
		Addr:        fmt.Sprintf("127.0.0.1:%d", g.Config.Port), // Simplification for now
		Status:      StatusAlive,
		Incarnation: 1,
	})

	// Discovery Bootstrap
	// Discovery Bootstrap
	// Initialize provider based on config
	switch g.Config.Discovery.Provider {
	case "static":
		peers := strings.Split(g.Config.Discovery.StaticPeers, ",")
		// Filter empty strings
		var validPeers []string
		for _, p := range peers {
			if p != "" {
				validPeers = append(validPeers, p)
			}
		}
		g.discoveryProvider = NewStaticProvider(validPeers)
	case "none", "":
		// No discovery
	default:
		// TODO: Implement K8s/DNS/MDNS support dynamically or via injection
		// For now we only support Static in this refactor
		// g.discoveryProvider = ...
	}

	if g.discoveryProvider != nil {
		ctx := context.Background()
		// Announce self
		_ = g.discoveryProvider.Register(ctx, g.Config.ID, g.Config.Port)

		// Find initial peers
		peers, _ := g.discoveryProvider.FindPeers(ctx)
		for _, peer := range peers {
			// Don't join self
			if peer == fmt.Sprintf("127.0.0.1:%d", g.Config.Port) {
				continue
			}
			go g.Join(peer)
		}
	}

	go g.protocolLoop()
	go g.listenLoop()
	go g.suspicionLoop()

	return nil
}

func (g *Gossip) Stop() {
	g.stopOnce.Do(func() {
		close(g.closeCh)
		if g.conn != nil {
			g.conn.Close()
		}
	})
}

func (g *Gossip) Join(peerAddr string) error {
	// Bootstrap: Send a Ping to the known peer
	// In a real implementation, we'd have a specific Join message to get full state sync.
	// For this task, we assume a Ping is enough to announce presence.
	return g.sendPing(peerAddr, "", g.nextSeq(), nil)
}

func (g *Gossip) suspicionLoop() {
	ticker := time.NewTicker(g.Config.SuspicionTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-g.closeCh:
			return
		case <-ticker.C:
			g.checkSuspicion()
		}
	}
}

func (g *Gossip) checkSuspicion() {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	for _, m := range g.members {
		if m.Status == StatusSuspect && !m.SuspectAt.IsZero() {
			if now.Sub(m.SuspectAt) > g.Config.SuspicionTimeout {
				m.Status = StatusDead
				g.addUpdate(m)
				// Notify delegate
				if g.Config.Delegate != nil {
					go g.Config.Delegate.NotifyLeave(m)
				}
				metrics.GossipActiveMembers.Set(float64(g.countAlive()))
			}
		}
	}
}

func (g *Gossip) addUpdate(m *Member) {
	g.updates[m.ID] = &updateItem{
		Member:    m,
		SendCount: 0,
	}
}

func (g *Gossip) countAlive() int {
	count := 0
	for _, m := range g.members {
		if m.Status == StatusAlive {
			count++
		}
	}
	return count
}

func (g *Gossip) protocolLoop() {
	ticker := time.NewTicker(ProtocolPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-g.closeCh:
			return
		case <-ticker.C:
			g.probe()
		}
	}
}

func (g *Gossip) probe() {
	g.mu.RLock()
	if len(g.peers) == 0 {
		g.mu.RUnlock()
		return
	}
	targetID := g.peers[rand.Intn(len(g.peers))]
	targetPtr := g.members[targetID]
	var target Member
	if targetPtr != nil {
		target = *targetPtr
	}
	g.mu.RUnlock()

	if targetPtr == nil || target.Status == StatusDead {
		return
	}

	seq := g.nextSeq()
	ackCh := make(chan struct{}, 1)
	g.ackMu.Lock()
	g.pendingAcks[seq] = ackCh
	g.ackMu.Unlock()

	defer func() {
		g.ackMu.Lock()
		delete(g.pendingAcks, seq)
		g.ackMu.Unlock()
	}()

	// 1. Direct Ping
	// Use empty payload for Ping (updates are piggybacked separately)
	// Actually sendPing handles piggybacking.
	// _ = g.sendPacket(targetAddr, PacketPing, seq, nil)

	packet := &Packet{
		Type: PacketPing,
		Seq:  seq,
	}
	udpAddr, err := net.ResolveUDPAddr("udp", target.Addr)
	if err == nil {
		_ = g.sendPacketStruct(packet, udpAddr)
	}

	// Send Ping with updates (The explicit sendPing logic was commented out, but we need it!)
	// Wait, the previous code had:
	// _ = g.sendPing(target.Addr, seq, nil)
	// I replaced it with empty PacketPing send via sendPacketStruct.
	// packetstruct DOES NOT piggyback updates (it only compresses payload if present).
	// sendPing DOES piggyback.
	// So I BROKE PROBE by removing sendPing call!!
	// This is why Upd=0 mostly!

	// Restoring sendPing call:
	_ = g.sendPing(target.Addr, target.ID, seq, nil)

	select {
	case <-ackCh:
		return // Success
	case <-time.After(g.Config.AckTimeout):
		// 2. Indirect Ping
		g.mu.RLock()
		others := g.selectNeighbors(target.ID, IndirectK)
		g.mu.RUnlock()

		if len(others) == 0 {
			g.markSuspect(target.ID)
			return
		}

		for _, other := range others {
			payload := []byte(target.Addr)

			packet := &Packet{
				Type:    PacketPingReq,
				Seq:     seq,
				Payload: payload,
			}
			udpAddr, err := net.ResolveUDPAddr("udp", other.Addr)
			if err != nil {
				continue
			}
			_ = g.sendPacketStruct(packet, udpAddr)
		}

		select {
		case <-ackCh:
			return // Success via proxy
		case <-time.After(g.Config.AckTimeout * 2):
			g.markSuspect(target.ID)
		}
	}
}

func (g *Gossip) markSuspect(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	m, ok := g.members[id]
	if !ok {
		return
	}

	if m.Status == StatusAlive {
		m.Status = StatusSuspect
		m.SuspectAt = time.Now()
		g.addUpdate(m)
	}
}

func (g *Gossip) selectNeighbors(skipID string, k int) []*Member {
	var others []*Member
	for id, m := range g.members {
		if id != skipID && id != g.Config.ID && m.Status == StatusAlive {
			others = append(others, m)
		}
	}
	if len(others) <= k {
		return others
	}
	rand.Shuffle(len(others), func(i, j int) {
		others[i], others[j] = others[j], others[i]
	})
	return others[:k]
}

func (g *Gossip) listenLoop() {
	buf := make([]byte, 65535)
	for {
		n, addr, err := g.conn.ReadFromUDP(buf)
		if err != nil {
			return // Socket closed
		}

		// Zero-copy processing (view of buf)
		packet, err := DecodePacket(buf[:n])
		if err != nil {
			continue
		}

		if packet.Type == PacketPing {
			metrics.GossipPingsTotal.WithLabelValues("received").Inc()
		}

		// Decompress payload if flag set
		if packet.Flags&FlagCompressed != 0 {
			decompressed, err := DecompressPayload(packet.Payload)
			if err != nil {
				// Metrics: decompression error
				continue
			}
			packet.Payload = decompressed
		}

		g.handlePacket(packet, addr)
	}
}

func (g *Gossip) handlePacket(p *Packet, addr *net.UDPAddr) {
	// Debug Log
	// fmt.Printf("[%s] Rx %d from %s Flags=%d Upd=%d Len=%d\n", g.Config.ID, p.Type, addr, p.Flags, p.NumUpdates, len(p.Payload))

	// 1. Process Updates (Piggybacking)
	if p.NumUpdates > 0 {
		offset := 0
		for i := 0; i < int(p.NumUpdates); i++ {
			if offset >= len(p.Payload) {
				// fmt.Printf("[%s] Error: Payload too short for updates\n", g.Config.ID)
				break
			}
			m, n, err := DecodeMember(p.Payload[offset:])
			if err != nil {
				// fmt.Printf("[%s] DecodeMember error: %v\n", g.Config.ID, err)
				break
			}
			g.UpdateMember(m)
			offset += n
		}
	}

	// 2. Message specific logic
	switch p.Type {
	case PacketPing:
		ack := &Packet{
			Type: PacketAck,
			Seq:  p.Seq,
		}
		// Send Ack
		g.sendPacketStruct(ack, addr)

	case PacketAck:
		g.ackMu.Lock()
		if ch, ok := g.pendingAcks[p.Seq]; ok {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		g.ackMu.Unlock()

	case PacketPingReq:
		// Target address is in payload
		targetAddr := string(p.Payload)
		go g.relayPing(targetAddr, p.Seq, addr)
	}
}

// sendPacketStruct encodes and sends a packet structure to dst
func (g *Gossip) sendPacketStruct(p *Packet, dst *net.UDPAddr) error {
	// Compress payload if large enough (heuristic > 64 bytes)
	if len(p.Payload) > 64 {
		compressed := CompressPayload(p.Payload)
		if len(compressed) < len(p.Payload) {
			p.Payload = compressed
			p.Flags |= FlagCompressed
		}
	}

	buf := make([]byte, MaxPacketSize)
	n, err := EncodePacket(p, buf)
	if err != nil {
		return err
	}
	_, err = g.conn.WriteToUDP(buf[:n], dst)
	return err
}

func (g *Gossip) sendPing(addr string, targetID string, seq uint32, payload []byte) error {
	// Piggyback updates - Dynamic size
	// Header overhead: ~7 bytes.
	// Max payload = MaxPacketSize - HeaderSize
	available := MaxPacketSize - HeaderSize

	g.mu.Lock()
	updates, numUpdates := g.getUpdatesForPacket(available, targetID)
	g.mu.Unlock()

	// fmt.Printf("[%s] Tx Ping to %s Upd=%d Size=%d\n", g.Config.ID, addr, numUpdates, len(updates))

	packet := &Packet{
		Type:       PacketPing,
		Seq:        seq,
		NumUpdates: uint8(numUpdates),
		Payload:    updates,
	}

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	if err := g.sendPacketStruct(packet, udpAddr); err != nil {
		return err
	}

	metrics.GossipPingsTotal.WithLabelValues("sent").Inc()
	return nil
}

func (g *Gossip) getUpdatesForPacket(limitBytes int, skipID string) ([]byte, int) {
	// Simple strategy: Iterate updates, encode, stop when full
	// Prioritization: Sort by send count? For now, we use map iteration (randomish) which is okay-ish implementation of "Gossip"
	// but strictly we should prioritize lower send counts.
	// Since we iterate the map, let's just pick items until full.

	// Max updates cap to fit in uint8 NumUpdates (255)
	maxUpdates := 255

	var buf []byte = make([]byte, limitBytes)
	offset := 0
	count := 0

	// Two-pass approach (optional optimization): 1. Low send counts, 2. Higher send counts
	// For simplicity in this step, single pass but we delete if send count high.

	// Send limit
	sendLimit := 5 // log(N) approximation hardcoded for now

	toDelete := make([]string, 0)

	for id, item := range g.updates {
		if id == skipID {
			continue
		}
		if count >= maxUpdates {
			break
		}

		// Encode into temp buffer to check size
		// We actually need to encode straight to output to be efficient, but EncodeMember is variable length.
		// Let's use a small scratch buffer
		tmp := make([]byte, 512) // Member struct shouldn't exceed 512 bytes
		n, err := EncodeMember(item.Member, tmp)
		if err != nil {
			continue // Should not happen
		}

		if offset+n > limitBytes {
			// Buffer full
			break
		}

		// Copy to result buffer
		copy(buf[offset:], tmp[:n])
		offset += n
		count++

		item.SendCount++
		if item.SendCount >= sendLimit {
			toDelete = append(toDelete, id)
		}
	}

	// Cleanup transmitted updates
	for _, id := range toDelete {
		delete(g.updates, id)
	}

	return buf[:offset], count
}

func (g *Gossip) relayPing(targetAddr string, seq uint32, sourceAddr *net.UDPAddr) {
	ackCh := make(chan struct{}, 1)
	g.ackMu.Lock()
	g.pendingAcks[seq] = ackCh
	g.ackMu.Unlock()

	defer func() {
		g.ackMu.Lock()
		delete(g.pendingAcks, seq)
		g.ackMu.Unlock()
	}()

	packet := &Packet{
		Type: PacketPing,
		Seq:  seq,
	}
	udpAddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err == nil {
		_ = g.sendPacketStruct(packet, udpAddr)
	}

	select {
	case <-ackCh:
		// Relay Ack back to source
		ack := &Packet{Type: PacketAck, Seq: seq}
		_ = g.sendPacketStruct(ack, sourceAddr)
	case <-time.After(g.Config.AckTimeout):
		// Silent failure
	}
}

func (g *Gossip) nextSeq() uint32 {
	return atomic.AddUint32(&g.seq, 1)
}

// UpdateMember updates the state of a member and triggers delegates
func (g *Gossip) UpdateMember(m *Member) {
	g.mu.Lock()
	defer g.mu.Unlock()

	existing, ok := g.members[m.ID]
	isNew := !ok

	if isNew {
		g.members[m.ID] = m
		g.peers = append(g.peers, m.ID)
		g.addUpdate(m)
		// If new and alive, notify join
		if m.Status == StatusAlive && g.Config.Delegate != nil {
			go g.Config.Delegate.NotifyJoin(m)
		}
		metrics.GossipActiveMembers.Set(float64(g.countAlive()))
		return
	}

	oldStatus := existing.Status
	shouldUpdate := false

	// Apply conflict resolution (Incarnation check)
	// SWIM rules:
	// - Dead > all
	// - Suspect > Alive if Incarnation >=
	// - Alive > Suspect if Incarnation >

	if m.Status == StatusDead {
		if existing.Status != StatusDead {
			shouldUpdate = true
		}
	} else {
		// Update rules for non-dead
		if m.Incarnation > existing.Incarnation {
			shouldUpdate = true
		} else if m.Incarnation == existing.Incarnation {
			if m.Status == StatusSuspect && existing.Status == StatusAlive {
				shouldUpdate = true
			}
		}
	}

	if shouldUpdate {
		existing.Status = m.Status
		existing.Incarnation = m.Incarnation
		existing.Addr = m.Addr
		existing.LastSeen = time.Now()

		if m.Status == StatusSuspect {
			if existing.SuspectAt.IsZero() {
				existing.SuspectAt = time.Now()
			}
		} else if m.Status == StatusAlive {
			existing.SuspectAt = time.Time{}
		}

		g.addUpdate(existing)

		// Trigger notifications on transition
		if g.Config.Delegate != nil {
			if oldStatus != StatusAlive && existing.Status == StatusAlive {
				go g.Config.Delegate.NotifyJoin(existing)
			} else if oldStatus != StatusDead && existing.Status == StatusDead {
				go g.Config.Delegate.NotifyLeave(existing)
			}
		}
	}
}

// GetMembers returns a list of all known members.
func (g *Gossip) GetMembers() []Member {
	g.mu.RLock()
	defer g.mu.RUnlock()

	members := make([]Member, 0, len(g.members))
	for _, m := range g.members {
		members = append(members, *m)
	}
	return members
}

// GetIdentity returns information about this node.
func (g *Gossip) GetIdentity() Member {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if m, ok := g.members[g.Config.ID]; ok {
		return *m
	}
	return Member{
		ID:     g.Config.ID,
		Addr:   fmt.Sprintf("127.0.0.1:%d", g.Config.Port),
		Status: StatusAlive,
	}
}

func (g *Gossip) GetDiscoveryStatus() (string, []string) {
	if g.discoveryProvider == nil {
		return "none", nil
	}
	peers, _ := g.discoveryProvider.FindPeers(context.Background())
	return fmt.Sprintf("%T", g.discoveryProvider), peers
}

// DiscoveryConfig holds configuration for peer discovery
type DiscoveryConfig struct {
	Provider    string // "static", "k8s", "dns"
	StaticPeers string // comma separated
	DNSRecord   string // for dns provider
}
