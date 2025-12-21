package mesh

import (
	"context"
	"fmt"
	"math/rand"
	"net"
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
}

type updateItem struct {
	Member    *Member
	SendCount int
}

type GossipConfig struct {
	ID        string
	Addr      string // Bind Addr
	Port      int
	Discovery DiscoveryProvider

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
	if g.Config.Discovery != nil {
		ctx := context.Background()
		// Announce self
		_ = g.Config.Discovery.Register(ctx, g.Config.ID, g.Config.Port)

		// Find initial peers
		peers, _ := g.Config.Discovery.FindPeers(ctx)
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
	return g.sendPing(peerAddr, g.nextSeq(), nil)
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
	target := g.members[targetID]
	g.mu.RUnlock()

	if target == nil || target.Status == StatusDead {
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
	_ = g.sendPing(target.Addr, seq, nil)

	select {
	case <-ackCh:
		return // Success
	case <-time.After(g.Config.AckTimeout):
		// 2. Indirect Ping
		g.mu.RLock()
		others := g.selectNeighbors(target.ID, IndirectK)
		g.mu.RUnlock()

		if len(others) == 0 {
			g.markSuspect(target)
			return
		}

		for _, other := range others {
			payload := []byte(target.Addr)
			_ = g.sendPacket(other.Addr, PacketPingReq, seq, payload)
		}

		select {
		case <-ackCh:
			return // Success via proxy
		case <-time.After(g.Config.AckTimeout * 2):
			g.markSuspect(target)
		}
	}
}

func (g *Gossip) markSuspect(m *Member) {
	g.mu.Lock()
	defer g.mu.Unlock()
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

func (g *Gossip) sendPacket(addr string, pType PacketType, seq uint32, payload []byte) error {
	packet := &Packet{
		Type:    pType,
		Seq:     seq,
		Payload: payload,
	}

	buf := make([]byte, MaxPacketSize)
	n, err := EncodePacket(packet, buf)
	if err != nil {
		return err
	}

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	_, err = g.conn.WriteToUDP(buf[:n], udpAddr)
	return err
}

func (g *Gossip) sendPing(addr string, seq uint32, payload []byte) error {
	// Piggyback updates
	g.mu.Lock()
	updates := g.getUpdatesForPacket()
	g.mu.Unlock()

	var updatesBuf []byte
	numUpdates := 0
	if len(updates) > 0 {
		tmp := make([]byte, MaxPacketSize)
		offset := 0
		for _, u := range updates {
			n, err := EncodeMember(u, tmp[offset:])
			if err != nil {
				break
			}
			offset += n
			numUpdates++
		}
		updatesBuf = tmp[:offset]
	}

	packet := &Packet{
		Type:       PacketPing,
		Seq:        seq,
		NumUpdates: uint8(numUpdates),
		Payload:    updatesBuf,
	}

	buf := make([]byte, MaxPacketSize)
	n, err := EncodePacket(packet, buf)
	if err != nil {
		return err
	}

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	_, err = g.conn.WriteToUDP(buf[:n], udpAddr)
	if err == nil {
		metrics.GossipPingsTotal.WithLabelValues("sent").Inc()
	}
	return err
}

func (g *Gossip) getUpdatesForPacket() []*Member {
	maxUpdates := 5
	res := make([]*Member, 0, maxUpdates)

	// log(N) * 3 is a good rule of thumb for send count
	limit := 5

	count := 0
	for id, item := range g.updates {
		if count >= maxUpdates {
			break
		}
		res = append(res, item.Member)
		item.SendCount++
		if item.SendCount >= limit {
			delete(g.updates, id)
		}
		count++
	}
	return res
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

		g.handlePacket(packet, addr)
	}
}

func (g *Gossip) handlePacket(p *Packet, addr *net.UDPAddr) {
	// 1. Process Updates (Piggybacking)
	if p.NumUpdates > 0 {
		offset := 0
		for i := 0; i < int(p.NumUpdates); i++ {
			m, n, err := DecodeMember(p.Payload[offset:])
			if err != nil {
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
		out := make([]byte, MaxPacketSize)
		n, _ := EncodePacket(ack, out)
		g.conn.WriteToUDP(out[:n], addr)

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
		// Send a direct ping to the target, using the same SEQ so the original source hears the Ack
		// Wait, SWIM specifies we should relay the Ack.
		// For simplicity, let's just Ping target. If target responds with Ack to proxy, proxy relays to source.
		// Actually, SWIM usually has proxy send Ping to Target, and Target sends Ack to Proxy, then Proxy sends Ack to Source.
		go g.relayPing(targetAddr, p.Seq, addr)
	}
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

	_ = g.sendPacket(targetAddr, PacketPing, seq, nil)

	select {
	case <-ackCh:
		// Relay Ack back to source
		ack := &Packet{Type: PacketAck, Seq: seq}
		buf := make([]byte, MaxPacketSize)
		n, _ := EncodePacket(ack, buf)
		g.conn.WriteToUDP(buf[:n], sourceAddr)
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
	if g.Config.Discovery == nil {
		return "none", nil
	}
	peers, _ := g.Config.Discovery.FindPeers(context.Background())
	return fmt.Sprintf("%T", g.Config.Discovery), peers
}
