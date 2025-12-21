package mesh

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
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
	members map[string]*Member // ID -> Member
	peers   []string           // List of IDs for random selection

	conn *net.UDPConn
	seq  uint32

	closeCh chan struct{}
}

type GossipConfig struct {
	ID        string
	Addr      string // Bind Addr
	Port      int
	Discovery DiscoveryProvider
}

func NewGossip(cfg GossipConfig) *Gossip {
	return &Gossip{
		Config:  cfg,
		members: make(map[string]*Member),
		peers:   make([]string, 0),
		closeCh: make(chan struct{}),
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

	return nil
}

func (g *Gossip) Stop() {
	close(g.closeCh)
	if g.conn != nil {
		g.conn.Close()
	}
}

func (g *Gossip) Join(peerAddr string) error {
	// Bootstrap: Send a Ping to the known peer
	// In a real implementation, we'd have a specific Join message to get full state sync.
	// For this task, we assume a Ping is enough to announce presence.
	return g.sendPing(peerAddr, g.nextSeq(), nil)
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
	// Select random peer (skipping self is implicit if logic is correct, but self shouldn't be in peers list usually)
	// For simplicity, we keep self in members but not in peers list for probing.
	targetID := g.peers[rand.Intn(len(g.peers))]
	target := g.members[targetID]
	g.mu.RUnlock()

	if target == nil || target.Status == StatusDead {
		return
	}

	seq := g.nextSeq()
	// TODO: Handle Ack wait and Indirect Ping
	// For this basic task, we just fire and forget the Ping to demonstrate structure.
	// Implementing full Ack Timeout + Indirect Ping requires a separate goroutine or callback map.
	_ = g.sendPing(target.Addr, seq, nil)
}

func (g *Gossip) sendPing(addr string, seq uint32, payload []byte) error {
	packet := &Packet{
		Type:    PacketPing,
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

		g.handlePacket(packet, addr)
	}
}

func (g *Gossip) handlePacket(p *Packet, addr *net.UDPAddr) {
	// Send Ack if Ping
	if p.Type == PacketPing {
		ack := &Packet{
			Type: PacketAck,
			Seq:  p.Seq,
		}
		out := make([]byte, MaxPacketSize)
		n, _ := EncodePacket(ack, out)
		g.conn.WriteToUDP(out[:n], addr)
	}

	// Process updates from payload
	// (Parse members and apply resolution rules)
}

func (g *Gossip) nextSeq() uint32 {
	return g.seq // Atomic increment in real usage
}

func (g *Gossip) UpdateMember(m *Member) {
	g.mu.Lock()
	defer g.mu.Unlock()

	existing, ok := g.members[m.ID]
	if !ok {
		m.LastSeen = time.Now()
		g.members[m.ID] = m
		if m.ID != g.Config.ID {
			g.peers = append(g.peers, m.ID)
		}
		return
	}

	// Apply conflict resolution (Incarnation check)
	if m.Incarnation >= existing.Incarnation {
		existing.Status = m.Status
		existing.Incarnation = m.Incarnation
		existing.Addr = m.Addr
		existing.LastSeen = time.Now()
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
