package sync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SyncPriority defines how often to check a peer for updates
type SyncPriority int

const (
	PriorityHigh   SyncPriority = iota // Check every 100ms
	PriorityNormal                     // Check every 1s
	PriorityLow                        // Check every 10s
)

// SyncWorker manages background delta replication from peers
type SyncWorker struct {
	store  *store.VectorStore
	logger *zap.Logger

	peers  map[string]*PeerState
	mu     sync.RWMutex
	stopCh chan struct{}
}

type PeerState struct {
	Addr        string
	LastSeenSeq uint64
	Priority    SyncPriority
}

func NewSyncWorker(s *store.VectorStore, logger *zap.Logger) *SyncWorker {
	return &SyncWorker{
		store:  s,
		logger: logger,
		peers:  make(map[string]*PeerState),
		stopCh: make(chan struct{}),
	}
}

func (w *SyncWorker) AddPeer(addr string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.peers[addr]; ok {
		return
	}
	w.peers[addr] = &PeerState{
		Addr:     addr,
		Priority: PriorityNormal,
	}
}

func (w *SyncWorker) Start() {
	go w.run()
}

func (w *SyncWorker) Stop() {
	close(w.stopCh)
}

func (w *SyncWorker) run() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.syncAll()
		}
	}
}

func (w *SyncWorker) syncAll() {
	// 1. Discover peers from Gossip if available
	if w.store.Mesh != nil {
		members := w.store.Mesh.GetMembers()
		for _, m := range members {
			// Skip self and non-alive nodes
			if m.ID != w.store.Mesh.Config.ID && m.Status == mesh.StatusAlive {
				// Gossip address might be UDP, but SyncWorker needs gRPC (TCP).
				// We assume they share the same host, but we might need a way to map ports.
				// For now, we use the Addr from gossip directly.
				w.AddPeer(m.Addr)
			}
		}
	}

	w.mu.RLock()
	var peers []*PeerState
	for _, p := range w.peers {
		peers = append(peers, p)
	}
	w.mu.RUnlock()

	for _, p := range peers {
		if err := w.syncPeer(p); err != nil {
			w.logger.Warn("Failed to sync with peer", zap.String("addr", p.Addr), zap.Error(err))
		}
	}
}

func (w *SyncWorker) syncPeer(p *PeerState) error {
	w.logger.Info("Syncing with peer", zap.String("addr", p.Addr), zap.Uint64("last_seq", p.LastSeenSeq))
	// 1. Connect to peer
	conn, err := grpc.NewClient(p.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// 2. Start DoExchange for sync
	stream, err := client.DoExchange(context.Background())
	if err != nil {
		return err
	}

	// 3. Send "sync" request with last seen seq
	lastSeqBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(lastSeqBuf, p.LastSeenSeq)

	req := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Cmd: []byte("sync"),
		},
		DataBody: lastSeqBuf,
	}

	if err := stream.Send(req); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}

	// 4. Compare Merkle Roots (using "ds1" as example)
	remoteRoot, err := w.fetchRemoteMerkleRoot(p.Addr, "ds1")
	if err == nil {
		localRoot := w.store.MerkleRoot("ds1")
		if remoteRoot == localRoot {
			metrics.MeshMerkleMatchTotal.WithLabelValues("match").Inc()
			w.logger.Info("Merkle roots match, skipping sync", zap.String("peer", p.Addr))
			// return nil
		} else {
			metrics.MeshMerkleMatchTotal.WithLabelValues("mismatch").Inc()
			w.logger.Info("Merkle roots differ, starting sync", zap.String("peer", p.Addr))
		}
	}

	// 5. Receive deltas
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if err != io.EOF {
				w.logger.Error("Recv error", zap.Error(err))
			}
			return err
		}

		// Unpack AppMetadata: Seq(8) | TS(8)
		if len(resp.AppMetadata) < 16 {
			continue // Invalid metadata
		}
		seq := binary.LittleEndian.Uint64(resp.AppMetadata[0:8])
		ts := int64(binary.LittleEndian.Uint64(resp.AppMetadata[8:16]))

		// Dataset name from path
		if len(resp.FlightDescriptor.Path) == 0 {
			continue
		}
		datasetName := resp.FlightDescriptor.Path[0]

		// Deserialize RecordBatch
		r, err := ipc.NewReader(bytes.NewReader(resp.DataBody))
		if err != nil {
			w.logger.Error("Failed to decode sync record", zap.Error(err))
			continue
		}

		if r.Next() {
			rec := r.Record()
			rec.Retain()

			// Apply to local store
			if err := w.store.ApplyDelta(datasetName, rec, seq, ts); err != nil {
				metrics.MeshSyncDeltasTotal.WithLabelValues("error").Inc()
				w.logger.Error("Failed to apply delta", zap.String("dataset", datasetName), zap.Error(err))
			} else {
				metrics.MeshSyncDeltasTotal.WithLabelValues("success").Inc()
				metrics.MeshSyncBytesTotal.Add(float64(len(resp.DataBody)))
			}

			// Update last seen seq
			if seq > p.LastSeenSeq {
				p.LastSeenSeq = seq
			}
		}
		r.Release()
	}

	return nil
}

func (w *SyncWorker) fetchRemoteMerkleRoot(addr, dataset string) ([32]byte, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return [32]byte{}, err
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)
	stream, err := client.DoExchange(context.Background())
	if err != nil {
		return [32]byte{}, err
	}

	// Request root (empty path)
	req := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Path: []string{dataset},
			Cmd:  []byte("merkle_node"),
		},
	}
	if err := stream.Send(req); err != nil {
		return [32]byte{}, err
	}
	if err := stream.CloseSend(); err != nil {
		return [32]byte{}, err
	}

	resp, err := stream.Recv()
	if err != nil {
		return [32]byte{}, err
	}

	if len(resp.DataBody) < 32 {
		return [32]byte{}, fmt.Errorf("invalid merkle response")
	}

	var root [32]byte
	copy(root[:], resp.DataBody[0:32])
	return root, nil
}
