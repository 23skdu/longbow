// Package sync manages background delta replication from peers.
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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SyncStore defines the interface required by SyncWorker to interact with the local store.
// This breaks the circular dependency between mesh/sync and store packages.
type SyncStore interface {
	ApplyDelta(datasetName string, rec arrow.RecordBatch, seq uint64, ts int64) error
	MerkleRoot(name string) [32]byte
	GetMeshMembers() []mesh.Member
}

// SyncPriority defines how often to check a peer for updates
type SyncPriority int

const (
	PriorityHigh   SyncPriority = iota // Check every 100ms
	PriorityNormal                     // Check every 1s
	PriorityLow                        // Check every 10s
)

// SyncWorker manages background delta replication from peers
type SyncWorker struct {
	store  SyncStore
	logger zerolog.Logger

	peers  map[string]*PeerState
	mu     sync.RWMutex
	stopCh chan struct{}
}

type PeerState struct {
	Addr        string
	LastSeenSeq uint64
	Priority    SyncPriority
}

func NewSyncWorker(s SyncStore, logger *zerolog.Logger) *SyncWorker {
	return &SyncWorker{
		store:  s,
		logger: *logger,
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
	members := w.store.GetMeshMembers()
	for i := range members {
		m := &members[i]
		// Skip non-alive nodes
		if m.Status == mesh.StatusAlive {
			// We can't easily check identity without more store info,
			// but AddPeer handles duplicates if needed.
			// Ideally we'd know our own ID here too.
			w.AddPeer(m.GRPCAddr)
		}
	}

	w.mu.RLock()
	peers := make([]*PeerState, 0, len(w.peers))
	for _, p := range w.peers {
		peers = append(peers, p)
	}
	w.mu.RUnlock()

	for _, p := range peers {
		if err := w.syncPeer(p); err != nil {
			w.logger.Warn().
				Str("addr", p.Addr).
				Err(err).
				Msg("Failed to sync with peer")
		}
	}
}

func (w *SyncWorker) syncPeer(p *PeerState) error {
	// 1. Compare Merkle Roots early to avoid unnecessary sync connection
	// We use "ds1" as default example for now, ideally this would be per-dataset
	remoteRoot, err := w.fetchRemoteMerkleRoot(p.Addr, "ds1")
	if err == nil {
		localRoot := w.store.MerkleRoot("ds1")
		if remoteRoot == localRoot {
			metrics.MeshMerkleMatchTotal.WithLabelValues("match").Inc()
			w.logger.Info().Str("peer", p.Addr).Msg("Merkle roots match, skipping sync")
			return nil
		}
		metrics.MeshMerkleMatchTotal.WithLabelValues("mismatch").Inc()
		w.logger.Info().Str("peer", p.Addr).Msg("Merkle roots differ, starting sync")
	}

	w.logger.Info().
		Str("addr", p.Addr).
		Uint64("last_seq", p.LastSeenSeq).
		Msg("Syncing with peer")

	// 2. Connect to peer
	conn, err := grpc.NewClient(p.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	client := flight.NewFlightServiceClient(conn)

	// 3. Start DoExchange for sync
	stream, err := client.DoExchange(context.Background())
	if err != nil {
		return err
	}

	// 4. Send "sync" request with last seen seq
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

	// 5. Receive deltas
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if err != io.EOF {
				w.logger.Error().Err(err).Msg("Recv error")
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
			w.logger.Error().Err(err).Msg("Failed to decode sync record")
			continue
		}

		if r.Next() {
			rec := r.RecordBatch()
			rec.Retain()

			// Apply to local store
			if err := w.store.ApplyDelta(datasetName, rec, seq, ts); err != nil {
				metrics.MeshSyncDeltasTotal.WithLabelValues("error").Inc()
				w.logger.Error().
					Str("dataset", datasetName).
					Err(err).
					Msg("Failed to apply delta")
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
	defer func() { _ = conn.Close() }()

	client := flight.NewFlightServiceClient(conn)
	stream, err := client.DoExchange(context.Background())
	if err != nil {
		return [32]byte{}, err
	}

	// Request root (empty path)
	req := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Cmd:  []byte("root"),
			Path: []string{dataset},
		},
	}

	if err := stream.Send(req); err != nil {
		return [32]byte{}, err
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return [32]byte{}, err
		}

		// Simple: first 32 bytes are the root
		if len(resp.DataBody) >= 32 {
			return [32]byte(resp.DataBody[:32]), nil
		}
	}

	return [32]byte{}, fmt.Errorf("root not found")
}
