package mesh

import (
	"encoding/binary"
	"errors"
	"time"
)

type MemberStatus uint8

const (
	StatusAlive   MemberStatus = 0
	StatusSuspect MemberStatus = 1
	StatusDead    MemberStatus = 2
)

// Member represents a peer in the mesh.
type Member struct {
	ID          string
	Addr        string // Gossip Host:Port (UDP)
	GRPCAddr    string // Service Host:Port (gRPC Data)
	MetaAddr    string // Service Host:Port (gRPC Meta)
	Status      MemberStatus
	Incarnation uint32
	LastSeen    time.Time
	SuspectAt   time.Time // When the node was marked Suspect
}

// EncodeMember serializes a member update into a compact binary format.
// Format:
// ID Len (1) | ID | Addr Len (1) | Addr | GRPCAddr Len (1) | GRPCAddr | MetaAddr Len (1) | MetaAddr | Status (1) | Incarnation (4)
func EncodeMember(m *Member, dst []byte) (int, error) {
	offset := 0

	// ID
	idLen := len(m.ID)
	if idLen > 255 {
		return 0, errors.New("id too long")
	}
	if len(dst) < 1+idLen {
		return 0, errors.New("buffer too small")
	}
	dst[offset] = byte(idLen)
	offset++
	copy(dst[offset:], m.ID)
	offset += idLen

	// Addr
	addrLen := len(m.Addr)
	if addrLen > 255 {
		return 0, errors.New("addr too long")
	}
	if len(dst) < offset+1+addrLen {
		return 0, errors.New("buffer too small")
	}
	dst[offset] = byte(addrLen)
	offset++
	copy(dst[offset:], m.Addr)
	offset += addrLen

	// GRPCAddr
	grpcLen := len(m.GRPCAddr)
	if grpcLen > 255 {
		return 0, errors.New("grpc addr too long")
	}
	if len(dst) < offset+1+grpcLen {
		return 0, errors.New("buffer too small")
	}
	dst[offset] = byte(grpcLen)
	offset++
	copy(dst[offset:], m.GRPCAddr)
	offset += grpcLen

	// MetaAddr
	metaLen := len(m.MetaAddr)
	if metaLen > 255 {
		return 0, errors.New("meta addr too long")
	}
	if len(dst) < offset+1+metaLen {
		return 0, errors.New("buffer too small")
	}
	dst[offset] = byte(metaLen)
	offset++
	copy(dst[offset:], m.MetaAddr)
	offset += metaLen

	// Status & Incarnation
	if len(dst) < offset+1+4 {
		return 0, errors.New("buffer too small")
	}
	dst[offset] = byte(m.Status)
	offset++
	binary.BigEndian.PutUint32(dst[offset:], m.Incarnation)
	offset += 4

	return offset, nil
}

// DecodeMember parses a member from src.
func DecodeMember(src []byte) (*Member, int, error) {
	offset := 0
	if len(src) < 1 {
		return nil, 0, errors.New("packet too short")
	}
	idLen := int(src[offset])
	offset++
	if len(src) < offset+idLen {
		return nil, 0, errors.New("packet too short")
	}
	id := string(src[offset : offset+idLen])
	offset += idLen

	if len(src) < offset+1 {
		return nil, 0, errors.New("packet too short")
	}
	addrLen := int(src[offset])
	offset++
	if len(src) < offset+addrLen {
		return nil, 0, errors.New("packet too short")
	}
	addr := string(src[offset : offset+addrLen])
	offset += addrLen

	// GRPCAddr
	if len(src) < offset+1 {
		return nil, 0, errors.New("packet too short")
	}
	grpcLen := int(src[offset])
	offset++
	if len(src) < offset+grpcLen {
		return nil, 0, errors.New("packet too short")
	}
	grpcAddr := string(src[offset : offset+grpcLen])
	offset += grpcLen

	// MetaAddr
	if len(src) < offset+1 {
		return nil, 0, errors.New("packet too short")
	}
	metaLen := int(src[offset])
	offset++
	if len(src) < offset+metaLen {
		return nil, 0, errors.New("packet too short")
	}
	metaAddr := string(src[offset : offset+metaLen])
	offset += metaLen

	if len(src) < offset+5 {
		return nil, 0, errors.New("packet too short")
	}
	status := MemberStatus(src[offset])
	offset++
	inc := binary.BigEndian.Uint32(src[offset:])
	offset += 4

	return &Member{
		ID:          id,
		Addr:        addr,
		GRPCAddr:    grpcAddr,
		MetaAddr:    metaAddr,
		Status:      status,
		Incarnation: inc,
	}, offset, nil
}

// Resolution Rules (SWIM)
// - Alive overrides Suspect if Incarnation > known
// - Suspect overrides Alive if Incarnation >= known
// - Dead overrides all
func (m *Member) Empty() bool {
	return m.ID == ""
}
