package mesh

import (
	"encoding/binary"
	"errors"
)

// PacketType identifies the message intent.
type PacketType uint8

const (
	PacketPing    PacketType = 1
	PacketAck     PacketType = 2
	PacketPingReq PacketType = 3
)

const (
	// HeaderSize: Type(1) + Seq(4) + NumUpdates(1)
	HeaderSize = 6
)

// Packet represents a gossip message.
// It is designed to map directly to a byte buffer.
type Packet struct {
	Type       PacketType
	Seq        uint32
	NumUpdates uint8
	Payload    []byte // Remaining bytes (membership updates)
}

// EncodePacket writes the packet header to dst.
// Retuns the number of bytes written.
// dst must be at least HeaderSize + len(payload).
func EncodePacket(p *Packet, dst []byte) (int, error) {
	if len(dst) < HeaderSize+len(p.Payload) {
		return 0, errors.New("buffer too small")
	}

	dst[0] = byte(p.Type)
	binary.BigEndian.PutUint32(dst[1:5], p.Seq)
	dst[5] = p.NumUpdates

	// Copy payload
	n := copy(dst[6:], p.Payload)
	return HeaderSize + n, nil
}

// DecodePacket parses a packet from src.
// The returned Packet's Payload relies on the underlying src array (zero-copy).
func DecodePacket(src []byte) (*Packet, error) {
	if len(src) < HeaderSize {
		return nil, errors.New("packet too short")
	}

	p := &Packet{
		Type:       PacketType(src[0]),
		Seq:        binary.BigEndian.Uint32(src[1:5]),
		NumUpdates: src[5],
		Payload:    src[6:],
	}
	return p, nil
}
