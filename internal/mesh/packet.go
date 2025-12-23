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
	// HeaderSize: Type(1) + Flags(1) + Seq(4) + NumUpdates(1)
	HeaderSize = 7

	// Flags
	FlagCompressed uint8 = 0x01
)

// Packet represents a gossip message.
// It is designed to map directly to a byte buffer.
type Packet struct {
	Type       PacketType
	Flags      uint8
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
	dst[1] = p.Flags
	binary.BigEndian.PutUint32(dst[2:6], p.Seq)
	dst[6] = p.NumUpdates

	// Copy payload
	n := copy(dst[7:], p.Payload)
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
		Flags:      src[1],
		Seq:        binary.BigEndian.Uint32(src[2:6]),
		NumUpdates: src[6],
		Payload:    src[7:],
	}
	return p, nil
}
