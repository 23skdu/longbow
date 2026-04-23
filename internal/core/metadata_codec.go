package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
)

// Metadata Type IDs
const (
	TypeNil    uint8 = 0
	TypeString uint8 = 1
	TypeInt64  uint8 = 2
	TypeFloat64 uint8 = 3
	TypeBool   uint8 = 4
	TypeBinary uint8 = 5
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func getBuffer() *bytes.Buffer {
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func putBuffer(b *bytes.Buffer) {
	bufferPool.Put(b)
}

// EncodeMetadata serializes a map to a compact binary format.
func EncodeMetadata(metadata map[string]interface{}) ([]byte, error) {
	if metadata == nil {
		return nil, nil
	}

	buf := getBuffer()
	defer putBuffer(buf)

	// num_fields (uint32)
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(metadata))); err != nil {
		return nil, err
	}

	for k, v := range metadata {
		// key_len (uint16)
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(k))); err != nil {
			return nil, err
		}
		// key
		buf.WriteString(k)

		if v == nil {
			buf.WriteByte(TypeNil)
			binary.Write(buf, binary.LittleEndian, uint32(0))
			continue
		}

		switch val := v.(type) {
		case string:
			buf.WriteByte(TypeString)
			binary.Write(buf, binary.LittleEndian, uint32(len(val)))
			buf.WriteString(val)
		case int64:
			buf.WriteByte(TypeInt64)
			binary.Write(buf, binary.LittleEndian, uint32(8))
			binary.Write(buf, binary.LittleEndian, val)
		case int:
			buf.WriteByte(TypeInt64)
			binary.Write(buf, binary.LittleEndian, uint32(8))
			binary.Write(buf, binary.LittleEndian, int64(val))
		case float64:
			buf.WriteByte(TypeFloat64)
			binary.Write(buf, binary.LittleEndian, uint32(8))
			binary.Write(buf, binary.LittleEndian, val)
		case bool:
			buf.WriteByte(TypeBool)
			binary.Write(buf, binary.LittleEndian, uint32(1))
			if val {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
		case []byte:
			buf.WriteByte(TypeBinary)
			binary.Write(buf, binary.LittleEndian, uint32(len(val)))
			buf.Write(val)
		default:
			// Fallback to string representation for unknown types
			s := fmt.Sprintf("%v", val)
			buf.WriteByte(TypeString)
			binary.Write(buf, binary.LittleEndian, uint32(len(s)))
			buf.WriteString(s)
		}
	}

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res, nil
}

// DecodeMetadata deserializes binary metadata back to a map.
func DecodeMetadata(data []byte) (map[string]interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}

	reader := bytes.NewReader(data)
	var numFields uint32
	if err := binary.Read(reader, binary.LittleEndian, &numFields); err != nil {
		return nil, err
	}

	res := make(map[string]interface{}, numFields)
	for i := uint32(0); i < numFields; i++ {
		var keyLen uint16
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}

		keyBuf := make([]byte, keyLen)
		if _, err := reader.Read(keyBuf); err != nil {
			return nil, err
		}
		key := string(keyBuf)

		var typeID uint8
		if err := binary.Read(reader, binary.LittleEndian, &typeID); err != nil {
			return nil, err
		}

		var valLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &valLen); err != nil {
			return nil, err
		}

		switch typeID {
		case TypeNil:
			res[key] = nil
		case TypeString:
			valBuf := make([]byte, valLen)
			if _, err := reader.Read(valBuf); err != nil {
				return nil, err
			}
			res[key] = string(valBuf)
		case TypeInt64:
			var v int64
			if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
				return nil, err
			}
			res[key] = v
		case TypeFloat64:
			var v float64
			if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
				return nil, err
			}
			res[key] = v
		case TypeBool:
			v, err := reader.ReadByte()
			if err != nil {
				return nil, err
			}
			res[key] = v != 0
		case TypeBinary:
			valBuf := make([]byte, valLen)
			if _, err := reader.Read(valBuf); err != nil {
				return nil, err
			}
			res[key] = valBuf
		default:
			// Skip unknown type
			if _, err := reader.Seek(int64(valLen), 1); err != nil {
				return nil, err
			}
		}
	}

	return res, nil
}
