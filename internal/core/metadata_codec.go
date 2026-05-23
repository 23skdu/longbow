package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
)

// Metadata Type IDs
const (
	TypeNil     uint8 = 0
	TypeString  uint8 = 1
	TypeInt64   uint8 = 2
	TypeFloat64 uint8 = 3
	TypeBool    uint8 = 4
	TypeBinary  uint8 = 5
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
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(metadata))); err != nil { // #nosec G115 -- intentional conversion for binary write
		return nil, err
	}

	for k, v := range metadata {
		// key_len (uint16)
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(k))); err != nil { // #nosec G115 -- intentional conversion for binary write
			return nil, err
		}
		// key
		if _, err := buf.WriteString(k); err != nil {
			return nil, err
		}

		if v == nil {
			buf.WriteByte(TypeNil)
			if err := binary.Write(buf, binary.LittleEndian, uint32(0)); err != nil {
				return nil, err
			}
			continue
		}

		switch val := v.(type) {
		case string:
			buf.WriteByte(TypeString)
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(val))); err != nil { // #nosec G115 -- intentional conversion for binary write
				return nil, err
			}
			if _, err := buf.WriteString(val); err != nil {
				return nil, err
			}
		case int64:
			buf.WriteByte(TypeInt64)
			if err := binary.Write(buf, binary.LittleEndian, uint32(8)); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, val); err != nil {
				return nil, err
			}
		case int:
			buf.WriteByte(TypeInt64)
			if err := binary.Write(buf, binary.LittleEndian, uint32(8)); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, int64(val)); err != nil {
				return nil, err
			}
		case float64:
			buf.WriteByte(TypeFloat64)
			if err := binary.Write(buf, binary.LittleEndian, uint32(8)); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, val); err != nil {
				return nil, err
			}
		case bool:
			buf.WriteByte(TypeBool)
			if err := binary.Write(buf, binary.LittleEndian, uint32(1)); err != nil {
				return nil, err
			}
			if val {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
		case []byte:
			buf.WriteByte(TypeBinary)
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(val))); err != nil { // #nosec G115 -- intentional conversion for binary write
				return nil, err
			}
			if _, err := buf.Write(val); err != nil {
				return nil, err
			}
		default:
			// Fallback to string representation for unknown types
			s := fmt.Sprintf("%v", val)
			buf.WriteByte(TypeString)
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(s))); err != nil { // #nosec G115 -- intentional conversion for binary write
				return nil, err
			}
			if _, err := buf.WriteString(s); err != nil {
				return nil, err
			}
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
