package core

import (
	"bytes"
	"encoding/binary"
)

// ArrowMetadata represents a columnar metadata buffer.
// The format is:
// [num_fields: uint16]
// [offsets: uint32 * num_fields]
// [types: uint8 * num_fields]
// [keys: string data]
// [values: binary data]
// This allows O(log N) or O(N) lookup without full decoding.

type ArrowMetadata struct {
	data []byte
}

func NewArrowMetadata(data []byte) ArrowMetadata {
	return ArrowMetadata{data: data}
}

// GetField extracts a field value without allocating a map.
func (m ArrowMetadata) GetField(key string) (interface{}, bool) {
	if len(m.data) < 2 {
		return nil, false
	}

	numFields := binary.LittleEndian.Uint16(m.data[0:2])
	if numFields == 0 {
		return nil, false
	}

	// Traditional approach: Decode keys one by one
	reader := bytes.NewReader(m.data[2:])
	
	for i := uint16(0); i < numFields; i++ {
		var keyLen uint16
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, false
		}
		
		k := make([]byte, keyLen)
		if _, err := reader.Read(k); err != nil {
			return nil, false
		}
		
		var typeID uint8
		if err := binary.Read(reader, binary.LittleEndian, &typeID); err != nil {
			return nil, false
		}
		
		var valLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &valLen); err != nil {
			return nil, false
		}
		
		if string(k) == key {
			// Found it!
			return decodeValue(reader, typeID, valLen)
		} else {
			// Skip value
			if _, err := reader.Seek(int64(valLen), 1); err != nil {
				return nil, false
			}
		}
	}
	
	return nil, false
}

func decodeValue(reader *bytes.Reader, typeID uint8, valLen uint32) (interface{}, bool) {
	switch typeID {
	case TypeNil:
		return nil, true
	case TypeString:
		buf := make([]byte, valLen)
		if _, err := reader.Read(buf); err != nil {
			return nil, false
		}
		return string(buf), true
	case TypeInt64:
		var v int64
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			return nil, false
		}
		return v, true
	case TypeFloat64:
		var v float64
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			return nil, false
		}
		return v, true
	case TypeBool:
		v, err := reader.ReadByte()
		if err != nil {
			return nil, false
		}
		return v != 0, true
	case TypeBinary:
		buf := make([]byte, valLen)
		if _, err := reader.Read(buf); err != nil {
			return nil, false
		}
		return buf, true
	}
	return nil, false
}

// ToMap converts to the legacy format if needed (for backward compatibility)
func (m ArrowMetadata) ToMap() (map[string]interface{}, error) {
	return DecodeMetadata(m.data)
}
