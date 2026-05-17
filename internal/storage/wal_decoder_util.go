package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// DecodeWALBlock decodes a single raw WAL block (as written to disk) into entries.
func DecodeWALBlock(data []byte, mem memory.Allocator) ([]DecodedWALEntry, error) {
	if len(data) < 32 {
		return nil, fmt.Errorf("block too short")
	}

	// Parse Header
	seq := binary.LittleEndian.Uint64(data[4:12])
	ts := int64(binary.LittleEndian.Uint64(data[12:20])) // #nosec G115
	nameLen := binary.LittleEndian.Uint32(data[20:24])
	recLen := binary.LittleEndian.Uint64(data[24:32])

	if uint64(len(data)) < 32+uint64(nameLen)+recLen {
		return nil, fmt.Errorf("block truncated")
	}

	name := string(data[32 : 32+nameLen])
	recBytes := data[32+nameLen : 32+uint64(nameLen)+recLen]

	storedChecksum := binary.LittleEndian.Uint32(data[0:4])
	crc := crc32.NewIEEE()
	_, _ = crc.Write([]byte(name))
	_, _ = crc.Write(recBytes)
	calculatedCRC := crc.Sum32()

	isCompressed := (storedChecksum == 0xFFFFFFFF)
	if !isCompressed && calculatedCRC != storedChecksum {
		return nil, fmt.Errorf("crc mismatch")
	}

	var results []DecodedWALEntry

	if isCompressed {
		if len(name) != 1 {
			return nil, fmt.Errorf("invalid compressed block name")
		}
		compType := name[0]

		var decompressed []byte
		var err error
		switch compType {
		case 1:
			decompressed, err = snappy.Decode(nil, recBytes)
		case 2:
			decoder, _ := zstd.NewReader(nil)
			decompressed, err = decoder.DecodeAll(recBytes, nil)
		case 3:
			decompressed = make([]byte, ts)
			_, err = lz4.UncompressBlock(recBytes, decompressed)
		default:
			err = fmt.Errorf("unknown compression %d", compType)
		}
		if err != nil {
			return nil, err
		}

		dr := bytes.NewReader(decompressed)
		innerHeader := make([]byte, 32)
		for {
			if _, err := io.ReadFull(dr, innerHeader); err != nil {
				break
			}
			inSeq := binary.LittleEndian.Uint64(innerHeader[4:12])
			inTs := int64(binary.LittleEndian.Uint64(innerHeader[12:20])) // #nosec G115
			inNameLen := binary.LittleEndian.Uint32(innerHeader[20:24])
			inRecLen := binary.LittleEndian.Uint64(innerHeader[24:32])

			inNameBytes := make([]byte, inNameLen)
			if _, err := io.ReadFull(dr, inNameBytes); err != nil {
				break
			}
			inRecBytes := make([]byte, inRecLen)
			if _, err := io.ReadFull(dr, inRecBytes); err != nil {
				break
			}

			r, err := ipc.NewReader(bytes.NewReader(inRecBytes), ipc.WithAllocator(mem))
			if err == nil {
				if r.Next() {
					rec := r.RecordBatch()
					rec.Retain()
					results = append(results, DecodedWALEntry{
						Name:   string(inNameBytes),
						Record: rec,
						Seq:    inSeq,
						Ts:     inTs,
					})
				}
				r.Release()
			}
		}
	} else {
		r, err := ipc.NewReader(bytes.NewReader(recBytes), ipc.WithAllocator(mem))
		if err == nil {
			if r.Next() {
				rec := r.RecordBatch()
				rec.Retain()
				results = append(results, DecodedWALEntry{
					Name:   name,
					Record: rec,
					Seq:    seq,
					Ts:     ts,
				})
			}
			r.Release()
		}
	}

	return results, nil
}
