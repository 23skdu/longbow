package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/rs/zerolog/log"
)

// rawWALBlock represents a raw block read from disk
type rawWALBlock struct {
	header   [32]byte
	name     string
	recBytes []byte
	seq      uint64
	ts       int64
	err      error
}

// decodedWALEntry represents a fully decoded entry ready for application
type decodedWALEntry struct {
	name   string
	record arrow.RecordBatch
	seq    uint64
	ts     int64
	err    error
}

// ReplayWAL reads the WAL and calls the applier for each entry.
// Utilizes a pipelined architecture (Reader -> Decoders -> Applier) to maximize throughput.
// Returns the maximum sequence number encountered.
func (e *StorageEngine) ReplayWAL(applier ApplierFunc) (uint64, error) {
	start := time.Now()
	defer func() {
		metrics.WalReplayDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	walPath := filepath.Join(e.dataPath, walFileName)
	f, err := os.Open(walPath)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error().Err(err).Msg("failed to close WAL file during replay")
		}
	}()

	// Pipeline channels
	// rawChan needs sufficient buffer to keep reader ahead
	// Increased buffer size for better throughput
	rawChan := make(chan rawWALBlock, 1000)
	// decodedChan holds ready-to-apply records. Retain/Release must be handled carefully.
	// Increased buffer size
	decodedChan := make(chan decodedWALEntry, 1000)

	// Context for cancellation? We'll just use close signals.

	// 1. Start Reader Goroutine
	go e.walReaderRoutine(f, rawChan)

	numDecoders := runtime.NumCPU()
	if numDecoders < 1 {
		numDecoders = 1
	}
	if numDecoders > 8 {
		numDecoders = 8
	}

	log.Info().Int("numDecoders", numDecoders).Msg("ReplayWAL: Starting parallel decoders")

	var wgDecoders sync.WaitGroup
	wgDecoders.Add(numDecoders)

	reorderedChan := make(chan decodedWALEntry, 100)

	go e.reorderBufferRoutine(decodedChan, reorderedChan, &wgDecoders, numDecoders)

	for i := 0; i < numDecoders; i++ {
		go func(id int) {
			defer wgDecoders.Done()
			e.walDecoderRoutine(rawChan, decodedChan)
		}(i)
	}

	// 3. Main Loop: Applier (read from reorderedChan to get properly ordered entries)
	var maxSeq uint64
	count := 0

	for entry := range reorderedChan {
		if entry.err != nil {
			// Stop immediately on error
			// We should probably drain/cancel others, but simpler to return
			return maxSeq, entry.err
		}

		// Update maxSeq
		if entry.seq > maxSeq {
			maxSeq = entry.seq
		}

		// Apply
		rec := entry.record
		log.Debug().
			Uint64("seq", entry.seq).
			Str("name", entry.name).
			Int64("rows", rec.NumRows()).
			Msg("ReplayWAL: Applying record")

		err := applier(entry.name, rec, entry.seq, entry.ts)

		// We are done with the record
		rec.Release()

		if err != nil {
			return maxSeq, fmt.Errorf("applier failed for record: %w", err)
		}
		count++
	}

	return maxSeq, nil
}

func (e *StorageEngine) walReaderRoutine(f *os.File, out chan<- rawWALBlock) {
	defer close(out)

	count := 0
	for {
		header := make([]byte, 32)
		// Use ReadFull to ensure we get the whole header or EOF
		if _, err := io.ReadFull(f, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			out <- rawWALBlock{err: fmt.Errorf("header read error at count %d: %w", count, err)}
			return
		}

		// Parse Header (lightweight)
		seq := binary.LittleEndian.Uint64(header[4:12])
		ts := int64(binary.LittleEndian.Uint64(header[12:20]))
		nameLen := binary.LittleEndian.Uint32(header[20:24])
		recLen := binary.LittleEndian.Uint64(header[24:32])

		// Logic check
		if nameLen > 1024*1024 || recLen > 1024*1024*1024 {
			log.Warn().Uint32("nameLen", nameLen).Uint64("recLen", recLen).Msg("ReplayWAL: skipping record with excessive length")
			break
		}

		// Read Name
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(f, nameBytes); err != nil {
			out <- rawWALBlock{err: fmt.Errorf("read name error: %w", err)}
			return
		}
		name := string(nameBytes)

		// Read Body
		recBytes := make([]byte, recLen)
		if _, err := io.ReadFull(f, recBytes); err != nil {
			out <- rawWALBlock{err: fmt.Errorf("read record error: %w", err)}
			return
		}

		// Send to decoder
		out <- rawWALBlock{
			header:   *(*[32]byte)(header),
			name:     name,
			recBytes: recBytes,
			seq:      seq,
			ts:       ts,
		}
		count++
	}
}

func (e *StorageEngine) walDecoderRoutine(in <-chan rawWALBlock, out chan<- decodedWALEntry) {
	for block := range in {
		if block.err != nil {
			out <- decodedWALEntry{err: block.err}
			return
		}

		storedChecksum := binary.LittleEndian.Uint32(block.header[0:4])

		// Validate CRC
		// Note: CRC calc is CPU bound, so good to be in decoder
		crc := crc32.NewIEEE()
		_, _ = crc.Write([]byte(block.name))
		_, _ = crc.Write(block.recBytes)
		calculatedCRC := crc.Sum32()

		// Metadata for compressed blocks
		isCompressed := (storedChecksum == 0xFFFFFFFF)

		if !isCompressed && calculatedCRC != storedChecksum {
			out <- decodedWALEntry{err: fmt.Errorf("wal crc mismatch at seq %d: expected %x, got %x", block.seq, storedChecksum, calculatedCRC)}
			return
		}

		if isCompressed {
			// Handle Compressed Block
			if len(block.name) != 1 {
				continue
			}
			compType := block.name[0]

			var decompressed []byte
			var err error
			switch compType {
			case 1: // Snappy
				decompressed, err = snappy.Decode(nil, block.recBytes)
			case 2: // Zstd
				decoder, _ := zstd.NewReader(nil)
				decompressed, err = decoder.DecodeAll(block.recBytes, nil)
			case 3: // LZ4
				rawSize := block.ts
				decompressed = make([]byte, rawSize)
				_, err = lz4.UncompressBlock(block.recBytes, decompressed)
			default:
				err = fmt.Errorf("unknown compression type: %d", compType)
			}

			if err != nil {
				log.Warn().Err(err).Uint32("type", uint32(compType)).Msg("ReplayWAL: failed to decompress block")
				continue
			}

			// Reader for the decompressed blob
			// Contains multiple records potentially
			dr := bytes.NewReader(decompressed)
			innerHeader := make([]byte, 32)
			for {
				if _, err := io.ReadFull(dr, innerHeader); err != nil {
					break
				}
				inSeq := binary.LittleEndian.Uint64(innerHeader[4:12])
				inTs := int64(binary.LittleEndian.Uint64(innerHeader[12:20]))
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

				// IPC Decode
				r, err := ipc.NewReader(bytes.NewReader(inRecBytes), ipc.WithAllocator(e.mem))
				if err == nil {
					if r.Next() {
						rec := r.RecordBatch()
						rec.Retain()

						out <- decodedWALEntry{
							name:   string(inNameBytes),
							record: rec,
							seq:    inSeq,
							ts:     inTs,
						}
					}
					r.Release()
				}
			}

		} else {
			// Handle Uncompressed Record
			r, err := ipc.NewReader(bytes.NewReader(block.recBytes), ipc.WithAllocator(e.mem))
			if err == nil {
				if r.Next() {
					rec := r.RecordBatch()
					rec.Retain()

					out <- decodedWALEntry{
						name:   block.name,
						record: rec,
						seq:    block.seq,
						ts:     block.ts,
					}
				}
				r.Release()
			}
		}
	}
}

// reorderBufferRoutine reorders decoded entries by sequence number. It collects entries from multiple decoder goroutines and outputs them in order.
func (e *StorageEngine) reorderBufferRoutine(in chan decodedWALEntry, out chan decodedWALEntry, wgDecoders *sync.WaitGroup, numDecoders int) {
	defer close(out)

	// Map to hold out-of-order entries
	buffer := make(map[uint64]decodedWALEntry)
	nextSeq := uint64(0)
	activeDecoders := numDecoders

	// Track decoder completion - close input channel when all decoders are done
	go func() {
		wgDecoders.Wait()
		close(in)
	}()

	for {
		select {
		case entry, ok := <-in:
			if !ok {
				// Input channel closed, all decoders are done
				activeDecoders--
				if activeDecoders == 0 {
					// Flush remaining buffer
					for len(buffer) > 0 {
						if entry, exists := buffer[nextSeq]; exists {
							delete(buffer, nextSeq)
							out <- entry
							nextSeq++
						} else {
							// We're missing entries, this shouldn't happen
							// But we need to skip to next available
							for seq := range buffer {
								if seq > nextSeq {
									nextSeq = seq
									break
								}
							}
						}
					}
					return
				}
				continue
			}

			// Handle errors immediately
			if entry.err != nil {
				out <- entry
				return
			}

			// If this is the expected next sequence, output it
			if entry.seq == nextSeq {
				out <- entry
				nextSeq++

				// Check if we can output more from buffer
				for {
					if buffered, exists := buffer[nextSeq]; exists {
						delete(buffer, nextSeq)
						out <- buffered
						nextSeq++
					} else {
						break
					}
				}
			} else if entry.seq > nextSeq {
				// Store out-of-order entry in buffer
				buffer[entry.seq] = entry
			} else {
				// This shouldn't happen (seq < nextSeq), but handle it
				log.Warn().
					Uint64("seq", entry.seq).
					Uint64("nextSeq", nextSeq).
					Msg("ReplayWAL: Received entry with seq < nextSeq, dropping")
			}
		}
	}
}
