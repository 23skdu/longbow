package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
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

// DecodedWALEntry represents a fully decoded entry ready for application
type DecodedWALEntry struct {
	Name   string
	Record arrow.RecordBatch
	Seq    uint64
	Ts     int64
	Err    error
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
	f, err := os.Open(filepath.Clean(walPath)) // #nosec G304
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
	decodedChan := make(chan DecodedWALEntry, 1000)

	// Context for cancellation? We'll just use close signals.

	firstSeqChan := make(chan uint64, 1)

	// 1. Start Reader Goroutine
	go e.walReaderRoutine(f, rawChan, firstSeqChan)

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

	reorderedChan := make(chan DecodedWALEntry, 100)

	go e.reorderBufferRoutine(decodedChan, reorderedChan, &wgDecoders, firstSeqChan)

	for i := 0; i < numDecoders; i++ {
		go func(id int) {
			defer wgDecoders.Done()
			e.walDecoderRoutine(rawChan, decodedChan)
		}(i)
	}

	// 3. Parallel Appliers
	numAppliers := runtime.NumCPU()
	if numAppliers > 16 {
		numAppliers = 16
	}
	if numAppliers < 1 {
		numAppliers = 1
	}
	metrics.WalReplayParallelism.Set(float64(numAppliers))
	defer metrics.WalReplayParallelism.Set(0)

	applierChans := make([]chan DecodedWALEntry, numAppliers)
	var wgAppliers sync.WaitGroup
	var applierErr atomic.Value

	for i := 0; i < numAppliers; i++ {
		applierChans[i] = make(chan DecodedWALEntry, 100)
		wgAppliers.Add(1)
		go func(ch chan DecodedWALEntry) {
			defer wgAppliers.Done()
			for entry := range ch {
				if applierErr.Load() != nil {
					entry.Record.Release()
					continue
				}

				log.Debug().
					Uint64("seq", entry.Seq).
					Str("name", entry.Name).
					Int64("rows", entry.Record.NumRows()).
					Msg("ReplayWAL: Applying record")

				err := applier(entry.Name, entry.Record, entry.Seq, entry.Ts)
				entry.Record.Release()
				if err != nil {
					applierErr.Store(err)
				}
			}
		}(applierChans[i])
	}

	// 4. Main Dispatch Loop
	var maxSeq uint64
	count := 0

	for entry := range reorderedChan {
		if entry.Err != nil {
			applierErr.Store(entry.Err)
			break
		}

		if errAny := applierErr.Load(); errAny != nil {
			if entry.Record != nil {
				entry.Record.Release()
			}
			break
		}

		// Update maxSeq
		if entry.Seq > maxSeq {
			maxSeq = entry.Seq
		}

		// Dispatch by hashed name to ensure per-dataset ordering
		h := fnv.New32a()
		_, _ = h.Write([]byte(entry.Name))
		workerIdx := h.Sum32() % uint32(numAppliers)

		applierChans[workerIdx] <- entry
		count++
	}

	// Close applier channels and wait
	for i := 0; i < numAppliers; i++ {
		close(applierChans[i])
	}
	wgAppliers.Wait()

	if errAny := applierErr.Load(); errAny != nil {
		return maxSeq, errAny.(error)
	}

	log.Info().Int("totalApplied", count).Msg("ReplayWAL: Completed successfully")
	return maxSeq, nil
}

func (e *StorageEngine) walReaderRoutine(f *os.File, out chan<- rawWALBlock, firstSeqChan chan<- uint64) {
	defer close(out)
	defer close(firstSeqChan)

	count := 0
	first := true
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

		if first {
			firstSeqChan <- seq
			first = false
		}
		ts := int64(binary.LittleEndian.Uint64(header[12:20])) // #nosec G115
		nameLen := binary.LittleEndian.Uint32(header[20:24])
		recLen := binary.LittleEndian.Uint64(header[24:32])

		// Logic check
		if nameLen > 1024*1024 || recLen > 1024*1024*1024 {
			log.Warn().Uint32("nameLen", nameLen).Uint64("recLen", recLen).Msg("ReplayWAL: skipping record with excessive length")
			// Skip this record's body bytes and continue to next entry
			skipName := make([]byte, nameLen)
			_, _ = io.ReadFull(f, skipName)
			skipRec := make([]byte, recLen)
			_, _ = io.ReadFull(f, skipRec)
			continue
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

func (e *StorageEngine) walDecoderRoutine(in <-chan rawWALBlock, out chan<- DecodedWALEntry) {
	for block := range in {
		if block.err != nil {
			out <- DecodedWALEntry{Err: block.err}
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
			out <- DecodedWALEntry{Err: fmt.Errorf("wal crc mismatch at seq %d: expected %x, got %x", block.seq, storedChecksum, calculatedCRC)}
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
				out <- DecodedWALEntry{Err: fmt.Errorf("wal decompression failed at seq %d type %d: %w", block.seq, compType, err)}
				return
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

				// IPC Decode
				r, err := ipc.NewReader(bytes.NewReader(inRecBytes), ipc.WithAllocator(e.mem))
				if err != nil {
					log.Warn().Err(err).Uint64("seq", inSeq).Msg("ReplayWAL: IPC decode failed for compressed inner record")
					continue
				}
				if r.Next() {
					rec := r.RecordBatch()
					rec.Retain()

					out <- DecodedWALEntry{
						Name:   string(inNameBytes),
						Record: rec,
						Seq:    inSeq,
						Ts:     inTs,
					}
				}
				r.Release()
			}

		} else {
			// Handle Uncompressed Record
			r, err := ipc.NewReader(bytes.NewReader(block.recBytes), ipc.WithAllocator(e.mem))
			if err != nil {
				log.Warn().Err(err).Uint64("seq", block.seq).Msg("ReplayWAL: IPC decode failed for uncompressed record")
				continue
			}
			if r.Next() {
				rec := r.RecordBatch()
				rec.Retain()

				out <- DecodedWALEntry{
					Name:   block.name,
					Record: rec,
					Seq:    block.seq,
					Ts:     block.ts,
				}
			}
			r.Release()
		}
	}
}

// reorderBufferRoutine reorders decoded entries by sequence number. It collects entries from multiple decoder goroutines and outputs them in order.
func (e *StorageEngine) reorderBufferRoutine(in chan DecodedWALEntry, out chan DecodedWALEntry, wgDecoders *sync.WaitGroup, firstSeqChan <-chan uint64) {
	defer close(out)

	// Map to hold out-of-order entries
	buffer := make(map[uint64]DecodedWALEntry)
	const maxBufferSize = 10000

	// Wait for the first sequence number
	nextSeq, ok := <-firstSeqChan
	if !ok {
		// WAL was empty
		wgDecoders.Wait()
		close(in)
		return
	}

	// Track decoder completion - close input channel when all decoders are done
	go func() {
		wgDecoders.Wait()
		close(in)
	}()

	for entry := range in {
		// Handle errors immediately
		if entry.Err != nil {
			out <- entry
			return
		}

		// Drop entries with seq < nextSeq (duplicates from reordered stream)
		if entry.Seq < nextSeq {
			if entry.Record != nil {
				entry.Record.Release()
			}
			continue
		}

		// If this is the expected next sequence, output it
		if entry.Seq == nextSeq {
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
		} else {
			// Store out-of-order entry in buffer
			// Enforce bounds to prevent OOM on corrupted WAL
			if len(buffer) >= maxBufferSize {
				// Evict oldest entries up to the gap
				for seq := nextSeq; seq < entry.Seq; seq++ {
					if _, exists := buffer[seq]; exists {
						delete(buffer, seq)
						// Skip missing sequences
						for nextSeq < seq {
							nextSeq++
						}
						break
					}
				}
				// If still full, drop the entry
				if len(buffer) >= maxBufferSize {
					if entry.Record != nil {
						entry.Record.Release()
					}
					log.Warn().
						Uint64("seq", entry.Seq).
						Int("buffer_size", len(buffer)).
						Msg("ReplayWAL: reorder buffer full, dropping entry")
					continue
				}
			}
			buffer[entry.Seq] = entry
		}
	}

	// Flush remaining buffer
	for len(buffer) > 0 {
		if entry, exists := buffer[nextSeq]; exists {
			delete(buffer, nextSeq)
			out <- entry
			nextSeq++
		} else {
			// We're missing entries — skip to next available
			foundNext := false
			for seq := range buffer {
				if seq > nextSeq {
					nextSeq = seq
					foundNext = true
					break
				}
			}
			if !foundNext {
				break
			}
		}
	}
}
