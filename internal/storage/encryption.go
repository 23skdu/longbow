package storage

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/23skdu/longbow/internal/security"
)

var (
	encMagic = [4]byte{'L', 'B', 'E', '1'}
)

const (
	defaultCipherBlockSize = 4096
)

// EnvelopeHeader stores metadata needed to unwrap a chunk's DEK without external state.
type EnvelopeHeader struct {
	KeyID      string
	WrappedDEK []byte
}

// CipherEngine performs AES-256-GCM envelope encryption backed by a security.KeyProvider.
type CipherEngine struct {
	kp         security.KeyProvider
	mu         sync.RWMutex
	activeDEK  []byte
	activeWrap security.WrappedDEK
	dekCache   map[string][]byte // sha256(keyID + wrapped) -> 32-byte DEK
}

// NewCipherEngine creates a CipherEngine and provisions an initial Data Encryption Key.
func NewCipherEngine(ctx context.Context, kp security.KeyProvider) (*CipherEngine, error) {
	if kp == nil {
		return nil, errors.New("storage: key provider is required")
	}
	ce := &CipherEngine{
		kp:       kp,
		dekCache: make(map[string][]byte),
	}
	if err := ce.RotateDEK(ctx); err != nil {
		return nil, err
	}
	return ce, nil
}

// RotateDEK generates a new DEK via the configured KeyProvider while retaining previous keys for decryption.
func (ce *CipherEngine) RotateDEK(ctx context.Context) error {
	dek, wrapped, err := ce.kp.GenerateDEK(ctx)
	if err != nil {
		return fmt.Errorf("storage: generate DEK failed: %w", err)
	}
	if len(dek) != 32 {
		return security.ErrInvalidKeySize
	}
	ce.mu.Lock()
	defer ce.mu.Unlock()
	ce.activeDEK = dek
	ce.activeWrap = wrapped
	ce.dekCache[cacheKey(wrapped)] = dek
	return nil
}

// Seal encrypts plaintext with AES-256-GCM and embeds an envelope header binding aad.
func (ce *CipherEngine) Seal(plaintext, aad []byte) ([]byte, error) {
	ce.mu.RLock()
	dek := ce.activeDEK
	wrapped := ce.activeWrap
	ce.mu.RUnlock()

	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	ct := aead.Seal(nil, nonce, plaintext, aad)

	// Wire format:
	// [4: magic "LBE1"][2: keyIDLen][N: keyID][2: wrapLen][M: wrappedDEK][nonceSize: nonce][ctLen: ct]
	keyIDBytes := []byte(wrapped.KeyID)
	totalLen := 4 + 2 + len(keyIDBytes) + 2 + len(wrapped.Ciphertext) + len(nonce) + len(ct)
	out := make([]byte, totalLen)
	pos := 0
	copy(out[pos:], encMagic[:])
	pos += 4
	binary.LittleEndian.PutUint16(out[pos:pos+2], uint16(len(keyIDBytes))) // #nosec G115
	pos += 2
	copy(out[pos:], keyIDBytes)
	pos += len(keyIDBytes)
	binary.LittleEndian.PutUint16(out[pos:pos+2], uint16(len(wrapped.Ciphertext))) // #nosec G115
	pos += 2
	copy(out[pos:], wrapped.Ciphertext)
	pos += len(wrapped.Ciphertext)
	copy(out[pos:], nonce)
	pos += len(nonce)
	copy(out[pos:], ct)
	return out, nil
}

// Open verifies envelope metadata and decrypts sealed ciphertext with AAD binding.
func (ce *CipherEngine) Open(ctx context.Context, ciphertext, aad []byte) ([]byte, error) {
	if len(ciphertext) < 8 || !bytes.Equal(ciphertext[:4], encMagic[:]) {
		return nil, errors.New("storage: invalid encrypted payload header")
	}
	pos := 4
	keyIDLen := int(binary.LittleEndian.Uint16(ciphertext[pos : pos+2]))
	pos += 2
	if pos+keyIDLen+2 > len(ciphertext) {
		return nil, errors.New("storage: truncated encrypted header (keyID)")
	}
	keyID := string(ciphertext[pos : pos+keyIDLen])
	pos += keyIDLen

	wrapLen := int(binary.LittleEndian.Uint16(ciphertext[pos : pos+2]))
	pos += 2
	if pos+wrapLen > len(ciphertext) {
		return nil, errors.New("storage: truncated encrypted header (wrappedDEK)")
	}
	wrappedBytes := ciphertext[pos : pos+wrapLen]
	pos += wrapLen

	wrapped := security.WrappedDEK{KeyID: keyID, Ciphertext: wrappedBytes}
	dek, err := ce.resolveDEK(ctx, wrapped)
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	ns := aead.NonceSize()
	if pos+ns > len(ciphertext) {
		return nil, errors.New("storage: truncated nonce in encrypted block")
	}
	nonce := ciphertext[pos : pos+ns]
	encBody := ciphertext[pos+ns:]
	return aead.Open(nil, nonce, encBody, aad)
}

func (ce *CipherEngine) resolveDEK(ctx context.Context, wrapped security.WrappedDEK) ([]byte, error) {
	k := cacheKey(wrapped)
	ce.mu.RLock()
	dek, ok := ce.dekCache[k]
	ce.mu.RUnlock()
	if ok {
		return dek, nil
	}

	unwrapped, err := ce.kp.UnwrapDEK(ctx, wrapped)
	if err != nil {
		return nil, fmt.Errorf("storage: unwrap DEK failed: %w", err)
	}
	if len(unwrapped) != 32 {
		return nil, security.ErrInvalidKeySize
	}

	ce.mu.Lock()
	ce.dekCache[k] = unwrapped
	ce.mu.Unlock()
	return unwrapped, nil
}

func cacheKey(w security.WrappedDEK) string {
	h := sha256.New()
	_, _ = h.Write([]byte(w.KeyID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(w.Ciphertext)
	return hex.EncodeToString(h.Sum(nil))
}

// EncryptedWALBackend wraps any WALBackend with length-prefixed AES-256-GCM frames.
type EncryptedWALBackend struct {
	inner WALBackend
	ce    *CipherEngine
	mu    sync.Mutex
	seq   uint64
}

// NewEncryptedWALBackend wraps a WALBackend with encryption at rest.
func NewEncryptedWALBackend(inner WALBackend, ce *CipherEngine) *EncryptedWALBackend {
	return &EncryptedWALBackend{
		inner: inner,
		ce:    ce,
	}
}

func (w *EncryptedWALBackend) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seq++
	var aad [8]byte
	binary.LittleEndian.PutUint64(aad[:], w.seq)
	sealed, err := w.ce.Seal(p, aad[:])
	if err != nil {
		return 0, err
	}
	frame := make([]byte, 12+len(sealed))
	binary.LittleEndian.PutUint64(frame[0:8], w.seq)
	binary.LittleEndian.PutUint32(frame[8:12], uint32(len(sealed))) // #nosec G115
	copy(frame[12:], sealed)
	if _, err := w.inner.Write(frame); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *EncryptedWALBackend) Sync() error    { return w.inner.Sync() }
func (w *EncryptedWALBackend) Close() error   { return w.inner.Close() }
func (w *EncryptedWALBackend) Name() string   { return w.inner.Name() + "+encrypted" }
func (w *EncryptedWALBackend) File() *os.File { return w.inner.File() }

// DecryptWALFrames reads and decrypts all encrypted WAL frames from r.
func DecryptWALFrames(ctx context.Context, r io.Reader, ce *CipherEngine) ([][]byte, error) {
	var out [][]byte
	var hdr [12]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) && len(out) == 0 && hdr == ([12]byte{}) {
				if errors.Is(err, io.EOF) {
					return out, nil
				}
			}
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return out, err
		}
		seq := binary.LittleEndian.Uint64(hdr[0:8])
		length := binary.LittleEndian.Uint32(hdr[8:12])
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			return out, err
		}
		var aad [8]byte
		binary.LittleEndian.PutUint64(aad[:], seq)
		plain, err := ce.Open(ctx, payload, aad[:])
		if err != nil {
			return out, fmt.Errorf("wal frame %d decryption failed: %w", seq, err)
		}
		out = append(out, plain)
	}
}

// EncryptedStorageBackend provides fixed-page authenticated encryption over StorageBackend
// so random-access positional ReadAt/WriteAt works for disk vector spill.
type EncryptedStorageBackend struct {
	inner     StorageBackend
	ce        *CipherEngine
	blockSize int
	mu        sync.RWMutex
	logicalSz int64
}

// NewEncryptedStorageBackend wraps a StorageBackend with page-level AES-256-GCM encryption.
func NewEncryptedStorageBackend(inner StorageBackend, ce *CipherEngine, blockSize int) *EncryptedStorageBackend {
	if blockSize <= 0 {
		blockSize = defaultCipherBlockSize
	}
	return &EncryptedStorageBackend{
		inner:     inner,
		ce:        ce,
		blockSize: blockSize,
	}
}

// Note: Physical slots store [4: payloadLen][sealedPayload padded to slotSize].
func (b *EncryptedStorageBackend) slotSize() int {
	// Envelope overhead (~128 bytes max) + blockSize + padding margin
	return 4 + b.blockSize + 256
}

func (b *EncryptedStorageBackend) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("storage: negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()

	readTotal := 0
	for readTotal < len(p) {
		curOff := off + int64(readTotal)
		pageIdx := curOff / int64(b.blockSize)
		pageOff := int(curOff % int64(b.blockSize))

		pagePlain, err := b.readPageLocked(pageIdx)
		if err != nil {
			if readTotal > 0 && errors.Is(err, io.EOF) {
				return readTotal, io.EOF
			}
			return readTotal, err
		}

		if pageOff >= len(pagePlain) {
			return readTotal, io.EOF
		}
		n := copy(p[readTotal:], pagePlain[pageOff:])
		readTotal += n
	}
	return readTotal, nil
}

func (b *EncryptedStorageBackend) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("storage: negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	written := 0
	for written < len(p) {
		curOff := off + int64(written)
		pageIdx := curOff / int64(b.blockSize)
		pageOff := int(curOff % int64(b.blockSize))

		var pageBuf []byte
		if pageOff != 0 || (len(p)-written) < b.blockSize {
			existing, err := b.readPageLocked(pageIdx)
			if err == nil {
				pageBuf = existing
			}
		}
		needed := pageOff + minInt(len(p)-written, b.blockSize-pageOff)
		if len(pageBuf) < needed {
			grown := make([]byte, needed)
			copy(grown, pageBuf)
			pageBuf = grown
		}
		n := copy(pageBuf[pageOff:], p[written:])
		if err := b.writePageLocked(pageIdx, pageBuf); err != nil {
			return written, err
		}
		written += n
		if curOff+int64(n) > b.logicalSz {
			b.logicalSz = curOff + int64(n)
		}
	}
	return written, nil
}

func (b *EncryptedStorageBackend) readPageLocked(pageIdx int64) ([]byte, error) {
	slot := make([]byte, b.slotSize())
	physOff := pageIdx * int64(b.slotSize())
	n, err := b.inner.ReadAt(slot, physOff)
	if n < 4 {
		if err != nil {
			return nil, io.EOF
		}
		return nil, io.EOF
	}
	payloadLen := int(binary.LittleEndian.Uint32(slot[:4]))
	if payloadLen <= 0 || 4+payloadLen > n {
		return nil, io.ErrUnexpectedEOF
	}
	var aad [8]byte
	binary.LittleEndian.PutUint64(aad[:], uint64(pageIdx)) // #nosec G115
	return b.ce.Open(context.Background(), slot[4:4+payloadLen], aad[:])
}

func (b *EncryptedStorageBackend) writePageLocked(pageIdx int64, plain []byte) error {
	var aad [8]byte
	binary.LittleEndian.PutUint64(aad[:], uint64(pageIdx)) // #nosec G115
	sealed, err := b.ce.Seal(plain, aad[:])
	if err != nil {
		return err
	}
	if 4+len(sealed) > b.slotSize() {
		return fmt.Errorf("storage: sealed payload (%d) exceeds slot size (%d)", len(sealed), b.slotSize())
	}
	slot := make([]byte, b.slotSize())
	binary.LittleEndian.PutUint32(slot[:4], uint32(len(sealed))) // #nosec G115
	copy(slot[4:], sealed)
	_, err = b.inner.WriteAt(slot, pageIdx*int64(b.slotSize()))
	return err
}

func (b *EncryptedStorageBackend) Readv(iovs [][]byte, off int64) (int, error) {
	total := 0
	curr := off
	for _, buf := range iovs {
		n, err := b.ReadAt(buf, curr)
		total += n
		curr += int64(n)
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (b *EncryptedStorageBackend) Writev(iovs [][]byte, off int64) (int, error) {
	total := 0
	curr := off
	for _, buf := range iovs {
		n, err := b.WriteAt(buf, curr)
		total += n
		curr += int64(n)
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (b *EncryptedStorageBackend) Sync() error  { return b.inner.Sync() }
func (b *EncryptedStorageBackend) Close() error { return b.inner.Close() }
func (b *EncryptedStorageBackend) Name() string { return b.inner.Name() + "+encrypted" }
func (b *EncryptedStorageBackend) Size() (int64, error) {
	b.mu.RLock()
	sz := b.logicalSz
	b.mu.RUnlock()
	if sz > 0 {
		return sz, nil
	}
	phys, err := b.inner.Size()
	if err != nil {
		return 0, err
	}
	pages := phys / int64(b.slotSize())
	return pages * int64(b.blockSize), nil
}

// EncryptedSnapshotBackend transparently encrypts snapshots before delegating to inner SnapshotBackend.
type EncryptedSnapshotBackend struct {
	inner SnapshotBackend
	ce    *CipherEngine
}

// NewEncryptedSnapshotBackend wraps a SnapshotBackend with AES-256-GCM envelope encryption.
func NewEncryptedSnapshotBackend(inner SnapshotBackend, ce *CipherEngine) SnapshotBackend {
	return &EncryptedSnapshotBackend{inner: inner, ce: ce}
}

func (s *EncryptedSnapshotBackend) WriteSnapshot(ctx context.Context, name string, data []byte) error {
	sealed, err := s.ce.Seal(data, []byte(name))
	if err != nil {
		return err
	}
	return s.inner.WriteSnapshot(ctx, name, sealed)
}

func (s *EncryptedSnapshotBackend) ReadSnapshot(ctx context.Context, name string) (io.ReadCloser, error) {
	rc, err := s.inner.ReadSnapshot(ctx, name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	sealed, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	plain, err := s.ce.Open(ctx, sealed, []byte(name))
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(plain)), nil
}

func (s *EncryptedSnapshotBackend) ListSnapshots(ctx context.Context) ([]string, error) {
	return s.inner.ListSnapshots(ctx)
}

func (s *EncryptedSnapshotBackend) DeleteSnapshot(ctx context.Context, name string) error {
	return s.inner.DeleteSnapshot(ctx, name)
}

func (s *EncryptedSnapshotBackend) WriteSnapshotAsync(name string, data []byte) {
	sealed, err := s.ce.Seal(data, []byte(name))
	if err != nil {
		return
	}
	s.inner.WriteSnapshotAsync(name, sealed)
}

func (s *EncryptedSnapshotBackend) WriteSnapshotFile(ctx context.Context, name, ext string, r io.Reader) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	sealed, err := s.ce.Seal(raw, []byte(name+ext))
	if err != nil {
		return err
	}
	return s.inner.WriteSnapshotFile(ctx, name, ext, bytes.NewReader(sealed))
}

func (s *EncryptedSnapshotBackend) ReadSnapshotFile(ctx context.Context, name, ext string) (io.ReadCloser, error) {
	rc, err := s.inner.ReadSnapshotFile(ctx, name, ext)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	sealed, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	plain, err := s.ce.Open(ctx, sealed, []byte(name+ext))
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(plain)), nil
}

func (s *EncryptedSnapshotBackend) Bucket() string { return s.inner.Bucket() }
func (s *EncryptedSnapshotBackend) Prefix() string { return s.inner.Prefix() }
func (s *EncryptedSnapshotBackend) GetHTTPTransport() *http.Transport {
	return s.inner.GetHTTPTransport()
}
func (s *EncryptedSnapshotBackend) GetHTTPClient() *http.Client { return s.inner.GetHTTPClient() }

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
