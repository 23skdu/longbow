package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/security"
)

var (
	// ErrEncryptionMasterKeyRequired indicates encryption was requested without a local master key.
	ErrEncryptionMasterKeyRequired = errors.New("storage: LONGBOW_KMS_MASTER_KEY is required when encryption at rest is enabled with the local KMS provider")
	// ErrUnsupportedKMSProvider indicates an encryption provider that cannot be configured from LONGBOW_* settings.
	ErrUnsupportedKMSProvider = errors.New("storage: unsupported KMS provider for encryption at rest")
)

// EncryptionConfig is the storage-facing subset of the server encryption settings.
// MasterKey is never included in errors or log messages.
type EncryptionConfig struct {
	Enabled   bool
	Provider  string
	MasterKey string
}

// String redacts the master key so accidental logging cannot expose it.
func (c EncryptionConfig) String() string {
	return fmt.Sprintf("EncryptionConfig{Enabled:%t Provider:%q MasterKey:<redacted>}", c.Enabled, c.NormalizedProvider())
}

// NormalizedProvider returns the canonical provider name (local by default).
func (c EncryptionConfig) NormalizedProvider() string {
	provider := strings.ToLower(strings.TrimSpace(c.Provider))
	if provider == "" {
		return "local"
	}
	return provider
}

// Validate rejects unsafe or unusable encryption settings before any key material
// is used. When encryption is disabled, leftover KMS settings are ignored so the
// plaintext default is preserved.
func (c EncryptionConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if strings.TrimSpace(c.MasterKey) == "" {
		return ErrEncryptionMasterKeyRequired
	}
	switch c.NormalizedProvider() {
	case "local":
		return nil
	default:
		return fmt.Errorf("%w: %q (supported: local)", ErrUnsupportedKMSProvider, c.NormalizedProvider())
	}
}

// NewCipherEngineFromConfig constructs a CipherEngine from validated encryption
// settings. It returns (nil, nil) when encryption is disabled.
func NewCipherEngineFromConfig(ctx context.Context, cfg EncryptionConfig) (*CipherEngine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return nil, nil
	}
	kp, err := security.NewKeyProviderFromConfig(security.KMSConfig{
		Provider:     cfg.NormalizedProvider(),
		MasterKeyHex: cfg.MasterKey,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: failed to initialize %s KMS provider: %w", cfg.NormalizedProvider(), err)
	}
	ce, err := NewCipherEngine(ctx, kp)
	if err != nil {
		return nil, fmt.Errorf("storage: failed to initialize encryption engine: %w", err)
	}
	return ce, nil
}

// defaultCipherEngine is used by storage backends created outside the storage
// engine (for example, disk vector spill created by internal/store). It is set
// once during server startup; the plaintext default is nil.
var defaultCipherEngine atomic.Pointer[CipherEngine]

// SetDefaultCipherEngine installs ce as the process-wide backend cipher and
// returns a function that restores the previous value. A nil ce restores
// plaintext behavior.
func SetDefaultCipherEngine(ce *CipherEngine) func() {
	previous := defaultCipherEngine.Swap(ce)
	return func() { defaultCipherEngine.Store(previous) }
}

// DefaultCipherEngine returns the process-wide backend cipher, if configured.
func DefaultCipherEngine() *CipherEngine {
	return defaultCipherEngine.Load()
}
