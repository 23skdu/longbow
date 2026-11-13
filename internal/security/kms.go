package security

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	ErrKeyNotFound    = errors.New("kms: key version not found")
	ErrInvalidKeySize = errors.New("kms: master key must be 32 bytes (AES-256)")
	ErrKMSUnavailable = errors.New("kms: key management service unavailable")
)

// KMSConfig configures key management for encryption at rest.
type KMSConfig struct {
	Provider     string `envconfig:"KMS_PROVIDER" json:"provider"` // "local", "env", "vault"
	MasterKeyHex string `envconfig:"KMS_MASTER_KEY" json:"master_key_hex,omitempty"`
	MasterKeyEnv string `envconfig:"KMS_MASTER_KEY_ENV" json:"master_key_env,omitempty"`
	VaultAddr    string `envconfig:"VAULT_ADDR" json:"vault_addr,omitempty"`
	VaultToken   string `envconfig:"VAULT_TOKEN" json:"vault_token,omitempty"`
	VaultMount   string `envconfig:"VAULT_TRANSIT_MOUNT" json:"vault_mount,omitempty"`
	VaultKeyName string `envconfig:"VAULT_TRANSIT_KEY" json:"vault_key_name,omitempty"`
}

// WrappedDEK holds a wrapped Data Encryption Key along with its wrapping KEK version ID.
type WrappedDEK struct {
	KeyID      string `json:"key_id"`
	Ciphertext []byte `json:"ciphertext"`
}

// KeyProvider abstracts local keys, environment keys, and external KMS/Vault Transit.
type KeyProvider interface {
	ActiveKeyID(ctx context.Context) (string, error)
	GenerateDEK(ctx context.Context) (plaintext []byte, wrapped WrappedDEK, err error)
	UnwrapDEK(ctx context.Context, wrapped WrappedDEK) ([]byte, error)
	RotateKey(ctx context.Context) (string, error)
}

// LocalKeyProvider implements envelope encryption with zero-downtime key rotation in memory.
type LocalKeyProvider struct {
	mu       sync.RWMutex
	activeID string
	keys     map[string][]byte // keyID -> 32-byte KEK
	seq      int
}

// NewLocalKeyProvider creates a LocalKeyProvider with an optional initial 32-byte KEK.
// If masterKey is nil, a fresh CSPRNG 32-byte KEK is generated.
func NewLocalKeyProvider(masterKey []byte) (*LocalKeyProvider, error) {
	kek := make([]byte, 32)
	if len(masterKey) == 0 {
		if _, err := rand.Read(kek); err != nil {
			return nil, fmt.Errorf("kms: failed generating random master key: %w", err)
		}
	} else {
		if len(masterKey) != 32 {
			return nil, ErrInvalidKeySize
		}
		copy(kek, masterKey)
	}
	return &LocalKeyProvider{
		activeID: "v1",
		keys:     map[string][]byte{"v1": kek},
		seq:      1,
	}, nil
}

// NewLocalKeyProviderFromSecret derives or decodes a 32-byte key from hex/base64 or passphrase string.
func NewLocalKeyProviderFromSecret(secret string) (*LocalKeyProvider, error) {
	if secret == "" {
		return NewLocalKeyProvider(nil)
	}
	if decoded, err := hex.DecodeString(strings.TrimSpace(secret)); err == nil && len(decoded) == 32 {
		return NewLocalKeyProvider(decoded)
	}
	if decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(secret)); err == nil && len(decoded) == 32 {
		return NewLocalKeyProvider(decoded)
	}
	sum := sha256.Sum256([]byte(secret))
	return NewLocalKeyProvider(sum[:])
}

func (p *LocalKeyProvider) ActiveKeyID(_ context.Context) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.activeID, nil
}

func (p *LocalKeyProvider) GenerateDEK(_ context.Context) ([]byte, WrappedDEK, error) {
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		return nil, WrappedDEK{}, fmt.Errorf("kms: generate DEK failed: %w", err)
	}

	p.mu.RLock()
	keyID := p.activeID
	kek := p.keys[keyID]
	p.mu.RUnlock()

	ct, err := wrapWithAESGCM(kek, dek, []byte(keyID))
	if err != nil {
		return nil, WrappedDEK{}, err
	}
	return dek, WrappedDEK{KeyID: keyID, Ciphertext: ct}, nil
}

func (p *LocalKeyProvider) UnwrapDEK(_ context.Context, wrapped WrappedDEK) ([]byte, error) {
	p.mu.RLock()
	kek, ok := p.keys[wrapped.KeyID]
	p.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, wrapped.KeyID)
	}
	return unwrapWithAESGCM(kek, wrapped.Ciphertext, []byte(wrapped.KeyID))
}

func (p *LocalKeyProvider) RotateKey(_ context.Context) (string, error) {
	newKEK := make([]byte, 32)
	if _, err := rand.Read(newKEK); err != nil {
		return "", fmt.Errorf("kms: generate rotated KEK failed: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seq++
	newID := fmt.Sprintf("v%d", p.seq)
	p.keys[newID] = newKEK
	p.activeID = newID
	return newID, nil
}

// VaultTransitKeyProvider delegates DEK generation/unwrapping/rotation to HashiCorp Vault Transit engine.
type VaultTransitKeyProvider struct {
	addr       string
	token      string
	mount      string
	keyName    string
	httpClient *http.Client
	mu         sync.RWMutex
	activeID   string
}

// NewVaultTransitKeyProvider creates a HashiCorp Vault Transit KeyProvider.
func NewVaultTransitKeyProvider(addr, token, mount, keyName string) (*VaultTransitKeyProvider, error) {
	if addr == "" || keyName == "" {
		return nil, errors.New("kms: vault addr and key_name are required")
	}
	if mount == "" {
		mount = "transit"
	}
	return &VaultTransitKeyProvider{
		addr:     strings.TrimRight(addr, "/"),
		token:    token,
		mount:    strings.Trim(mount, "/"),
		keyName:  keyName,
		activeID: keyName,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}, nil
}

func (v *VaultTransitKeyProvider) ActiveKeyID(_ context.Context) (string, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.activeID, nil
}

func (v *VaultTransitKeyProvider) GenerateDEK(ctx context.Context) ([]byte, WrappedDEK, error) {
	endpoint := fmt.Sprintf("%s/v1/%s/datakey/plaintext/%s", v.addr, v.mount, v.keyName)
	body, err := v.doVaultRequest(ctx, http.MethodPost, endpoint, map[string]any{"bits": 256})
	if err != nil {
		return nil, WrappedDEK{}, err
	}
	var resp struct {
		Data struct {
			Plaintext  string `json:"plaintext"`
			Ciphertext string `json:"ciphertext"`
			KeyVersion int    `json:"key_version"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, WrappedDEK{}, fmt.Errorf("kms: invalid vault datakey response: %w", err)
	}
	dek, err := base64.StdEncoding.DecodeString(resp.Data.Plaintext)
	if err != nil {
		return nil, WrappedDEK{}, fmt.Errorf("kms: invalid vault plaintext key encoding: %w", err)
	}
	keyID := v.keyName
	if resp.Data.KeyVersion > 0 {
		keyID = fmt.Sprintf("%s:v%d", v.keyName, resp.Data.KeyVersion)
		v.mu.Lock()
		v.activeID = keyID
		v.mu.Unlock()
	}
	return dek, WrappedDEK{
		KeyID:      keyID,
		Ciphertext: []byte(resp.Data.Ciphertext),
	}, nil
}

func (v *VaultTransitKeyProvider) UnwrapDEK(ctx context.Context, wrapped WrappedDEK) ([]byte, error) {
	endpoint := fmt.Sprintf("%s/v1/%s/decrypt/%s", v.addr, v.mount, v.keyName)
	body, err := v.doVaultRequest(ctx, http.MethodPost, endpoint, map[string]any{
		"ciphertext": string(wrapped.Ciphertext),
	})
	if err != nil {
		return nil, err
	}
	var resp struct {
		Data struct {
			Plaintext string `json:"plaintext"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("kms: invalid vault decrypt response: %w", err)
	}
	return base64.StdEncoding.DecodeString(resp.Data.Plaintext)
}

func (v *VaultTransitKeyProvider) RotateKey(ctx context.Context) (string, error) {
	endpoint := fmt.Sprintf("%s/v1/%s/keys/%s/rotate", v.addr, v.mount, v.keyName)
	if _, err := v.doVaultRequest(ctx, http.MethodPost, endpoint, nil); err != nil {
		return "", err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.activeID = fmt.Sprintf("%s@%d", v.keyName, time.Now().Unix())
	return v.activeID, nil
}

func (v *VaultTransitKeyProvider) doVaultRequest(ctx context.Context, method, url string, payload any) ([]byte, error) {
	var bodyReader io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	if v.token != "" {
		req.Header.Set("X-Vault-Token", v.token)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.httpClient.Do(req) // #nosec G107
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrKMSUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%w: vault status %d: %s", ErrKMSUnavailable, resp.StatusCode, string(respBytes))
	}
	return respBytes, nil
}

// NewKeyProviderFromConfig constructs a KeyProvider from KMSConfig.
func NewKeyProviderFromConfig(cfg KMSConfig) (KeyProvider, error) {
	switch strings.ToLower(cfg.Provider) {
	case "", "local":
		return NewLocalKeyProviderFromSecret(cfg.MasterKeyHex)
	case "env":
		envVar := cfg.MasterKeyEnv
		if envVar == "" {
			envVar = "LONGBOW_MASTER_KEY"
		}
		secret := os.Getenv(envVar)
		if secret == "" {
			return nil, fmt.Errorf("kms: environment variable %s is empty", envVar)
		}
		return NewLocalKeyProviderFromSecret(secret)
	case "vault":
		return NewVaultTransitKeyProvider(cfg.VaultAddr, cfg.VaultToken, cfg.VaultMount, cfg.VaultKeyName)
	default:
		return nil, fmt.Errorf("kms: unsupported provider %q", cfg.Provider)
	}
}

func wrapWithAESGCM(kek, plaintext, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
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
	out := make([]byte, len(nonce)+len(ct))
	copy(out, nonce)
	copy(out[len(nonce):], ct)
	return out, nil
}

func unwrapWithAESGCM(kek, data, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	ns := aead.NonceSize()
	if len(data) < ns {
		return nil, errors.New("kms: ciphertext too short")
	}
	return aead.Open(nil, data[:ns], data[ns:], aad)
}
