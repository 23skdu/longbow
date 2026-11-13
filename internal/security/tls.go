package security

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TLSConfig defines server/client TLS and mTLS settings.
type TLSConfig struct {
	Enabled           bool          `envconfig:"TLS_ENABLED" json:"enabled"`
	CertFile          string        `envconfig:"TLS_CERT_FILE" json:"cert_file"`
	KeyFile           string        `envconfig:"TLS_KEY_FILE" json:"key_file"`
	CAFile            string        `envconfig:"TLS_CA_FILE" json:"ca_file"`
	RequireClientCert bool          `envconfig:"TLS_REQUIRE_CLIENT_CERT" json:"require_client_cert"`
	MinVersion        string        `envconfig:"TLS_MIN_VERSION" json:"min_version"` // "1.2" or "1.3"
	ReloadInterval    time.Duration `envconfig:"TLS_RELOAD_INTERVAL" json:"reload_interval"`
	ServerName        string        `envconfig:"TLS_SERVER_NAME" json:"server_name,omitempty"`
}

// Validate checks TLS configuration consistency.
func (c TLSConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.CertFile == "" || c.KeyFile == "" {
		return errors.New("tls: both cert_file and key_file are required when TLS is enabled")
	}
	if c.RequireClientCert && c.CAFile == "" {
		return errors.New("tls: ca_file is required when require_client_cert (mTLS) is enabled")
	}
	switch c.MinVersion {
	case "", "1.2", "1.3", "TLS1.2", "TLS1.3":
	default:
		return fmt.Errorf("tls: unsupported min_version %q (supported: 1.2, 1.3)", c.MinVersion)
	}
	return nil
}

func (c TLSConfig) parseMinVersion() uint16 {
	if c.MinVersion == "1.3" || c.MinVersion == "TLS1.3" {
		return tls.VersionTLS13
	}
	return tls.VersionTLS12
}

// CertReloader loads and hot-reloads X.509 key pairs and CA trust pools without restarts.
type CertReloader struct {
	cfg       TLSConfig
	cert      atomic.Pointer[tls.Certificate]
	caPool    atomic.Pointer[x509.CertPool]
	mu        sync.Mutex
	stopCh    chan struct{}
	stopOnce  sync.Once
	rotations atomic.Int64
}

// NewCertReloader initializes a CertReloader and loads initial certificates from disk.
func NewCertReloader(cfg TLSConfig) (*CertReloader, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	r := &CertReloader{
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
	if cfg.Enabled {
		if err := r.Reload(); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Reload reloads the certificate/key pair and optional CA bundle atomically.
func (r *CertReloader) Reload() error {
	if !r.cfg.Enabled {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	certPath := filepath.Clean(r.cfg.CertFile)
	keyPath := filepath.Clean(r.cfg.KeyFile)

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return fmt.Errorf("tls: failed loading X509 key pair: %w", err)
	}

	if r.cfg.CAFile != "" {
		caBytes, err := os.ReadFile(filepath.Clean(r.cfg.CAFile)) // #nosec G304
		if err != nil {
			return fmt.Errorf("tls: failed reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return errors.New("tls: failed parsing CA certificate PEM")
		}
		r.caPool.Store(pool)
	}

	r.cert.Store(&cert)
	r.rotations.Add(1)
	return nil
}

// StartAutoReload begins periodic certificate rotation checks until Stop is called.
func (r *CertReloader) StartAutoReload(interval time.Duration) {
	if interval <= 0 {
		interval = r.cfg.ReloadInterval
	}
	if interval <= 0 || !r.cfg.Enabled {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = r.Reload()
			case <-r.stopCh:
				return
			}
		}
	}()
}

// Stop stops the background certificate reloader.
func (r *CertReloader) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
}

// Rotations returns the number of successful certificate loads/rotations.
func (r *CertReloader) Rotations() int64 {
	return r.rotations.Load()
}

// GetCertificate returns the active server certificate for TLS handshakes.
func (r *CertReloader) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	c := r.cert.Load()
	if c == nil {
		return nil, errors.New("tls: no certificate loaded")
	}
	return c, nil
}

// GetClientCertificate returns the active client certificate for mTLS handshakes.
func (r *CertReloader) GetClientCertificate(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	c := r.cert.Load()
	if c == nil {
		return nil, errors.New("tls: no client certificate loaded")
	}
	return c, nil
}

// BuildServerTLSConfig creates a server *tls.Config using dynamic certificate and CA lookup.
func (r *CertReloader) BuildServerTLSConfig() (*tls.Config, error) {
	if !r.cfg.Enabled {
		return nil, nil
	}
	minVer := r.cfg.parseMinVersion()
	return &tls.Config{
		MinVersion:     minVer,
		GetCertificate: r.GetCertificate,
		GetConfigForClient: func(_ *tls.ClientHelloInfo) (*tls.Config, error) {
			clientAuth := tls.NoClientCert
			if r.cfg.RequireClientCert {
				clientAuth = tls.RequireAndVerifyClientCert
			}
			return &tls.Config{
				MinVersion:     minVer,
				GetCertificate: r.GetCertificate,
				ClientCAs:      r.caPool.Load(),
				ClientAuth:     clientAuth,
			}, nil
		},
	}, nil
}

// BuildClientTLSConfig builds a client *tls.Config with optional mTLS and custom CA trust.
func BuildClientTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	minVer := cfg.parseMinVersion()
	tlsCfg := &tls.Config{
		MinVersion: minVer,
		ServerName: cfg.ServerName,
	}
	if cfg.CAFile != "" {
		caBytes, err := os.ReadFile(filepath.Clean(cfg.CAFile)) // #nosec G304
		if err != nil {
			return nil, fmt.Errorf("tls: read client CA failed: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, errors.New("tls: failed parsing client CA PEM")
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(filepath.Clean(cfg.CertFile), filepath.Clean(cfg.KeyFile))
		if err != nil {
			return nil, fmt.Errorf("tls: load client keypair failed: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

// GRPCServerOption returns the grpc.Creds server option for this reloader when TLS is enabled.
func (r *CertReloader) GRPCServerOption() (grpc.ServerOption, error) {
	if !r.cfg.Enabled {
		return nil, nil
	}
	tlsCfg, err := r.BuildServerTLSConfig()
	if err != nil {
		return nil, err
	}
	return grpc.Creds(credentials.NewTLS(tlsCfg)), nil
}

// GRPCDialOption returns gRPC transport credentials for clients.
func GRPCDialOption(cfg TLSConfig) (grpc.DialOption, error) {
	if !cfg.Enabled {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	tlsCfg, err := BuildClientTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)), nil
}
