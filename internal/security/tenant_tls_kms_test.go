package security

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func generateTestCertPair(t *testing.T, dir, prefix string, isCA bool, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) (certPath, keyPath string, cert *x509.Certificate, key *ecdsa.PrivateKey) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: prefix},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	if isCA {
		tmpl.IsCA = true
		tmpl.BasicConstraintsValid = true
		tmpl.KeyUsage |= x509.KeyUsageCertSign
	}

	signerCert := tmpl
	signerKey := priv
	if caCert != nil && caKey != nil {
		signerCert = caCert
		signerKey = caKey
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, signerCert, &priv.PublicKey, signerKey)
	require.NoError(t, err)
	parsed, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)

	certPath = filepath.Join(dir, prefix+".crt")
	keyPath = filepath.Join(dir, prefix+".key")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
	return certPath, keyPath, parsed, priv
}

func TestTenantContextAndRLSEvaluation(t *testing.T) {
	eval := NewRLSEvaluator()
	require.NoError(t, eval.SetPolicy(RLSPolicy{
		Name:      "tenant-isolation",
		Namespace: "acme",
		Dataset:   "docs",
		Enabled:   true,
		Rules: []RLSRule{
			{Field: "tenant_id", Operator: RLSOpEq, ExpectedFromClaim: "tenant_id"},
			{Field: "classification", Operator: RLSOpIn, Value: "public,internal"},
		},
	}))

	tcAcme := &TenantContext{
		TenantID:    "tenant-acme",
		Namespace:   "acme",
		UserID:      "u1",
		Permissions: []string{"read"},
		Datasets:    []string{"docs"},
	}
	ctx := ContextWithTenant(context.Background(), tcAcme)
	got, ok := TenantFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, "tenant-acme", got.TenantID)

	require.True(t, tcAcme.CanAccessDataset("acme", "docs"))
	require.False(t, tcAcme.CanAccessDataset("other", "docs"))
	require.False(t, tcAcme.CanAccessDataset("acme", "secrets"))

	// Matching row
	require.True(t, eval.EvaluateMetadata(tcAcme, "acme", "docs", map[string]any{
		"tenant_id":      "tenant-acme",
		"classification": "internal",
	}))
	// Wrong tenant
	require.False(t, eval.EvaluateMetadata(tcAcme, "acme", "docs", map[string]any{
		"tenant_id":      "tenant-beta",
		"classification": "internal",
	}))
	// Disallowed classification
	require.False(t, eval.EvaluateMetadata(tcAcme, "acme", "docs", map[string]any{
		"tenant_id":      "tenant-acme",
		"classification": "top-secret",
	}))

	// Admin bypasses RLS
	admin := &TenantContext{IsAdmin: true}
	require.True(t, eval.EvaluateMetadata(admin, "acme", "docs", map[string]any{
		"tenant_id": "other",
	}))
}

func TestTLSAndMTLSReloader(t *testing.T) {
	dir := t.TempDir()
	caPath, _, caCert, caKey := generateTestCertPair(t, dir, "ca", true, nil, nil)
	srvCertPath, srvKeyPath, _, _ := generateTestCertPair(t, dir, "server", false, caCert, caKey)
	cliCertPath, cliKeyPath, _, _ := generateTestCertPair(t, dir, "client", false, caCert, caKey)

	srvCfg := TLSConfig{
		Enabled:           true,
		CertFile:          srvCertPath,
		KeyFile:           srvKeyPath,
		CAFile:            caPath,
		RequireClientCert: true,
		MinVersion:        "1.2",
	}
	reloader, err := NewCertReloader(srvCfg)
	require.NoError(t, err)
	defer reloader.Stop()
	require.Equal(t, int64(1), reloader.Rotations())

	serverTLS, err := reloader.BuildServerTLSConfig()
	require.NoError(t, err)

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	srv := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("mtls-ok"))
		}),
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	clientTLS, err := BuildClientTLSConfig(TLSConfig{
		Enabled:    true,
		CertFile:   cliCertPath,
		KeyFile:    cliKeyPath,
		CAFile:     caPath,
		ServerName: "localhost",
	})
	require.NoError(t, err)

	hc := &http.Client{
		Transport: &http.Transport{TLSClientConfig: clientTLS},
		Timeout:   5 * time.Second,
	}
	resp, err := hc.Get("https://" + ln.Addr().String())
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, "mtls-ok", string(body))

	// Rotate server cert on disk and hot-reload
	newCertPath, newKeyPath, _, _ := generateTestCertPair(t, dir, "server2", false, caCert, caKey)
	newCertData, err := os.ReadFile(newCertPath)
	require.NoError(t, err)
	newKeyData, err := os.ReadFile(newKeyPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(srvCertPath, newCertData, 0o600))
	require.NoError(t, os.WriteFile(srvKeyPath, newKeyData, 0o600))
	require.NoError(t, reloader.Reload())
	require.Equal(t, int64(2), reloader.Rotations())

	// Connection without client cert must fail mTLS
	badClientTLS, err := BuildClientTLSConfig(TLSConfig{
		Enabled:    true,
		CAFile:     caPath,
		ServerName: "localhost",
	})
	require.NoError(t, err)
	badHC := &http.Client{
		Transport: &http.Transport{TLSClientConfig: badClientTLS},
		Timeout:   3 * time.Second,
	}
	_, err = badHC.Get("https://" + ln.Addr().String())
	require.Error(t, err)
}

func TestLocalAndVaultKMSProvider(t *testing.T) {
	ctx := context.Background()

	// Local KEK + DEK + rotation
	kp, err := NewLocalKeyProviderFromSecret("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	id1, err := kp.ActiveKeyID(ctx)
	require.NoError(t, err)
	require.Equal(t, "v1", id1)

	dek1, wrapped1, err := kp.GenerateDEK(ctx)
	require.NoError(t, err)
	require.Len(t, dek1, 32)

	id2, err := kp.RotateKey(ctx)
	require.NoError(t, err)
	require.Equal(t, "v2", id2)

	// Old wrapped DEK still unwraps after KEK rotation
	unwrapped1, err := kp.UnwrapDEK(ctx, wrapped1)
	require.NoError(t, err)
	require.Equal(t, dek1, unwrapped1)

	// Mock Vault Transit server
	dekRaw := make([]byte, 32)
	_, err = rand.Read(dekRaw)
	require.NoError(t, err)
	dekB64 := base64.StdEncoding.EncodeToString(dekRaw)
	cipherToken := "vault:v1:" + base64.StdEncoding.EncodeToString(dekRaw)

	vaultSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/datakey/plaintext/"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"plaintext":   dekB64,
					"ciphertext":  cipherToken,
					"key_version": 1,
				},
			})
		case strings.Contains(r.URL.Path, "/decrypt/"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"plaintext": dekB64,
				},
			})
		case strings.Contains(r.URL.Path, "/rotate"):
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer vaultSrv.Close()

	vk, err := NewKeyProviderFromConfig(KMSConfig{
		Provider:     "vault",
		VaultAddr:    vaultSrv.URL,
		VaultToken:   "root",
		VaultMount:   "transit",
		VaultKeyName: "longbow",
	})
	require.NoError(t, err)

	vdek, vwrapped, err := vk.GenerateDEK(ctx)
	require.NoError(t, err)
	require.Equal(t, dekRaw, vdek)

	vunwrapped, err := vk.UnwrapDEK(ctx, vwrapped)
	require.NoError(t, err)
	require.Equal(t, dekRaw, vunwrapped)

	rotID, err := vk.RotateKey(ctx)
	require.NoError(t, err)
	require.Contains(t, rotID, "longbow@")
}

func FuzzRLSEvaluation(f *testing.F) {
	f.Add("t1", "ns1", "ds1", "t1", "public")
	f.Add("t1", "ns1", "ds2", "t2", "secret")
	f.Add("", "*", "*", "", "")
	f.Fuzz(func(t *testing.T, tenantID, ns, ds, metaTenant, cls string) {
		eval := NewRLSEvaluator()
		_ = eval.SetPolicy(RLSPolicy{
			Name:      "fuzz",
			Namespace: ns,
			Dataset:   ds,
			Enabled:   true,
			Rules: []RLSRule{
				{Field: "tenant_id", Operator: RLSOpEq, ExpectedFromClaim: "tenant_id"},
				{Field: "cls", Operator: RLSOpPrefix, Value: "pub"},
			},
		})
		tc := &TenantContext{TenantID: tenantID, Namespace: ns, Datasets: []string{ds}}
		_ = eval.EvaluateMetadata(tc, ns, ds, map[string]any{"tenant_id": metaTenant, "cls": cls})
	})
}

func FuzzTLSConfigValidation(f *testing.F) {
	f.Add(true, "/a.crt", "/a.key", "/ca.crt", true, "1.3")
	f.Add(true, "", "/a.key", "", false, "invalid")
	f.Fuzz(func(t *testing.T, enabled bool, cert, key, ca string, mtls bool, ver string) {
		cfg := TLSConfig{
			Enabled:           enabled,
			CertFile:          cert,
			KeyFile:           key,
			CAFile:            ca,
			RequireClientCert: mtls,
			MinVersion:        ver,
		}
		_ = cfg.Validate()
	})
}
