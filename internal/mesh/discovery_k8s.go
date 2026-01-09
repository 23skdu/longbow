package mesh

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	tokenPath     = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	caPath        = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	namespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	apiHost       = "kubernetes.default.svc"
)

// K8sProvider performs discovery by querying the Kubernetes API directly.
// It is optimized for high performance and low memory allocation.
type K8sProvider struct {
	Namespace     string
	LabelSelector string
	Port          int

	token      string
	caCertPool *x509.CertPool
	client     *http.Client
	bufferPool sync.Pool

	initOnce sync.Once
	initErr  error
}

// NewK8sProvider creates a new Kubernetes discovery provider.
func NewK8sProvider(namespace, labelSelector string, port int) *K8sProvider {
	return &K8sProvider{
		Namespace:     namespace,
		LabelSelector: labelSelector,
		Port:          port,
		bufferPool: sync.Pool{
			New: func() interface{} {
				// Pre-allocate a reasonable buffer size for K8s responses (e.g. 64KB)
				b := make([]byte, 65536)
				return &b
			},
		},
	}
}

// init loads credentials and sets up the HTTP client.
func (k *K8sProvider) init() error {
	// Read Token
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		return fmt.Errorf("failed to read service account token: %w", err)
	}
	k.token = string(tokenBytes)

	// Read CA Cert
	caBytes, err := os.ReadFile(caPath)
	if err != nil {
		return fmt.Errorf("failed to read CA cert: %w", err)
	}
	k.caCertPool = x509.NewCertPool()
	if !k.caCertPool.AppendCertsFromPEM(caBytes) {
		return fmt.Errorf("failed to append CA cert")
	}

	// Determine Namespace if not set
	if k.Namespace == "" {
		nsBytes, err := os.ReadFile(namespacePath)
		if err != nil {
			return fmt.Errorf("failed to read namespace: %w", err)
		}
		k.Namespace = string(nsBytes)
	}

	// Setup Transport
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: k.caCertPool,
		},
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true, // Avoid gzip overhead if possible, we want raw speed
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	k.client = &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}

	return nil
}

// FindPeers queries the K8s API for pods matching the label selector.
func (k *K8sProvider) FindPeers(ctx context.Context) ([]string, error) {
	k.initOnce.Do(func() {
		k.initErr = k.init()
	})
	if k.initErr != nil {
		return nil, k.initErr
	}

	url := fmt.Sprintf("https://%s/api/v1/namespaces/%s/pods?labelSelector=%s", apiHost, k.Namespace, k.LabelSelector)
	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)

	resp, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("k8s api returned status: %d", resp.StatusCode)
	}

	// Use pooled buffer to read response
	bufPtr := k.bufferPool.Get().(*[]byte)
	buf := *bufPtr
	defer k.bufferPool.Put(bufPtr)

	// Reset buffer length logic is tricky with slice pointers in pool.
	// We read into 'buf' up to its capacity.
	// If response > 64KB, we might truncate.
	// For specialized zero-alloc, determining size is hard without fully reading.
	// Let's use io.ReadAll but with our buffer as a scratch if it fits?
	// Actually io.ReadAll allocates.
	// Let's implement a simple reader loop into our big buffer.

	offset := 0
	for {
		if offset >= len(buf) {
			// Buffer overflow - fallback to growing (allocation) or error?
			// For this optimization, let's grow.
			newBuf := make([]byte, len(buf)*2)
			copy(newBuf, buf)
			buf = newBuf
			// We effectively detached from the pool for this large response,
			// or we need to update the pointer.
			// Ideally we don't return oversized buffers to the pool to prevent memory leaks.
		}
		n, err := resp.Body.Read(buf[offset:])
		offset += n
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	// Parse using our zero-alloc parser
	ips, err := ExtractPodIPs(buf[:offset])
	if err != nil {
		return nil, err
	}

	// Format peers with port
	peers := make([]string, 0, len(ips))
	for _, ip := range ips {
		peers = append(peers, fmt.Sprintf("%s:%d", ip, k.Port))
	}

	return peers, nil
}

// Register is a no-op for K8s provider.
func (k *K8sProvider) Register(ctx context.Context, id string, port int) error {
	return nil
}
