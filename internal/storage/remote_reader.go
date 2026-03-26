package storage

import (
	"io"

	"github.com/23skdu/longbow/internal/metrics"
)

// metricsReader wraps an io.ReadCloser to intercept Read and Close calls
// and observe the total bytes downloaded for Prometheus tracking.
type metricsReader struct {
	r        io.ReadCloser
	provider string
}

func (m *metricsReader) Read(p []byte) (n int, err error) {
	n, err = m.r.Read(p)
	if n > 0 {
		metrics.RemoteStorageDownloadBytes.WithLabelValues(m.provider).Add(float64(n))
	}
	return n, err
}

func (m *metricsReader) Close() error {
	return m.r.Close()
}
