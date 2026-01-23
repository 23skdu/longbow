package store

import (
	"bytes"
	"encoding/json"
	"sync"
	"time"
)

// MeshStatusCache caches the serialized mesh status to avoid repeated allocations.
type MeshStatusCache struct {
	mu          sync.RWMutex
	cachedJSON  []byte
	lastUpdate  time.Time
	cacheTTL    time.Duration
	memberCount int
}

// NewMeshStatusCache creates a new mesh status cache with the specified TTL.
func NewMeshStatusCache(ttl time.Duration) *MeshStatusCache {
	return &MeshStatusCache{
		cacheTTL: ttl,
	}
}

// Get returns the cached JSON if valid, otherwise returns nil.
func (c *MeshStatusCache) Get(currentMemberCount int) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Invalidate if member count changed or TTL expired
	if c.memberCount != currentMemberCount || time.Since(c.lastUpdate) > c.cacheTTL {
		return nil
	}

	// Return a copy to prevent mutation
	result := make([]byte, len(c.cachedJSON))
	copy(result, c.cachedJSON)
	return result
}

// Set updates the cache with new JSON data.
func (c *MeshStatusCache) Set(data []byte, memberCount int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cachedJSON = make([]byte, len(data))
	copy(c.cachedJSON, data)
	c.lastUpdate = time.Now()
	c.memberCount = memberCount
}

// Invalidate clears the cache.
func (c *MeshStatusCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cachedJSON = nil
	c.lastUpdate = time.Time{}
	c.memberCount = 0
}

// jsonEncoderPool pools JSON encoders and buffers to reduce allocations.
var jsonEncoderPool = sync.Pool{
	New: func() any {
		buf := new(bytes.Buffer)
		enc := json.NewEncoder(buf)
		return &jsonEncoderWrapper{
			buf: buf,
			enc: enc,
		}
	},
}

type jsonEncoderWrapper struct {
	buf *bytes.Buffer
	enc *json.Encoder
}

// GetJSONEncoder retrieves a pooled encoder and buffer.
func GetJSONEncoder() (*bytes.Buffer, *json.Encoder) {
	wrapper := jsonEncoderPool.Get().(*jsonEncoderWrapper)
	wrapper.buf.Reset()
	return wrapper.buf, wrapper.enc
}

// PutJSONEncoder returns the encoder and buffer to the pool.
func PutJSONEncoder(buf *bytes.Buffer, enc *json.Encoder) {
	// Discard oversized buffers to prevent memory bloat
	if buf.Cap() > 1<<20 { // 1MB
		return
	}
	wrapper := &jsonEncoderWrapper{buf: buf, enc: enc}
	jsonEncoderPool.Put(wrapper)
}
