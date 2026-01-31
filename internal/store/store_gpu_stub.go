//go:build !gpu

package store

// initGPUIfEnabled is a no-op when GPU support is not compiled in
func (vs *VectorStore) initGPUIfEnabled(idx VectorIndex) {
	// GPU support not enabled in this build
}
