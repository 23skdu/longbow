//go:build !amd64 && !arm64

package simd

func pause() {}
