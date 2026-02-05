package gpu

import (
	"errors"
	"fmt"
)

// GPUNotAvailableError indicates that GPU is not available on this system
type GPUNotAvailableError struct {
	Reason string
}

func (e *GPUNotAvailableError) Error() string {
	return fmt.Sprintf("GPU not available: %s", e.Reason)
}

// IsGPUNotAvailableError checks if an error is GPUNotAvailableError
func IsGPUNotAvailableError(err error) bool {
	var e *GPUNotAvailableError
	return errors.As(err, &e)
}

// GPUMemoryError indicates GPU memory allocation failure
type GPUMemoryError struct {
	Requested uint64
	Available uint64
	DeviceID  int
}

func (e *GPUMemoryError) Error() string {
	return fmt.Sprintf("GPU memory error on device %d: requested %d bytes, available %d bytes",
		e.DeviceID, e.Requested, e.Available)
}

// IsGPUMemoryError checks if an error is GPUMemoryError
func IsGPUMemoryError(err error) bool {
	var e *GPUMemoryError
	return errors.As(err, &e)
}

// GPUInitializationError indicates failure during GPU initialization
type GPUInitializationError struct {
	DeviceID int
	Backend  GPUBackend
	Cause    error
}

func (e *GPUInitializationError) Error() string {
	return fmt.Sprintf("GPU initialization failed for device %d (backend: %s): %v",
		e.DeviceID, e.Backend, e.Cause)
}

// Unwrap returns the underlying cause
func (e *GPUInitializationError) Unwrap() error {
	return e.Cause
}

// IsGPUInitializationError checks if an error is GPUInitializationError
func IsGPUInitializationError(err error) bool {
	var e *GPUInitializationError
	return errors.As(err, &e)
}

// GPUComputeError indicates failure during GPU computation
type GPUComputeError struct {
	Operation string
	DeviceID  int
	Cause     error
}

func (e *GPUComputeError) Error() string {
	return fmt.Sprintf("GPU computation error on device %d during %s: %v",
		e.DeviceID, e.Operation, e.Cause)
}

// Unwrap returns the underlying cause
func (e *GPUComputeError) Unwrap() error {
	return e.Cause
}

// IsGPUComputeError checks if an error is GPUComputeError
func IsGPUComputeError(err error) bool {
	var e *GPUComputeError
	return errors.As(err, &e)
}

// GPUSyncError indicates failure during GPU synchronization
type GPUSyncError struct {
	BatchSize int
	DeviceID  int
	Cause     error
}

func (e *GPUSyncError) Error() string {
	return fmt.Sprintf("GPU sync error on device %d for batch of %d vectors: %v",
		e.DeviceID, e.BatchSize, e.Cause)
}

// Unwrap returns the underlying cause
func (e *GPUSyncError) Unwrap() error {
	return e.Cause
}

// IsGPUSyncError checks if an error is GPUSyncError
func IsGPUSyncError(err error) bool {
	var e *GPUSyncError
	return errors.As(err, &e)
}

// IsGPUError checks if an error is any type of GPU error
func IsGPUError(err error) bool {
	return IsGPUNotAvailableError(err) ||
		IsGPUMemoryError(err) ||
		IsGPUInitializationError(err) ||
		IsGPUComputeError(err) ||
		IsGPUSyncError(err)
}

// IsRetriableGPUError determines if a GPU error is retriable
func IsRetriableGPUError(err error) bool {
	if !IsGPUError(err) {
		return false
	}

	// Memory errors might be retriable if caused by temporary fragmentation
	if IsGPUMemoryError(err) {
		return true
	}

	// Sync errors are often retriable
	if IsGPUSyncError(err) {
		return true
	}

	// Compute errors might be transient
	if IsGPUComputeError(err) {
		return true
	}

	return false
}
