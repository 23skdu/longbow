package types

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzGPUErrorClassification(f *testing.F) {
	f.Add(uint8(0), "no GPU found", "", int32(0), 0, uint64(0), false)
	f.Add(uint8(1), "", "", int32(3), 64, uint64(1024), false)
	f.Add(uint8(2), "driver missing", "init", int32(1), 0, uint64(0), true)
	f.Add(uint8(3), "conv2d failed", "conv2d", int32(0), 0, uint64(0), true)
	f.Add(uint8(4), "sync timeout", "", int32(2), 100, uint64(0), false)
	f.Add(uint8(5), "plain failure", "", int32(0), 0, uint64(0), false)
	f.Add(uint8(255), "wrapped", "kernel", int32(-7), -1, ^uint64(0), true)

	f.Fuzz(func(t *testing.T, kind uint8, payload, op string, deviceID int32, batchSize int, requested uint64, wrap bool) {
		// Bound inputs; oversized messages are out of range and dropped.
		if len(payload) > 512 || len(op) > 128 {
			return
		}

		cause := errors.New(payload)
		sel := int(kind % 6)

		var err error
		switch sel {
		case 0:
			err = &GPUNotAvailableError{Reason: payload}
		case 1:
			err = &GPUMemoryError{Requested: requested, Available: requested / 2, DeviceID: deviceID}
		case 2:
			err = &GPUInitializationError{DeviceID: deviceID, Backend: GPUBackend(sel), Cause: cause}
		case 3:
			err = &GPUComputeError{Operation: op, DeviceID: deviceID, Cause: cause}
		case 4:
			err = &GPUSyncError{BatchSize: batchSize, DeviceID: deviceID, Cause: cause}
		default:
			err = cause
		}
		inner := err
		if wrap {
			err = fmt.Errorf("operation context: %w", err)
		}

		// Type predicates must classify exactly the constructed kind,
		// including through an outer wrapping error.
		require.Equal(t, sel == 0, IsGPUNotAvailableError(err))
		require.Equal(t, sel == 1, IsGPUMemoryError(err))
		require.Equal(t, sel == 2, IsGPUInitializationError(err))
		require.Equal(t, sel == 3, IsGPUComputeError(err))
		require.Equal(t, sel == 4, IsGPUSyncError(err))

		// IsGPUError is exactly "one of the five GPU error types".
		require.Equal(t, sel != 5, IsGPUError(err))

		// Retriability: memory, compute and sync errors are retriable;
		// not-available, initialization and plain errors are not.
		wantRetriable := sel == 1 || sel == 3 || sel == 4
		require.Equal(t, wantRetriable, IsRetriableGPUError(err))
		// A retriable GPU error is always a GPU error.
		if IsRetriableGPUError(err) {
			require.True(t, IsGPUError(err))
		}

		// Rendering is deterministic and carries the fuzzed payload.
		msg1 := err.Error()
		msg2 := err.Error()
		require.Equal(t, msg1, msg2)
		if sel != 5 || payload != "" {
			require.NotEmpty(t, msg1)
		}
		switch sel {
		case 1:
			require.Contains(t, msg1, "GPU memory error")
			require.Contains(t, msg1, fmt.Sprintf("%d", requested))
		case 5:
			require.Equal(t, payload, msg1)
		default:
			require.Contains(t, msg1, payload)
		}

		// Cause-carrying errors keep their cause reachable through wrapping.
		if sel == 2 || sel == 3 || sel == 4 {
			require.ErrorIs(t, err, cause)
			u, ok := inner.(interface{ Unwrap() error })
			require.True(t, ok)
			require.Equal(t, cause, u.Unwrap())
		}
	})
}

func FuzzGPUBackendString(f *testing.F) {
	f.Add(0)
	f.Add(1)
	f.Add(2)
	f.Add(3)
	f.Add(4)
	f.Add(99)
	f.Add(-1)
	f.Add(1 << 30)

	f.Fuzz(func(t *testing.T, v int) {
		got := GPUBackend(v).String()

		// The formatter must only ever emit one of its known labels, and
		// must map exactly the defined backend constants (OpenCL has no
		// dedicated label today) to a named backend.
		require.Contains(t, []string{"CPU", "CUDA", "Metal", "TPU", "Unknown"}, got)
		want := "Unknown"
		switch v {
		case 0:
			want = "CPU"
		case 1:
			want = "CUDA"
		case 2:
			want = "Metal"
		case 4:
			want = "TPU"
		}
		require.Equal(t, want, got, "backend %d", v)
		require.Equal(t, got, GPUBackend(v).String(), "String must be deterministic")
	})
}
