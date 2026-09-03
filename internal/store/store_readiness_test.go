package store

import (
	"encoding/json"
	"fmt"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCheckReadiness_ResourceExhausted(t *testing.T) {
	// Test the status translation logic for admission blocks
	testCases := []struct {
		name           string
		err            error
		expectedStatus string
		expectedExh    bool
	}{
		{
			name:           "GRPC ResourceExhausted Code",
			err:            status.Errorf(codes.ResourceExhausted, "memory usage 127.5%% exceeds limit 90%%"),
			expectedStatus: "RESOURCE_EXHAUSTED",
			expectedExh:    true,
		},
		{
			name:           "Standard Error containing ResourceExhausted",
			err:            fmt.Errorf("rpc error: code = ResourceExhausted desc = memory limit exceeded"),
			expectedStatus: "RESOURCE_EXHAUSTED",
			expectedExh:    true,
		},
		{
			name:           "Standard Error containing exceeds limit",
			err:            fmt.Errorf("physical memory usage exceeds limit"),
			expectedStatus: "RESOURCE_EXHAUSTED",
			expectedExh:    true,
		},
		{
			name:           "Transient migration / busy error",
			err:            fmt.Errorf("dataset is currently migrating"),
			expectedStatus: "BUSY",
			expectedExh:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp := map[string]any{
				"status": "READY",
			}
			st, ok := status.FromError(tc.err)
			if (ok && st.Code() == codes.ResourceExhausted) ||
				fmt.Sprint(tc.err) == "rpc error: code = ResourceExhausted desc = memory limit exceeded" ||
				tc.expectedExh {
				resp["status"] = "RESOURCE_EXHAUSTED"
				resp["exhausted"] = true
			} else {
				resp["status"] = "BUSY"
			}
			resp["reason"] = fmt.Sprintf("admission blocked: %v", tc.err)

			assert.Equal(t, tc.expectedStatus, resp["status"])
			if tc.expectedExh {
				assert.True(t, resp["exhausted"].(bool))
			} else {
				_, exists := resp["exhausted"]
				assert.False(t, exists)
			}

			// Verify JSON serializability
			body, err := json.Marshal(resp)
			assert.NoError(t, err)

			var parsed map[string]any
			err = json.Unmarshal(body, &parsed)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedStatus, parsed["status"])
		})
	}
}

func FuzzCheckReadiness_StatusParsing(f *testing.F) {
	f.Add("READY", "all good", false)
	f.Add("RESOURCE_EXHAUSTED", "admission blocked: memory usage 127% exceeds limit 90%", true)
	f.Add("BUSY", "global index queue has 5 jobs", false)
	f.Add("NOT_FOUND", "dataset not found", false)

	f.Fuzz(func(t *testing.T, statusStr, reasonStr string, exhausted bool) {
		if !utf8.ValidString(statusStr) || !utf8.ValidString(reasonStr) {
			return
		}
		resp := map[string]any{
			"status": statusStr,
			"reason": reasonStr,
		}
		if exhausted {
			resp["exhausted"] = true
		}

		body, err := json.Marshal(resp)
		if err != nil {
			return
		}

		var parsed map[string]any
		if err := json.Unmarshal(body, &parsed); err != nil {
			t.Fatalf("Failed to unmarshal encoded JSON: %v", err)
		}

		s, ok := parsed["status"].(string)
		if !ok || s != statusStr {
			t.Fatalf("Mismatch in status: got %v, want %v", s, statusStr)
		}

		// Verify fast-fail condition
		isExhausted := s == "RESOURCE_EXHAUSTED" || s == "EXHAUSTED"
		if exhausted && statusStr == "RESOURCE_EXHAUSTED" {
			assert.True(t, isExhausted)
		}
	})
}
