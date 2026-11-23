package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWirePayloadRoundTrip(t *testing.T) {
	payload := []byte(`{"id":"doc-1","vector":[0.25,1]}`)
	encoded := EncodeWirePayload(42, payload)
	require.Len(t, encoded, 5+len(payload))

	schemaID, hasHeader, decoded, err := DecodeWirePayload(encoded)
	require.NoError(t, err)
	require.True(t, hasHeader)
	require.Equal(t, uint32(42), schemaID)
	require.Equal(t, payload, decoded)

	plainID, hasHeader, decoded, err := DecodeWirePayload(payload)
	require.NoError(t, err)
	require.False(t, hasHeader)
	require.Zero(t, plainID)
	require.Equal(t, payload, decoded)

	_, hasHeader, _, err = DecodeWirePayload([]byte{ConfluentWireMagic})
	require.Error(t, err)
	require.True(t, hasHeader)
}

func FuzzWirePayloadRoundTrip(f *testing.F) {
	f.Add(uint32(42), `{"id":"doc-1"}`)
	f.Add(uint32(0), "")
	f.Add(uint32(4294967295), "payload")

	f.Fuzz(func(t *testing.T, schemaID uint32, payload string) {
		encoded := EncodeWirePayload(schemaID, []byte(payload))
		gotID, hasHeader, decoded, err := DecodeWirePayload(encoded)
		if payload == "" {
			require.Error(t, err)
			require.True(t, hasHeader)
			return
		}
		require.NoError(t, err)
		require.True(t, hasHeader)
		require.Equal(t, schemaID, gotID)
		require.Equal(t, payload, string(decoded))
	})
}
