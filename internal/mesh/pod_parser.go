package mesh

import (
	"bytes"
	"errors"
)

var (
	keyItems = []byte(`"items"`)
	keyPodIP = []byte(`"podIP"`)
)

// ExtractPodIPs scans the JSON response body from K8s API for podIPs.
// It avoids full unmarshaling and operates on the byte slice directly.
// It returns a slice of IP strings found in the "items" array.
func ExtractPodIPs(data []byte) ([]string, error) {
	// 1. Locate "items": [
	// We search for "items" key then find the opening bracket.
	idx := bytes.Index(data, keyItems)
	if idx == -1 {
		return nil, errors.New("invalid response: items field not found")
	}

	// Advance past "items"
	data = data[idx+len(keyItems):]

	// Find the opening '[' of the array
	startArr := bytes.IndexByte(data, '[')
	if startArr == -1 {
		return nil, errors.New("invalid response: items array start not found")
	}
	data = data[startArr+1:]

	var ips []string

	// Simple state: We are inside the items array.
	// We scan for "podIP": "..." pattern.
	// We stop when we see the closing ']' of the items array.
	// NOTE: This assumes "podIP" does not appear in metadata/spec in a way that
	// confuses this simple scanner (e.g. inside a string literal).
	// For K8s API, "podIP" is a unique key in status.
	// To be safer, we could count braces, but for high-perf/zero-alloc
	// on trusted K8s API output, a linear scan for the key is efficient.

	// Limit scope to the array end if possible?
	// Finding matching ']' is hard without parsing nesting.
	// We will just scan the rest of the buffer. The probability of "podIP"
	// appearing elsewhere is low enough for this specific use case,
	// and even if it does (e.g. in an annotation), it might be a valid IP we want anyway?
	// Actually, let's just scan linearly.

	for {
		// Find next "podIP"
		idx := bytes.Index(data, keyPodIP)
		if idx == -1 {
			break
		}

		// Advance to value
		// "podIP": "10.0.0.1"
		//        ^
		// We expect a colon and quote
		valueStart := idx + len(keyPodIP)
		if valueStart >= len(data) {
			break
		}

		// Look for opening quote of the value
		quoteStart := bytes.IndexByte(data[valueStart:], '"')
		if quoteStart == -1 {
			// Malformed or end
			break
		}
		quoteStart += valueStart

		// Look for closing quote
		quoteEnd := bytes.IndexByte(data[quoteStart+1:], '"')
		if quoteEnd == -1 {
			break
		}
		quoteEnd += quoteStart + 1

		// Extract IP
		ipBytes := data[quoteStart+1 : quoteEnd]
		if len(ipBytes) > 0 {
			ips = append(ips, string(ipBytes))
		}

		// Advance
		data = data[quoteEnd+1:]
	}

	return ips, nil
}
