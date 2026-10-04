package query

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// StringDictionary maps categorical string values to 16-bit unsigned integer IDs.
type StringDictionary struct {
	mu      sync.RWMutex
	lookup  map[string]uint16
	symbols []string
}

// NewStringDictionary creates an initialized StringDictionary.
// ID 0 is reserved for empty/null or unmapped.
func NewStringDictionary() *StringDictionary {
	return &StringDictionary{
		lookup:  make(map[string]uint16),
		symbols: []string{""},
	}
}

// GetOrCreate returns the 16-bit integer ID for s, allocating a new ID if not present.
func (d *StringDictionary) GetOrCreate(s string) uint16 {
	d.mu.RLock()
	id, ok := d.lookup[s]
	d.mu.RUnlock()
	if ok {
		return id
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if id, ok := d.lookup[s]; ok {
		return id
	}
	if len(d.symbols) >= 65535 {
		return 0
	}
	id = uint16(len(d.symbols))
	d.lookup[s] = id
	d.symbols = append(d.symbols, s)
	return id
}

// Lookup returns the 16-bit integer ID for s if present.
func (d *StringDictionary) Lookup(s string) (uint16, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	id, ok := d.lookup[s]
	return id, ok
}

// EncodeArray encodes an Arrow String array into a contiguous slice of 16-bit integer IDs.
func (d *StringDictionary) EncodeArray(arr *array.String) []uint16 {
	n := arr.Len()
	codes := make([]uint16, n)
	for i := 0; i < n; i++ {
		if arr.IsValid(i) {
			codes[i] = d.GetOrCreate(arr.Value(i))
		} else {
			codes[i] = 0
		}
	}
	return codes
}
