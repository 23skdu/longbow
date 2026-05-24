package index

import (
	"hash/fnv"
	"math"
	"sync/atomic"
)

// BloomFilter is a space-efficient probabilistic data structure
// for testing set membership with no false negatives.
type BloomFilter struct {
	bits    []uint64 // bit array stored as uint64 words
	size    uint64   // total number of bits
	numHash int      // number of hash functions
}

// NewBloomFilter creates a bloom filter optimized for n items with false positive rate p.
func NewBloomFilter(n int, p float64) *BloomFilter {
	if n <= 0 {
		n = 1
	}
	if p <= 0 || p >= 1 {
		p = 0.01
	}

	// Calculate optimal size (number of bits)
	ln2 := math.Ln2
	m := -float64(n) * math.Log(p) / (ln2 * ln2)
	size := uint64(math.Ceil(m))
	if size < 64 {
		size = 64
	}

	// Calculate optimal number of hash functions
	k := int(math.Ceil((float64(size) / float64(n)) * ln2))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30 // cap to avoid excessive hashing
	}

	// Allocate bit array (round up to uint64 boundary)
	numWords := (size + 63) / 64

	return &BloomFilter{
		bits:    make([]uint64, numWords),
		size:    size,
		numHash: k,
	}
}

// hash computes hashes for the given item.
func (bf *BloomFilter) hash(item string) []uint64 {
	h1 := fnv.New64a()
	_, _ = h1.Write([]byte(item))
	hash1 := h1.Sum64()

	h2 := fnv.New64()
	_, _ = h2.Write([]byte(item))
	hash2 := h2.Sum64()

	if hash2%2 == 0 {
		hash2++
	}

	hashes := make([]uint64, bf.numHash)
	for i := 0; i < bf.numHash; i++ {
		hashes[i] = (hash1 + uint64(i)*hash2) % bf.size
	}
	return hashes
}

// Add inserts an item into the bloom filter atomically.
func (bf *BloomFilter) Add(item string) {
	hashes := bf.hash(item)

	for _, h := range hashes {
		wordIdx := h / 64
		bitIdx := h % 64
		mask := uint64(1) << bitIdx
		atomic.OrUint64(&bf.bits[wordIdx], mask)
	}
}

// Contains checks if an item might be in the set atomically.
func (bf *BloomFilter) Contains(item string) bool {
	hashes := bf.hash(item)

	for _, h := range hashes {
		wordIdx := h / 64
		bitIdx := h % 64
		mask := uint64(1) << bitIdx
		if atomic.LoadUint64(&bf.bits[wordIdx])&mask == 0 {
			return false
		}
	}
	return true
}
