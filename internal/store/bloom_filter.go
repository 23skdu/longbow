package store

import (
"hash/fnv"
"math"
"sync"
)

// BloomFilter is a space-efficient probabilistic data structure
// for testing set membership with no false negatives.
type BloomFilter struct {
mu       sync.RWMutex
bits     []uint64   // bit array stored as uint64 words
size     uint64     // total number of bits
numHash  int        // number of hash functions
}

// NewBloomFilter creates a bloom filter optimized for n items with false positive rate p.
// Uses optimal size and hash count formulas:
//   m = -n*ln(p) / (ln(2)^2)
//   k = m/n * ln(2)
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

// hash computes two base hashes and derives k hash values using double hashing.
// h(i) = h1 + i*h2 mod m
func (bf *BloomFilter) hash(data string) []uint64 {
// First hash: FNV-1a
h1 := fnv.New64a()
h1.Write([]byte(data))
hash1 := h1.Sum64()

// Second hash: FNV-1 (different algorithm)
h2 := fnv.New64()
h2.Write([]byte(data))
hash2 := h2.Sum64()

// Ensure hash2 is odd for better distribution
if hash2%2 == 0 {
hash2++
}

// Generate k hash values using double hashing
hashes := make([]uint64, bf.numHash)
for i := 0; i < bf.numHash; i++ {
hashes[i] = (hash1 + uint64(i)*hash2) % bf.size
}
return hashes
}

// Add inserts an item into the bloom filter.
func (bf *BloomFilter) Add(item string) {
hashes := bf.hash(item)

bf.mu.Lock()
for _, h := range hashes {
wordIdx := h / 64
bitIdx := h % 64
bf.bits[wordIdx] |= 1 << bitIdx
}
bf.mu.Unlock()
}

// Contains checks if an item might be in the set.
// Returns true if possibly present (may be false positive).
// Returns false if definitely not present (never false negative).
func (bf *BloomFilter) Contains(item string) bool {
hashes := bf.hash(item)

bf.mu.RLock()
defer bf.mu.RUnlock()

for _, h := range hashes {
wordIdx := h / 64
bitIdx := h % 64
if bf.bits[wordIdx]&(1<<bitIdx) == 0 {
return false
}
}
return true
}
