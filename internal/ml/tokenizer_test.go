package ml

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenizer(t *testing.T) {
	// 1. Create a dummy vocab file
	vocabContent := "hello\nworld\n[CLS]\n[SEP]\n[MASK]\n[UNK]\n"
	tmpVocab, err := os.CreateTemp("", "vocab.txt")
	require.NoError(t, err)
	defer os.Remove(tmpVocab.Name())
	
	_, err = tmpVocab.WriteString(vocabContent)
	require.NoError(t, err)
	tmpVocab.Close()

	// 2. Load tokenizer
	tokenizer, err := NewTokenizer(tmpVocab.Name(), 512)
	require.NoError(t, err)
	require.NotNil(t, tokenizer)

	// 3. Test encoding
	ids, mask := tokenizer.Encode("hello world")
	assert.NotEmpty(t, ids)
	assert.NotEmpty(t, mask)
	assert.Equal(t, len(ids), len(mask))
	
	// CLS + hello + world + SEP = 4 tokens
	assert.GreaterOrEqual(t, len(ids), 4)
	assert.Equal(t, int64(1), mask[0]) // CLS mask should be 1
}

func TestTokenizerMissingVocab(t *testing.T) {
	// Test loading with non-existent file - should fail
	tokenizer, err := NewTokenizer("non-existent-vocab.txt", 128)
	require.Error(t, err)
	assert.Nil(t, tokenizer)
}
