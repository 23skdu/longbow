package ml

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Tokenizer implements a basic WordPiece tokenizer
type Tokenizer struct {
	vocab    map[string]int
	invVocab map[int]string
	maxLen   int
}

// ResolveTokenizerPath returns the best tokenizer asset path for a model path.
// It searches the model's directory for tokenizer.json then vocab.txt, falling
// back to the provided path when no asset is found.
func ResolveTokenizerPath(modelPath string) string {
	dir := filepath.Dir(modelPath)
	candidates := []string{
		filepath.Join(dir, "tokenizer.json"),
		filepath.Join(dir, "vocab.txt"),
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	if modelPath != "" {
		return modelPath
	}
	return "vocab.txt"
}

// NewTokenizerForModel creates a tokenizer resolved relative to a model path.
func NewTokenizerForModel(modelPath string, maxLen int) (*Tokenizer, error) {
	return NewTokenizer(ResolveTokenizerPath(modelPath), maxLen)
}

// NewTokenizer creates a new tokenizer from a vocab file
func NewTokenizer(vocabPath string, maxLen int) (*Tokenizer, error) {
	vocab := make(map[string]int)
	invVocab := make(map[int]string)
	// #nosec G304
	file, err := os.Open(filepath.Clean(vocabPath)) // #nosec G304
	if err != nil {
		return nil, fmt.Errorf("failed to open vocab file %s: %w", vocabPath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	id := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			vocab[line] = id
			invVocab[id] = line
			id++
		}
	}

	return &Tokenizer{
		vocab:    vocab,
		invVocab: invVocab,
		maxLen:   maxLen,
	}, nil
}

// Encode converts text to token IDs and attention mask
func (t *Tokenizer) Encode(text string) ([]int64, []int64) {
	tokens := []int64{int64(t.vocab["[CLS]"])}

	words := strings.Fields(strings.ToLower(text))
	for _, word := range words {
		if len(tokens) >= t.maxLen-1 {
			break
		}

		subtokens := t.wordpiece(word)
		for _, sub := range subtokens {
			if len(tokens) >= t.maxLen-1 {
				break
			}
			if id, ok := t.vocab[sub]; ok {
				tokens = append(tokens, int64(id))
			} else {
				tokens = append(tokens, int64(t.vocab["[UNK]"]))
			}
		}
	}

	tokens = append(tokens, int64(t.vocab["[SEP]"]))

	mask := make([]int64, t.maxLen)
	paddedTokens := make([]int64, t.maxLen)
	for i := 0; i < len(tokens) && i < t.maxLen; i++ {
		paddedTokens[i] = tokens[i]
		mask[i] = 1
	}

	return paddedTokens, mask
}

func (t *Tokenizer) wordpiece(word string) []string {
	if _, ok := t.vocab[word]; ok {
		return []string{word}
	}

	var res []string
	start := 0
	for start < len(word) {
		end := len(word)
		var curSub string
		for start < end {
			sub := word[start:end]
			if start > 0 {
				sub = "##" + sub
			}
			if _, ok := t.vocab[sub]; ok {
				curSub = sub
				break
			}
			end--
		}

		if curSub == "" {
			return []string{"[UNK]"}
		}
		res = append(res, curSub)
		start = end
	}
	return res
}
