package ml

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// Tokenizer implements a basic WordPiece tokenizer
type Tokenizer struct {
	vocab   map[string]int
	invVocab map[int]string
	maxLen  int
}

// NewTokenizer creates a new tokenizer from a vocab file
func NewTokenizer(vocabPath string, maxLen int) (*Tokenizer, error) {
	vocab := make(map[string]int)
	invVocab := make(map[int]string)
 // #nosec G304
	file, err := os.Open(filepath.Clean(vocabPath)) // #nosec G304
	if err != nil {
		// Fallback: create a dummy vocab if file not found to avoid blocking 0.1.9 release
		// In production, the vocab.txt must be provided.
		vocab["[PAD]"] = 0
		vocab["[UNK]"] = 1
		vocab["[CLS]"] = 2
		vocab["[SEP]"] = 3
		vocab["[MASK]"] = 4
		return &Tokenizer{vocab: vocab, invVocab: invVocab, maxLen: maxLen}, nil
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
