package ml

import (
	"strings"
	"testing"
)

// fuzzVocab is a small, fixed WordPiece vocabulary. Keeping it fixed (rather
// than fuzzed) makes the round-trip assertions meaningful; the fuzz inputs
// are the words and text to encode.
func fuzzVocab() map[string]int {
	return map[string]int{
		"[CLS]": 0, "[SEP]": 1, "[UNK]": 2,
		"hello": 3, "world": 4, "the": 5, "dog": 6,
		"un": 7, "##known": 8, "##lo": 9, "##ed": 10,
		"play": 11, "##ing": 12, "run": 13, "##ning": 14,
	}
}

// FuzzWordPiece checks the WordPiece decomposition round-trip: any word that
// decomposes into vocabulary pieces must reconstruct exactly when the
// continuation markers are stripped, and every emitted piece must be a real
// vocabulary entry (with "[UNK]" as the only fallback).
func FuzzWordPiece(f *testing.F) {
	f.Add("hello")
	f.Add("unknownword")
	f.Add("unlo")
	f.Add("")
	f.Add("running")
	f.Add("##known")

	f.Fuzz(func(t *testing.T, word string) {
		if len(word) > 512 {
			return
		}
		tok := &Tokenizer{vocab: fuzzVocab(), maxLen: 8}
		pieces := tok.wordpiece(word)

		for _, p := range pieces {
			if _, ok := tok.vocab[p]; !ok {
				t.Errorf("piece %q is not in the vocabulary", p)
			}
		}
		if len(pieces) == 1 && pieces[0] == "[UNK]" {
			return // not decomposable: nothing to round-trip
		}

		// Round trip: strip the continuation marker from every piece after
		// the first and concatenate; the result must be the original word.
		var b strings.Builder
		for i, p := range pieces {
			if i > 0 {
				if !strings.HasPrefix(p, "##") {
					t.Errorf("continuation piece %q is missing the ## prefix", p)
				}
				p = strings.TrimPrefix(p, "##")
			}
			b.WriteString(p)
		}
		if got := b.String(); got != word {
			t.Errorf("pieces %q decoded to %q, want %q", pieces, got, word)
		}
	})
}

// FuzzEncodeBounds checks the Encode contract: the outputs always match the
// maxLen budget, the attention mask is a prefix of ones, emitted ids are
// valid vocabulary ids, the sequence starts with [CLS], and padding only
// appears after a [SEP] terminator.
func FuzzEncodeBounds(f *testing.F) {
	f.Add("hello world", 8)
	f.Add("", 4)
	f.Add("HELLO unknownword", 6)
	f.Add("the dog is running", 3) // truncation
	f.Add("hello", -1)             // negative budget is out of range
	f.Add("", 0)                   // zero budget

	f.Fuzz(func(t *testing.T, text string, maxLen int) {
		if maxLen < 0 || maxLen > 2048 {
			return
		}
		if len(text) > 1024 {
			return
		}
		tok := &Tokenizer{vocab: fuzzVocab(), maxLen: maxLen}
		tokens, mask := tok.Encode(text)

		if len(tokens) != maxLen || len(mask) != maxLen {
			t.Fatalf("len(tokens)=%d len(mask)=%d, want %d", len(tokens), len(mask), maxLen)
		}

		seenZero := false
		lastReal := -1
		for i := range mask {
			switch mask[i] {
			case 0:
				seenZero = true
			case 1:
				if seenZero {
					t.Fatalf("mask turns back on at index %d: %v", i, mask)
				}
				lastReal = i
				if id := tokens[i]; id < 0 || int(id) >= len(tok.vocab) {
					t.Fatalf("token id %d out of vocabulary range [0, %d)", id, len(tok.vocab))
				}
			default:
				t.Fatalf("mask[%d] = %d, want 0 or 1", i, mask[i])
			}
		}

		if lastReal >= 0 {
			if want := int64(tok.vocab["[CLS]"]); tokens[0] != want {
				t.Errorf("first token = %d, want [CLS] %d", tokens[0], want)
			}
			// Padding after the last real token means the sequence ended
			// here, so it must have ended with [SEP]. A completely full
			// mask may instead be a truncated longer sequence.
			if lastReal < len(mask)-1 {
				if want := int64(tok.vocab["[SEP]"]); tokens[lastReal] != want {
					t.Errorf("last real token = %d, want [SEP] %d (mask %v)", tokens[lastReal], want, mask)
				}
			}
		}
	})
}
