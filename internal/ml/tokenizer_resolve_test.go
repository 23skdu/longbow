package ml

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveTokenizerPath(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.onnx")
	if err := os.WriteFile(filepath.Join(dir, "vocab.txt"), []byte("[CLS]\n[SEP]\n[UNK]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got := ResolveTokenizerPath(modelPath)
	if got != filepath.Join(dir, "vocab.txt") {
		t.Fatalf("got %s", got)
	}
}
