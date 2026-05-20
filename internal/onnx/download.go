package onnx

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// validRepoID matches Hugging Face repo ID format: owner/repo
var validRepoID = regexp.MustCompile(`^[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+$`)

// DownloadModel downloads an ONNX model and its associated files from Hugging Face
func DownloadModel(repoID, destDir string) error {
	if repoID == "" {
		return fmt.Errorf("repoID cannot be empty")
	}
	if !validRepoID.MatchString(repoID) {
		return fmt.Errorf("invalid repoID format: must be 'owner/repo' with alphanumeric characters, dots, hyphens, and underscores")
	}

	// Create destination directory
	if err := os.MkdirAll(filepath.Clean(destDir), 0750); err != nil { // #nosec G301
		return fmt.Errorf("failed to create directory %s: %w", destDir, err)
	}

	// Files to download
	files := []string{
		"model.onnx",
		"config.json",
		"vocab.txt",
		"tokenizer.json",
		"tokenizer_config.json",
		"special_tokens_map.json",
	}

	fmt.Printf("Downloading model %s to %s...\n", repoID, destDir)

	for _, file := range files {
		url := fmt.Sprintf("https://huggingface.co/%s/resolve/main/%s", repoID, file)
		destPath := filepath.Join(destDir, file)

		fmt.Printf("  Checking %s...\n", file)
		err := downloadFile(url, destPath)
		if err != nil {
			// Some files might not exist (e.g., vocab.txt vs tokenizer.json), so we continue
			if strings.Contains(err.Error(), "404") {
				continue
			}
			return fmt.Errorf("failed to download %s: %w", file, err)
		}
		fmt.Printf("  Successfully downloaded %s\n", file)
	}

	return nil
}

func downloadFile(url, destPath string) error {
	// URL is constructed from validated repoID in DownloadModel (regex-validated)
	resp, err := http.Get(url) // #nosec G107 - URL constructed from validated repoID
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download: status code %d", resp.StatusCode)
	}

	out, err := os.Create(filepath.Clean(destPath)) // #nosec G304
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
