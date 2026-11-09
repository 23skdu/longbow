//go:build !onnx
// +build !onnx

package onnx

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubErr is the exact error every session entry point returns on this build.
const stubErr = "ONNX Runtime not available in this build"

// manualValidRepoID is an independent reference implementation of the
// owner/repo format enforced by validRepoID, used to cross-check the regex.
func manualValidRepoID(id string) bool {
	parts := strings.Split(id, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	for _, part := range parts {
		for i := 0; i < len(part); i++ {
			c := part[i]
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
				(c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-' {
				continue
			}
			return false
		}
	}
	return true
}

// FuzzRepoIDValidation asserts the DownloadModel pre-flight contract: the
// manual owner/repo classifier agrees with validRepoID, invalid repo IDs are
// rejected deterministically before any directory is created (and therefore
// before any network access), and valid repo IDs stop after the structural
// check.
func FuzzRepoIDValidation(f *testing.F) {
	// Seeds: valid ids, empty, missing/extra separators, bad characters,
	// embedded newline, allowed punctuation.
	f.Add("owner/repo")
	f.Add("")
	f.Add("no-slash")
	f.Add("a/b/c")
	f.Add("owner/re po")
	f.Add("up/down.model-1_x")
	f.Add("trailing/")
	f.Add("with\nnewline/repo")

	f.Fuzz(func(t *testing.T, repoID string) {
		// Bound the input; the validation itself is O(len).
		if len(repoID) > 512 {
			return
		}

		// The reference implementation must mirror the package regex.
		require.Equal(t, validRepoID.MatchString(repoID), manualValidRepoID(repoID),
			"manual repoID check must mirror validRepoID for %q", repoID)

		// Valid repo IDs would trigger real downloads: stop after the
		// structural check (no directory creation, no network).
		if validRepoID.MatchString(repoID) {
			return
		}

		// Invalid repo IDs must fail fast, before MkdirAll runs.
		dest := filepath.Join(t.TempDir(), "dest")
		err := DownloadModel(repoID, dest)
		require.Error(t, err)
		if repoID == "" {
			assert.Equal(t, "repoID cannot be empty", err.Error())
		} else {
			assert.Contains(t, err.Error(), "invalid repoID format")
		}
		_, statErr := os.Stat(dest)
		require.ErrorIs(t, statErr, os.ErrNotExist,
			"dest dir must not be created for invalid repoID %q", repoID)

		// Validation is deterministic: a second attempt fails identically.
		err2 := DownloadModel(repoID, dest)
		require.Error(t, err2)
		require.Equal(t, err.Error(), err2.Error())
	})
}

// FuzzStubSessionContract asserts the !onnx Session contract: NewSession
// always fails with the exact stub error and returns no session, Score/Embed
// fail identically and deterministically without touching their inputs, and
// Close is a no-op.
func FuzzStubSessionContract(f *testing.F) {
	// Seeds: typical paths, empty inputs, unicode, repeated docs,
	// path-traversal-looking input, multi-doc batches.
	f.Add("fake.onnx", "query", "doc1\x00doc2")
	f.Add("", "", "")
	f.Add("/tmp/model.onnx", "q", "only-doc")
	f.Add("model with spaces.onnx", "ünïcode query", "d")
	f.Add(strings.Repeat("a", 100), strings.Repeat("b", 100), strings.Repeat("c", 100))
	f.Add("../../etc/passwd", "q\x00r", "x\x00y\x00z")

	f.Fuzz(func(t *testing.T, modelPath, query, docsJoined string) {
		// Bound work: strings stay small and the doc batch stays finite.
		if len(modelPath) > 4096 || len(query) > 4096 || len(docsJoined) > 4096 {
			return
		}
		if strings.Count(docsJoined, "\x00") > 64 {
			return
		}

		// Init is a no-op on this build.
		require.NoError(t, Init())

		// NewSession must fail with the exact stub error and no session.
		s, err := NewSession(modelPath)
		require.Error(t, err)
		require.Equal(t, stubErr, err.Error())
		require.Nil(t, s)

		sess := &Session{}
		docs := strings.Split(docsJoined, "\x00")
		docs0 := slices.Clone(docs)

		scores, scoreErr := sess.Score(context.Background(), query, docs)
		require.Error(t, scoreErr)
		require.Equal(t, stubErr, scoreErr.Error())
		require.Nil(t, scores)

		embeds, embedErr := sess.Embed(context.Background(), docs)
		require.Error(t, embedErr)
		require.Equal(t, stubErr, embedErr.Error())
		require.Nil(t, embeds)

		// Determinism: repeated calls fail with the identical error.
		scores2, scoreErr2 := sess.Score(context.Background(), query, docs)
		require.Equal(t, scoreErr.Error(), scoreErr2.Error())
		require.Nil(t, scores2)

		// Close is a no-op.
		require.NoError(t, sess.Close())

		// Inputs untouched.
		require.Equal(t, docs0, docs)
	})
}
